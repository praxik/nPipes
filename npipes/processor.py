# -*- mode: python;-*-

from typing import Tuple, NamedTuple, List, Dict, Union, Type, Any, Optional, Sequence
import subprocess
import string
import logging
from pathlib import Path

# from .message.message import (
#     Body, BodyInString, BodyInAsset, Message,
#     peekStep, popStep, peekTrigger)

from .message.header import (
    Asset,
    Trigger,
    OutputChannel, OutputChannelStdout, OutputChannelFile,
    Encoding, EncodingPlainText, EncodingGzB64,
    Command,
    Step, Header, Body, BodyInString, BodyInAsset, Message,
    peekStep, popStep, peekTrigger)

# from .message.body import Body, BodyInString, BodyInAsset

from .assethandlers.assets import localizeAssets, decideLocalTarget, randomName
from .configuration import Configuration
from .serialize import toJson
from .producers.producer import Producer
from .outcome import Outcome, Success, Failure
from .utils.iteratorextras import consume
from .utils.typeshed import pathlike
from .utils.autodeleter import AutoDeleter
from .utils.compressionutils import fromGzB64


# Probably going to move into some sort of Factory / Service Discovery setup:
# from .producers.sqs import ProducerSqs
# from .producers.yyy import ProducerY

# This file is largely organized according to a "dependencies first" rule.
# Control flow starts near the *bottom* of the file, and moves upward as needed.


def scrapeOutput(command:Command, cmdstdout:str) -> Outcome:
    oc = command.outputChannel
    if isinstance(oc, OutputChannelStdout):
        result:Outcome = Success(cmdstdout)
    elif isinstance(oc, OutputChannelFile):
        p = Path(oc.filepath)
        if p.is_file():
            result = Success(p.read_text())
        else:
            result = Failure("Output file {} does not exist".format(p))
    else:
        result = Failure("Unknown type of OutputChannel")
    return result


def runProcess(command:Command, input:Optional[bytes], timeout:Optional[int]) -> Outcome:
    """Runs command in a new process and returns Outcome containing stdout as
       a stringin case of Succees, or stdout and stderr as a string in case of
       Failure
    """
    try:
        cp = subprocess.run(command.arglist, input=input, timeout=timeout,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.TimeoutExpired as teerr:
        return Failure("Command timed out")
    except Exception as err:
        return Failure("Unknown error running command: {}".format(err))

    if cp.returncode is not 0:
        return Failure("Exit code: {}\nstdout: {}\nstderr: {}".format(
                        cp.returncode, cp.stdout.decode(), cp.stderr.decode()))
    else:
        return Success(cp.stdout.decode())


def runCommand(command:Command, body:str) -> Outcome:
    """Runs a command with pre-expanded tokens. Handles logic of monitoring for
       early exit.
    """
    # TODO: Push this onto another thread and periodically examine state.run
    # to handle early exit
    timeout = None if command.timeout is 0 else command.timeout

    input = body.encode("utf-8") if command.inputChannelStdin else None

    return ( runProcess(command, input, timeout) >>
             (lambda output: scrapeOutput(command, output))
           )


def toUniqueFile(data:str) -> str:
    filename = randomName()
    Path(filename).write_text(data)
    return filename

# TODO: add to this: timeout
# There is a lot of logic going on in here. Would be nice to break it up a bit more.
def expandCommand(command:Command,
                  assets:Sequence[Asset],
                  body:str,
                  bodyfile,
                  headerfile,
                  outputfile,
                  pid) -> Command:
    """Expands token variables in a *Command*'s *arglist*
    """
    targetsForIds = {"bodyfile"    : bodyfile,
                     "bodycontents": body,
                     "headerfile"  : headerfile,
                     "outputfile"  : outputfile,
                     "pid"         : pid}
    # Add asset markers into the dict
    targetsForIds.update(dict(zip(map(lambda a: a.settings.id, assets),
                                  map(decideLocalTarget, assets))))

    newargs = list(map(lambda s: string.Template(s).safe_substitute(targetsForIds),
                       command.arglist))

    # Special thing #1: we don't want to generate an escaped string unless requested
    # since bodycontents may be large and we don't want to duplicate it unnecessarily
    if any(map(lambda arg: "${escapedbodycontents}" in arg, newargs)):
        ebc = {"bodycontents": repr(body)}
        newargs = list(map(lambda s: string.Template(s).safe_substitute(ebc), newargs))

    # Special thing #2: OutputFileChannel is the other place that can carry a
    # command var
    if isinstance(command.outputChannel, OutputChannelFile):
        fp = ( string.Template(command.outputChannel.filepath)
                     .safe_substitute({"bodyfile": bodyfile}) )
        command = command._with([(".outputChannel.filepath", fp)])

    return command._with([(".arglist", newargs)])


def triggerNextStep(result:Message) -> Outcome:
    # Will get more complicated once parallel pipelines are added
    return peekTrigger(result).sendMessage(result)


def makeMessage(result: str, newHeader:Header) -> Outcome:
    return Success(Message(header=newHeader, body=BodyInString(result)))


def extractBodyInString(body:BodyInString) -> str:
    if isinstance(body.encoding, EncodingPlainText):
        return body.string
    else: # must be EncodingGzB64
       return fromGzB64(body.string.encode())


def extractBodyInAsset(body:BodyInAsset, assets:Sequence[Asset]) -> str:
    for asset in assets:
        if body.assetId == asset.settings.id:
            with open(decideLocalTarget(asset)) as f:
                return f.read()
    return ""


def extractBody(body:Body, assets:Sequence[Asset]) -> str:
    """Extract the contents of a *Body* as a string
    """
    if isinstance(body, BodyInString):
        return extractBodyInString(body)
    elif isinstance(body, BodyInAsset):
        return extractBodyInAsset(body, assets)
    else:
        return ""


def chooseCommand(config:Configuration, command:Command) -> Command:
    """Chooses between the command specified in the original configuation
       and the one passed in. Currently just based on command locking,
       but in future could be expanded allow cryptographic signing to
       override the locking.
    """
    if config.lockCommand:
        return config.command
    else:
        return command


def handleMessage(config, msg):
    """Handles a single *Message*
    """
    step, newHeader = popStep(msg.header)

    lao = localizeAssets(step.assets)
    if isinstance(lao, Failure):
        result = lao
    else:
        with AutoDeleter() as deleter:
            body = extractBody(msg.body, step.assets)
            bodyfile = deleter.add( toUniqueFile(body) )
            headerfile = deleter.add( toUniqueFile(toJson(msg.header)) )
            outputfile = deleter.add( randomName() )
            consume( map(deleter.add, lao.value) )

            result = ( ( Success(chooseCommand(config, step.command))
                         >> (lambda cmd: Success(expandCommand(cmd, step.assets, body, bodyfile,
                                           headerfile, outputfile, config.pid)))
                         >> (lambda expcmd: runCommand(expcmd, body)) )
                       >> (lambda res: makeMessage(res, newHeader))
                       >> (lambda message: triggerNextStep(message)) )
    return result


def runMessageProducer(config:Configuration, producer:Producer) -> None:
    """Runs a message *Producer* as a stream
    """
    stream = producer.messages()
    for msg in stream: # () -> Message
        result = handleMessage(config, msg)

        stream.send(result)

        if isinstance(result, Failure):
            logging.fatal(result.reason)

        # TODO: check value of state.run and break out if False
    return None
