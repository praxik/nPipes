# -*- mode: python;-*-

from typing import Tuple, Type, NewType, Sequence, Dict, Any, List
import yaml
import pathlib
import secrets
import platform
import os
from contextlib import contextmanager

from .header import (Header, Step, Trigger, TriggerGet, Uri, QueueName, TriggerSqs,
                     ProtocolEZQ, Command, FilePath, OutputChannelFile, Asset,
                     S3Asset, Decompression, AssetSettings, Body, BodyInString, BodyInAsset,
                     Message)

from ..assethandlers.s3path import S3Path
from ..utils.autodeleter import AutoDeleter
from ..assethandlers.assets import randomName

# TODO: Add type annotations to this file?
# TODO: Find suitable place to warn about NPIPES_SqsOverflowPath env var

# Using injection of cls = Message to break dep cycle. Downside is this
# hides the Message and Body types, so we're not getting full benefit of
# typechecking in this module.
@contextmanager
def convertFromEZQ(cls, s):
    ezqHeader, bodyStr = fromString(s)
    # Only handleing S3 assets, since that is all we've ever used with EZQ
    assets = makeAssets(ezqHeader)
    body, assets = makeBody(bodyStr, ezqHeader, assets)
    # For OutputChannel, EZQ assumed a file named "output_#{@id}.txt",
    # where @id is the sqs :message_id
    # Apps set to run under ezq mostly take a -o cmdline arg and expect
    # the variable $id to be expanded into whatever is needed. So here we
    # choose an id, expand $id in the cmdStr, then set outfile to output_$id.txt
    # Following EZQ's workflow, we also remove the file "output_#{@id}.txt"
    # at the end.
    id = secrets.token_hex(4)
    outfile = FilePath("output_{}.txt".format(id))
    fullMsgFilename = randomName() + ".ezq_full_msg"
    arglist = substituteNpipesMarkers(fullMsgFilename, makeArglist(id, ezqHeader))
    command = Command( arglist=arglist,
                       outputChannel=OutputChannelFile(outfile))
    steps = makeSteps(id, command, assets, ezqHeader)

    with AutoDeleter() as deleter:
        deleter.add(outfile)
        deleter.add(fullMsgFilename)
        writeEZQFullMessage(fullMsgFilename, ezqHeader, bodyStr)
        yield cls(Header(steps=steps), body=body)


def writeEZQFullMessage(filename, preamble, body):
    with open(filename, "w+") as file:
        file.write(
            yaml.safe_dump(
                {"body": body, "preamble": preamble}))

def makeSteps(id, command, assets, ezqHeader):
    firstStep = Step( id="0",
                      command=command,
                      assets=assets )
    result_queue_name = ezqHeader.get("result_queue_name", False)
    # In future, we will be able to tunnel one-step-removed information on
    # future steps through EZQ. This will pick them up and decode them:
    tunneledSteps = list(map(lambda d: Step._fromDict(d), ezqHeader.get("npipes_next_steps",[])))
    if result_queue_name:
        # EZQ has no concept of future Steps; only the Trigger and Protocol matter.
        # For the latter, if it came in as EZQ, we assume it must leave the same way
        secondStep = Step( id="1",
                           trigger=TriggerSqs(QueueName(result_queue_name),
                                              overflowPath=os.environ.get("NPIPES_SqsOverflowPath", "")),
                           protocol=ProtocolEZQ())
        return [firstStep, secondStep] + tunneledSteps
    else:
        return [firstStep] + tunneledSteps


def fromString(s):
    ezqHeaderStr, bodyStr = s.split("\n...\n")
    ezqHeader = yaml.safe_load(ezqHeaderStr)["EZQ"]
    return [ezqHeader, bodyStr]


def makeArglist(id, ezqHeader):
    arglist = [""]
    cmdStr = ezqHeader.get("process_command", False)
    if cmdStr:
        # EZQ handles commands as strings, and assumes a shell for invocation,
        # so duplicate that here.
        ncs = cmdStr.replace("$id", id)
        if platform.system() == "Windows":
            # Windows always invokes a shell, and always converts a proper arglist
            # to a command string anyway
            arglist = [ncs]
        else:
            arglist = ["bash", "-c", ncs]
    return arglist


def substituteNpipesMarkers(fullMsgFilename:str, arglist:Sequence[str]):
    subbed = []
    for elem in arglist:
        newelem = ( elem.replace("$msg_contents", "${escapedbodycontents}")
                        .replace("$timeout", "${timeout}")
                        .replace("$input_file", "${bodyfile}")
                        .replace("$full_msg_file", fullMsgFilename)
                  )
        for x in range(10):
            newelem = newelem.replace("$s3_{}".format(x), "${{asset_{}}}".format(x))
        subbed.append(newelem)
    return subbed
# For the tests to be written later:
# substituteNpipesMarkers(["bash", "-c", "command $s3_1 $s3_2 static $s3_4 $s3_3"])
# ['bash', '-c', 'command ${asset_1} ${asset_2} static ${asset_4} ${asset_3}']

def makeBody(bodyStr:str,
             ezqHeader:Dict[str, Any], assets:List[Asset]) -> Tuple[Body, Sequence[Asset]]:
    body:Body
    if ezqHeader.get("get_s3_file_as_body", False):
        bodyAssetIndex = len(assets)
        bodyAsset:List[Asset] = [s3DictToAsset(ezqHeader["get_s3_file_as_body"], bodyAssetIndex)]
        body = BodyInAsset(assetId=str(bodyAssetIndex))
    else:
        body = BodyInString(string=bodyStr)
        bodyAsset = []
    return (body, assets + bodyAsset)


def makeAssets(ezqHeader:Dict[str, Any]) -> List[Asset]:
    assets:List[Asset] = []
    # aindex is a monotonically increasing number to use as an ID
    for aindex, item in enumerate(ezqHeader.get("get_s3_files", [])):
        assets.append(s3DictToAsset(item, aindex))
    return assets


def s3DictToAsset(d:Dict[str, Any], idint:int) -> S3Asset:
    key = d["key"]
    path = "s3://{bucket}/{key}".format( bucket=d["bucket"],
                                            key=key)
    decompress = d.get("decompress", False)
    # EZQ specifies that .gz files *always* be decompressed,
    # regardless of decompression flag
    if pathlib.Path(key).suffix == ".gz":
        decompress = True

    dc = Decompression(decompress)
    settings = AssetSettings( id="asset_{}".format(idint),
                              decompression=dc)
    return S3Asset( path=S3Path(path), settings=settings )


def convertToEZQ(message:Message) -> str:
    header = message.header
    body = message.body

    step, *otherSteps = header.steps

    directives:Dict[str, Any] = {}
    # EZQ uses only command strings
    directives["process_command"] = " ".join(step.command.arglist)

    if len(otherSteps) > 0:
        nextStep = otherSteps[0]
        if isinstance(nextStep.trigger, TriggerSqs):
            directives["result_queue_name"] = nextStep.trigger.queueName

    directives["get_s3_files"] = assetsToGetFiles(step.assets)

    if isinstance(body, BodyInAsset):
        aid = body.assetId
        bodyAsset = list(filter(lambda a: a.settings.id == aid, step.assets))[0]
        assert(isinstance(bodyAsset, S3Asset)) # EZQ doesn't support anything else for this case
        bodyString = "Message body was diverted to S3 as {}".format(bodyAsset.path)
        directives["get_s3_file_as_body"] = assetToBkDict(bodyAsset)
    else:
        assert(isinstance(body, BodyInString))
        bodyString = body.string

    # pack up remaining steps, emit as dict, store in directive "npipes_next_steps"
    # With some minor changes to EZQ, this will eventually let us tunnel npipes
    # information through EZQ
    directives["npipes_next_steps"] = list(map(lambda x: x._toMinDict(), otherSteps))

    preamble = {"EZQ": directives}
    fullMessageString = "---\n{}\n...\n{}".format(yaml.dump(preamble), bodyString)
    return fullMessageString


# Since everything currently using EZQ only deals with assets in S3, we're
# going to assume that anything coming from or going to EZQ format only deals
# with assets in S3. This logic will need to be expanded to include other Asset
# types if that ever changes.
def assetsToGetFiles(assets:List[Asset]) -> Sequence[Dict]:
    return list(map(assetToBkDict, assets))


def assetToBkDict(asset:Asset) -> Dict:
    assert(isinstance(asset, S3Asset))
    s3path = S3Path(asset.path)
    return {"bucket": s3path.bucket, "key": s3path.key}


def toEzqOrJsonLines(message:Message) -> str:
    if isinstance(message.header.steps[0].protocol, ProtocolEZQ):
        return convertToEZQ(message)
    else:
        return message.toMinJsonLines()
