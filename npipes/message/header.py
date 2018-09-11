# -*- mode: python;-*-

from typing import NewType, List, Union, Any, NamedTuple, TypeVar
from operator import methodcaller

from dataclasses import dataclass, field

from ..serialize import Serializable, subtractDicts
from ..assethandlers.s3path import S3Path
from ..outcome import Outcome, Success, Failure
# import npipes.message.message
from typing import Tuple, Type, NewType, Union
import json
import yaml
import pathlib
import secrets
from dataclasses import dataclass
from operator import methodcaller
from contextlib import contextmanager

from ..serialize import Serializable, toJson, toMinJson
# This one vvv is at the end of the file to avoid import cycle
# from .ezqconverter import convertFromEZQ


##############################################################################
# Typealiases
##############################################################################

Uri = NewType("Uri", str)
Topic = NewType("Topic", str)
QueueName = NewType("QueueName", str)
FilePath = NewType("FilePath", str)

# Works a bit like a forward declaration for Message so we can refer to it
# in Trigger without causing some sort of type-recursive meltdown
M = TypeVar("M", bound="Message")

##############################################################################
# Primary Types
##############################################################################

########################
# Trigger
########################
class Trigger(Serializable):
    def sendMessage(self, message:M) -> Outcome:
        pass
    def _fromDict(d):
        typ = d.get("type", "nothing").lower()
        if typ == "sns":
            return TriggerSns(Topic(d["topic"]))
        elif typ == "sqs":
            return TriggerSqs(QueueName(d["queueName"]), d["overflowPath"])
        elif typ == "get":
            return TriggerGet(Uri(d["uri"]))
        elif typ == "post":
            return TriggerPost(Uri(d["uri"]))
        elif typ == "lambda":
            return TriggerLambda(d["name"])
        elif typ =="filesystem":
            return TriggerFilesystem(d["dir"])
        else:
            return TriggerNothing()

# The inline imports for Trigger subclasses ensures that all heavy dependencies
# on outside packages (requests, boto3, etc.) happen in the subclass modules.
# Crucially, those modules are not loaded until the first call to the actual
# trigger, allowing a project to **omit dependencies for Trigger types it
# doesn't use.**
#
# Longer-term, a better solution to both problems might be to put triggers in
# a dedicated directory and use a Factory / ServiceDiscovery pattern to find
# and load available triggers and create them as needed. The advantage to that is it
# allows the later addition of Triggers without altering any of this core code.
# _fromDict up there could call into each discovered trigger to attempt to deserialize
# it. Sending would happen via a factory method followed by the call.
@dataclass(frozen=True)
class TriggerSns(Trigger):
    topic:Topic

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"topic": self.topic, "type": "SNS"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, message:M) -> Outcome:
        import npipes.triggers.sns
        return npipes.triggers.sns.sendMessage(self.topic, message)

@dataclass(frozen=True)
class TriggerSqs(Trigger):
    queueName:QueueName
    overflowPath:str

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"queueName": self.queueName,
                "overflowPath": self.overflowPath,
                "type": "SQS"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, message:M) -> Outcome:
        import npipes.triggers.sqs
        return npipes.triggers.sqs.sendMessage(self.queueName, self.overflowPath, message)

@dataclass(frozen=True)
class TriggerGet(Trigger):
    uri:Uri

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"uri": self.uri, "type": "Get"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, message:M) -> Outcome:
        import npipes.triggers.uri
        return npipes.triggers.uri.sendMessageGet(self.uri, message)

@dataclass(frozen=True)
class TriggerPost(Trigger):
    uri:Uri

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"uri": self.uri, "type": "Post"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, message:M) -> Outcome:
        import npipes.triggers.uri
        return npipes.triggers.uri.sendMessagePost(self.uri, message)

@dataclass(frozen=True)
class TriggerLambda(Trigger):
    name:str

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"name": self.name, "type": "Lambda"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, message:M) -> Outcome:
        import npipes.triggers.awsLambda
        return npipes.triggers.awsLambda.sendMessage(self.name, message)

@dataclass(frozen=True)
class TriggerFilesystem(Trigger):
    dir:str

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"dir": self.dir, "type": "Filesystem"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, message:M) -> Outcome:
        import npipes.triggers.filesystem
        return npipes.triggers.filesystem.sendMessage(self.dir, message)

@dataclass(frozen=True)
class TriggerNothing(Trigger):
    """TriggerNothing triggers nothing."""
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"type": "Nothing"}
    def _toMinDict(self):
        return self._toDict()

    def sendMessage(self, _:M) -> Outcome:
        return Success(None)


########################
# Decompression
########################

# Using wrapper class rather than raw bool since more options may be needed later.
@dataclass(frozen=True)
class Decompression(Serializable):
    decompress:bool=False

    def _toDict(self, meth=methodcaller("_toDict")):
        # return dict(self._asdict())
        return {"decompress": self.decompress}
    def _fromDict(d):
        return Decompression( decompress=d.get("decompress", False))


@dataclass(frozen=True)
class AssetSettings(Serializable):
    id:str=""
    decompression:Decompression=Decompression()
    localTarget:str=""
    """
    **id** is used as an expansion variable in a Command; eg. id=foo can be
    referenced in a command as **${foo}**. Expansion value is determined by the first
    appropriate rule as follows:
    1. If localTarget is a non-empty string, its value is the expanded value.
       NOTE: if the asset is an archive format (zip, tar, etc) and decompression
       is turned on for it, localTarget will refer to a **directory** on disk,
       not to a single file. (Makes sense once you think about it....)
    2. The default local target associated with a particular Asset type is used.
       See documentation for individual Asset types.
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        return { "id": self.id,
                  "decompression": meth(self.decompression),
	           "localTarget": self.localTarget }
    def _fromDict(d):
        return AssetSettings( id=d.get("id"),
                              decompression=Decompression._fromDict((d.get("decompression", {}))),
                              localTarget=d.get("localTarget", ""))


########################
# Asset
########################
class Asset(Serializable):
    """Sum type describing non-local assets that should be localized prior to
       running a Step's Command.
    """
    # Stub property. Should this use ABC or protocol instead?
    settings = AssetSettings("")

    def _fromDict(d):
        typ = d.get("type").lower()
        if typ == "s3":
            return S3Asset._fromDict(d)
        else: # typ == "uri":
            return UriAsset._fromDict(d)

@dataclass(frozen=True)
class S3Asset(Asset):
    path: S3Path
    settings: AssetSettings
    """An asset, stored in S3.

       **path**: S3 path of the form s3://bucket/key/parts/here
                 NOTE: This is *not* a web url! If you want to access S3 assets
                 explicitly via the REST interface to S3, use UriAsset.

       The default local target for an S3Asset is $PWD/key/parts/here ; that is,
       everything beyond s3://bucket/ is used as part of the local filename
       (unless an explicit localTarget is set in the AssetSettings).
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        return { "path": str(self.path),
                 "type": "S3",
                 "settings": meth(self.settings) }
    def _toMinDict(self):
        md = self._toDict()
        md["settings"] = self.settings._toMinDict()
        return md
    def _fromDict(d):
        return S3Asset( path=S3Path(d.get("path")),
                        settings=AssetSettings._fromDict(d.get("settings", {})))

@dataclass(frozen=True)
class UriAsset(Asset):
    uri:Uri
    settings:AssetSettings
    """An asset that exists at a URI.

       **uri**: A uri like `https://my.domain.com/file.txt`

       The default local target for a UriAsset is everything beyond the last `/`
       in the uri. For the example above, the local target would be `file.txt`.
       If your uri contains a query or anything that might lead to a long and
       unwieldy filename, it is highly recommended to explicitly set a
       localTarget in the AssetSettings.
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        return { "uri": self.uri,
                 "type": "Uri",
                 "settings": meth(self.settings) }
    def _toMinDict(self):
        md = self._toDict()
        md["settings"] = self.settings._toMinDict()
        return md
    def _fromDict(d):
        return UriAsset( uri=Uri(d.get("uri")),
                         settings=AssetSettings._fromDict(d.get("settings", {})))


########################
# Protocol
########################
class Protocol(Serializable):
    """Sum type describing which *input* message protocol to use for a Step:
       ProtocolNpipes (default, and recommended)
       ProtocolEZQ (deprecated, available for backward compatibility)
    """
    def _fromDict(d):
        val = d.get("value", "npipes").lower()
        if val == "ezq":
            return ProtocolEZQ()
        else: #"npipes":
            return ProtocolNpipes()


@dataclass(frozen=True)
class ProtocolEZQ(Protocol):
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"value": "EZQ"}
    def _toMinDict(self):
        return self._toDict()

@dataclass(frozen=True)
class ProtocolNpipes(Protocol):
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"value": "npipes"}
    # def _toMinDict(self) = self._toDict()


########################
# OutputChannel
########################
class OutputChannel(Serializable):
    """Sum type describing where Command should look for output.
       OutputChannelStdout indicates the STDOUT of the command's child process.
       OutputChannelFile indicates a named file on disk.
    """
    def _fromDict(d):
        typ = d.get("type", "stdout").lower()
        if typ == "stdout":
            return OutputChannelStdout()
        else: #"file":
            return OutputChannelFile(FilePath(d.get("filepath")))

@dataclass(frozen=True)
class OutputChannelStdout(OutputChannel):
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"type": "Stdout"}
    def _toMinDict(self):
        return self._toDict()

@dataclass(frozen=True)
class OutputChannelFile(OutputChannel):
    filepath:FilePath=FilePath("${unique}")
    """Specifies that nPipes should pick up processing results from a named file
       on disk. The file will be read as text. Filenames can be either relative
       or absolute paths, and will be treated accordingly.

       If nPipes should instead generate a unique filename for output, set the
       filepath to the string "${unique}". This will cause nPipes to do TWO things:
       1. Generate a unique filename and substitute it for the filepath
       2. Substitute all references to "${outputfile}" in the Command's arglist
          with this unique filename

       Since this unique output file generation is usually what you want (really,
       you do), this is the default setting of filepath.
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"filepath": self.filepath, "type": "File"}
    def _toMinDict(self):
        return self._toDict()



########################
# Command
########################
@dataclass(frozen=True)
class Command(Serializable):
    arglist:List[str]=field(default_factory=list)
    timeout:int=0
    inputChannelStdin:bool=False
    outputChannel:OutputChannel=OutputChannelStdout()
    """Command name and all arguments should appear as separate string entries
       in arglist. If you need your command to run inside a shell, do something
       like this: arglist=["bash", "-c", "ls -Fal *.txt | grep foo | wc"]

       timeout specifies seconds to wait for the executable in arglist to
       complete. A timeout of 0 indicates NO timeout. This is orthogonal
       to the timeout associated with a Step.

       If inputChannelStdin is True, the message body will be placed directly on
       stdin; otherwise, the message body is written to disk (absolute file path
       can be referenced as $bodyfile) and the body contents are expanded into
       the variable $bodycontents. See below for more information about these
       variables.

       Special command variables
       -------------------------
       Each string in arglist can include variables of the form ${varname}
       that will be expanded into a final commandstring before a command is
       run. This allows you to easily reference assets by consistent identifiers,
       rather than having to generate a custom commandline for every single step
       of every single message. The following variables are supported:

       **${some_asset_id}** -- each asset in an Assets list has a string-based
                           identifier which is automatically turned into a
                           variable. See docs for AssetSettings for full
                           documentation of how these are expanded.

       **${bodyfile}** -- absolute path of the file written to disk containing
                      the message body. This allows nPipes to write bodies to
                      unique, randomized filenames to avoid collision with other
                      files on disk. This variable (and only this one) is also
                      expanded in the value of OutputChannel if it is of type
                      OutputChannelFile. This allows you to let nPipes know that
                      your command will write its output to a file with a name
                      like $bodyfile.out

       **${bodycontents}** -- the message body as a string

       **${escapedbodycontents}** -- as above, but escaped to allow passing through
                        a shell. Unless you're explicitly using the shell hack
                        mentioned in the very first paragraph of this docstring, you
                        probably don't need this variable.

       **${headerfile}** -- absolute path of a file containing the Header of
                        the current message. This is provided in case your
                        transform logic needs to mutate the details of
                        subsequent Steps in the message Header.

       **${outputfile}** -- this variable is expanded to the contents of
                        the filepath in OutputChannelFile. See that class for more
                        information.

       **${pid}** -- Integer pid of the nPipes process. Commands can use this
                     to set up separate sandbox directories in case they are not
                     designed to be run in a multi-process architecture. This is
                     important when multiple nPipes processes are being run on a single
                     machine in the same userspace.
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        return { "arglist": self.arglist,
                 "timeout": self.timeout,
                 "inputChannelStdin": self.inputChannelStdin,
                 "outputChannel": meth(self.outputChannel) }
    def _fromDict(d):
        return Command(arglist=d.get("arglist",[]),
                       timeout=d.get("timeout", 0),
                       inputChannelStdin=d.get("inputChannelStdin", False),
                       outputChannel=OutputChannel._fromDict(d.get("outputChannel", {})))


########################
# Step
########################
@dataclass(frozen=True)
class Step(Serializable):
    id:str="NPIPES_EMPTY"
    trigger:Trigger=TriggerNothing()
    command:Command=Command()
    stepTimeout:int=0
    assets:List[Asset]=field(default_factory=list)
    protocol:Protocol=ProtocolNpipes()
    description:str=""
    """Describes a single processing Step in a pipeline.

    **id**:          Unique id to allow searching for this Step; "NPIPES_EMPTY"
                     is reserved
    **trigger**:     How do we trigger this Step?
    **command**:     Command to run for this step; overrides the command baked
                     into the worker iff lockCommand is False on the worker
    **stepTimeout**: Allow *Step* to consume at most this much clock time
	             (seconds); 0 means no timeout
    **assets**:      Non-local file assets that Command expects to find locally
    **protocol**:    Does this Step run on the old EZQ protocol or the new
		     Netpipes one?
    **description**: What does this Step do?
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        return { "id": self.id,
                 "trigger": meth(self.trigger),
                 "command": meth(self.command),
                 "stepTimeout": self.stepTimeout,
                 "assets": list(map(meth, self.assets)),
                 "protocol": meth(self.protocol),
                 "description": self.description }

    def _fromDict(d):
        return Step(id=d.get("id"),
                    trigger=Trigger._fromDict(d.get("trigger",{})),
                    command=Command._fromDict(d.get("command", {})),
                    stepTimeout=d.get("stepTimeout", 0),
                    assets=list(map(Asset._fromDict, d.get("assets", []))),
                    protocol=Protocol._fromDict(d.get("protocol", {})),
                    description=d.get("description", ""))


########################
# Encoding
########################
class Encoding(Serializable):
    """Indicates whether a Message Header (or Body?) is plain text
       or some other encoded form. Only current option is gzb64
       (gzipped, then base64 encoded). This class is intended in part as
       a likely place to specify signing and encryption of headers and bodies.
    """
    def _fromDict(d):
        typ = d.get("type", "plaintext").lower()
        if typ == "plaintext":
            return EncodingPlainText()
        else: #"gzb64":
            return EncodingGzB64()

@dataclass(frozen=True)
class EncodingPlainText(Encoding):
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"type": "plaintext"}
    def _toMinDict(self):
        return self._toDict()

@dataclass(frozen=True)
class EncodingGzB64(Encoding):
    def _toDict(self, meth=methodcaller("_toDict")):
        return {"type": "gzb64"}
    def _toMinDict(self):
        return self._toDict()

########################
# Header
########################
# NestedStepListType is a bit confusing; it allows any of the following:
# [Step, Step, Step]
# [Step, [Step, Step], Step]
# [Step, [Step, Step, [Step, Step, Step]], Step]
# etc. It allows you to nest Lists of Steps to indicate parallel pipelines.
# We bottom out to an Any because mypy doesn't currently support recursive
# types.
#
# This type is not good enough. Needs to be able to handle a type like this:
# [Step, [[Step, Step], [Step, Step]], Step]
# so a List of: Step, List[Step], or List[List[Step]]
# It's a complicated type. It will also need a separate tool to
# convert a list of steps to a graphviz `dot` diagram for easy visual
# checking that complicated pipelines are specified correctly.
# NestedStepListType = List[ Union[ Step,
#                                   List[ Union[ Step,
#                                                List[ Union[ Step,
#                                                             Any
#                                                           ]]]]]]


@dataclass(frozen=True)
class Header(Serializable):
    encoding:Encoding=EncodingPlainText()
    steps:List[Step]=field(default_factory=list)

    def _toDict(self, meth=methodcaller("_toDict")):
        return {"encoding": meth(self.encoding),
                "steps": list(map(meth, self.steps))}
    def _fromDict(d):
        return Header( encoding=Encoding._fromDict(d.get("encoding", {})),
                       steps=list(map(Step._fromDict, d.get("steps", []))))


########################
# Body
########################
class Body(Serializable):
    def _fromDict(d):
        typ = d.get("type", "string").lower()
        if typ == "string":
            return BodyInString._fromDict(d)
        elif typ == "asset":
            return BodyInAsset._fromDict(d)

@dataclass(frozen=True)
class BodyInString(Body):
    string:str
    encoding:Encoding=EncodingPlainText()

    def _toDict(self, meth=methodcaller("_toDict")):
        return { "type": "string",
                 "string": self.string,
                 "encoding": meth(self.encoding) }
    def _fromDict(d):
        return BodyInString(string=d.get("string", ""),
                            encoding=Encoding._fromDict(d.get("encoding", {})))


@dataclass(frozen=True)
class BodyInAsset(Body):
    assetId:str

    def _toDict(self, meth=methodcaller("_toDict")):
        return { "type": "asset",
                 "assetId": self.assetId }
    def _fromDict(d):
        return BodyInAsset(assetId=d.get("assetId"))


########################
# Message
########################
@dataclass(frozen=True)
class Message(Serializable):
    header:Header=Header()
    body:Body=BodyInString("")

    def _toDict(self, meth=methodcaller("_toDict")):
        return { "header": meth(self.header),
                 "body": meth(self.body) }
    def _fromDict(d):
        return Message( body=Body._fromDict(d.get("body", {})),
                        header=Header._fromDict(d.get("header", {})))

    def toJsonLines(self, f=toJson):
        return "{header}\n{body}".format(
                              header=f(self.header),
                              body=f(self.body))

    def toMinJsonLines(self):
        return self.toJsonLines(f=toMinJson)

    def fromJsonLines(s):
        # Header is a single JSON line; Body is remainder of string
        h, *t = s.splitlines()
        header = json.loads(h)
        body = json.loads("\n".join(t))
        return Message._fromDict({"header": header, "body": body})

    @contextmanager
    def fromStr(s):
        """Contextmanager that yields a single message. The message should only
           be considered valid within the context's scope. This is necessary in
           order to allow conversion and management of the message resource
           itself.

           Ex:
           with Message.fromStr("...") as msg:
               # do stuff with msg
               ...

           # msg is now invalid
        """
        if s.startswith("---\nEZQ"):
            with convertFromEZQ(Message, s) as convMess:
                 yield convMess
        else:
            yield Message.fromJsonLines(s)


##############################################################################
# Functions
##############################################################################

# FIXME: This ignores the encoding...and that won't do unless
# we are explicitly normalizing the encoding on initial contact somehow


def peekStep(x:Union[Header, Message], n:int=0) -> Step:
    """Returns the nth Step in x"""
    if isinstance(x, Message):
        return peekStep(x.header, n)
    else:
        if len(x.steps) > n:
            return x.steps[n]
        else:
            return Step()


# def popStep(header:Header) -> Tuple[Union[Step, NestedStepListType], Header]:
def popStep(header:Header) -> Tuple[Step, Header]:
    """Returns first Step in header, along with a new Header
       containing the remaining Step's"""
    step = peekStep(header)
    nh = Header(header.encoding, header.steps[1:])
    return (step, nh)


def peekTrigger(x:Union[Header, Message], n:int=0) -> Trigger:
    """Returns the Trigger for the nth Step in x"""
    if isinstance(x, Message):
        return peekTrigger(x.header, n)
    else:
        return peekStep(x, n).trigger


from .ezqconverter import convertFromEZQ
