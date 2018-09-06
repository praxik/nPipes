# -*- mode: python;-*-

from typing import Tuple, Type, NewType, Union
import json
import yaml
import pathlib
import secrets
from dataclasses import dataclass
from operator import methodcaller
from contextlib import contextmanager

from ..serialize import Serializable, toJson
from .header import (Header, Step, Trigger, TriggerGet, Uri, QueueName, TriggerSqs,
                     ProtocolEZQ, Command, FilePath, OutputChannelFile,
                     S3Asset, Decompression, AssetSettings )
from .body import Body, BodyInString, BodyInAsset
from .ezqconverter import convertFromEZQ

##############################################################################
# Typealiases
##############################################################################

# class Body(Serializable):
#     def _fromDict(d):
#         case d["type"].lower():
#             match "string":
#                 return BodyInString._fromDict(d)
#             match "asset":
#                 return BodyInAsset._fromDict(d)

# data BodyInString(string:str) from Body:
#     def _toDict(self) = {"type": "string", "string": self.string}
#     def _fromDict(d) = BodyInString(string=d.get("string"))

# data BodyInAsset(assetId:str) from Body:
#     def _toDict(self) = {"type": "asset", "assetId": self.assetId}
#     def _fromDict(d) = BodyInAsset(assetId=d.get("assetId"))


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

    def toJsonLines(self):
        return "{header}\n{body}".format(
                              header=toJson(self.header),
                              body=toJson(self.body))

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
