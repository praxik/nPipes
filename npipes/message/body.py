# -*- mode: python;-*-

from operator import methodcaller

from dataclasses import dataclass

from ..serialize import Serializable
from .header import Encoding, EncodingPlainText, EncodingGzB64

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
