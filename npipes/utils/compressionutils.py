# -*- mode: python;-*-

from gzip import compress, decompress
from base64 import b64encode, b64decode


def toGzB64(s:str) -> bytes:
    """Convert a plain string to a base-64-encoded, gzipped bytes list;
       inverse function of *fromGzB64*
    """
    return b64encode(compress(s.encode("utf-8")))

def fromGzB64(b:bytes) -> str:
    """Convert a base-64-encoded, gzipped bytes list to a plain string;
       inverse function of *toGzB64*
    """
    return decompress(b64decode(b)).decode()
