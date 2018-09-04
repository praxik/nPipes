# -*- mode: python;-*-
from typing import Union, Optional, TypeVar, Type
import pathlib

from ..utils.typeshed import pathlike

T = TypeVar("T", bound="S3Path")

class S3Path:
    """Represents path to an object in AWS S3.
       Two accessible properties: bucket, key; both are strings (str)
       Use str(my_s3_path) to get a nice "s3://bucket/key" string representation.

       Can be instantiated in 3 different ways:

       # copy constructor:
       new_s3_path = S3Path(old_s3_path)

       # bucket and key strings:
       s3_path = S3Path("bucket", "key")

       # bucket and key pathlib.Path's:
       s3_path = S3Path(Path("bucket"), Path("key"))
    """
    def __init__(self, a:Union[T, pathlike], k:Optional[pathlike]=None) -> None:
        if k:
            if isinstance(a, str) or isinstance(a, pathlib.Path):
                self.bucket = str(a)
                self.key = str(k)
            else:
                raise TypeError("When k is provided, a must be either a str or a pathlib.Path")
        else:
            if isinstance(a, S3Path):
                self.bucket = a.bucket
                self.key = a.key
            else:
                _, pth = str(a).split("://")
                bucket, *keyParts = pth.split("/")
                self.bucket = bucket
                self.key = "/".join(keyParts)

    def __eq__(self, other):
        return str(self) == str(other)

    def add(self:T, path:str) -> T:
        """Adds a new "filename" onto end of path
        """
        # This confusing stuff with cls is because we can't refer directly to
        # S3Path as a type, and we can't use the S3Path ctor here and still
        # typecheck.
        cls = self.__class__
        return cls(self.bucket,
                   pathlib.Path(self.key).joinpath(path) )

    def __str__(self):
        return "s3://{}/{}".format(self.bucket, self.key)
