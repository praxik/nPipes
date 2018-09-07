# -*- mode: python;-*-
from typing import Union, AnyStr, Any
import pathlib
import hashlib
import boto3

from ..outcome import Outcome, Success, Failure
from ..utils.typeshed import pathlike
from .s3path import S3Path




# -> Outcome[str, pathlike]
def downloadFile(remotePath:Union[str, S3Path], localPath:pathlike) -> Outcome:
    """Assumes AWS credentials exist in the environment
       Though the types involved are different, the signature for
       downloadFile, uploadFile, and uploadData follows the same pattern:
       source -> destination -> destination
    """
    s3path = S3Path(remotePath)

    try:
        obj = boto3.resource("s3").Object(s3path.bucket, s3path.key)
        pth = pathlib.Path(localPath)
        # If we already have the current version, don't download it again
        if isCurrent(obj, pth):
            return Success(localPath)
        else:
            preparePath(pth)
            obj.download_file(str(localPath))
            return checkLocal(pth)
    except Exception as err:
        return Failure("Unable to download {}. Reason: {}".format(remotePath, err))


def isCurrent(obj:Any, pth:pathlib.Path) -> bool:
    # obj really should be a boto S3 resource object, but there are currently no
    # type annotations for that.
    if not pth.exists():
        return False
    md5 = fileMd5(str(pth))
    if md5 == obj.e_tag.replace("\"", "") or md5 == obj.metadata.get("md5","").replace("\"", ""):
        return True
    else:
        return False


def fileMd5(file:str) -> str:
    hsh = hashlib.md5()
    with open(file, "rb") as f:
        for b in iter(lambda: f.read(65536), b""):
            hsh.update(b)
    return hsh.hexdigest()


def preparePath(pth:pathlib.Path) -> None:
    pth.parent.mkdir(parents=True, exist_ok=True)


def checkLocal(pth:pathlib.Path) -> Outcome:
    if pth.exists() and pth.stat().st_size > 0:
        return Success(str(pth))
    else:
        return Failure("Error downloading {}; local file does not exist or is empty".format(str(pth)))


# TODO: For both of these upload functions, should probably be doing something sane
# with ContentType and ContentEncoding
# -> Outcome[str, Union[str, S3Path]]
def uploadFile(localPath:pathlike, remotePath:Union[str, S3Path]) -> Outcome:
    s3path = S3Path(remotePath)
    try:
        obj = boto3.resource("s3").Object(s3path.bucket, s3path.key)
        if isCurrent(obj, pathlib.Path(localPath)):
            return Success(localPath)
        else:
            obj.upload_file(str(localPath))
            return Success(localPath)
    except Exception as err:
        return Failure("Unable to upload {} to {}. Reason: {}"
                        .format(str(localPath), str(remotePath), err))


# -> Outcome[str, Union[str, S3Path]]
def uploadData(data:AnyStr, remotePath:Union[str, S3Path]) -> Outcome:
    s3path = S3Path(remotePath)
    try:
        obj = boto3.resource("s3").Object(s3path.bucket, s3path.key)
        if isinstance(data, str):
            bData = data.encode()
        else:
            bData = data
        hsh = hashlib.md5()
        hsh.update(bData)
        md5 = hsh.hexdigest()
        obj.put(Body=bData, ContentMD5=md5)
        return Success(remotePath)
    except Exception as err:
        return Failure("Unable to upload data to {}. Reason: {}"
                        .format(str(s3path), err))