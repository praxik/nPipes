# -*- mode: python;-*-

from typing import Tuple, NamedTuple, List, Dict, Union, Type, Any, Optional, Sequence
import secrets
import zipfile
import gzip
import shutil
import string
import logging
from pathlib import Path
import distutils.dir_util


from ..message.header import (
    Uri, Topic, QueueName, FilePath,
    Asset, S3Asset, UriAsset, AssetSettings, Decompression)

from ..outcome import Outcome, Success, Failure, onFailure, filterMapSucceeded
from ..utils.fp import concurrentMap
from ..utils.iteratorextras import consume
from .s3path import S3Path
from ..utils.track import track
from ..utils.typeshed import pathlike


def localizeAssets(assets:Sequence[Asset]) -> Outcome[str, Sequence[pathlike]]:
    """Localize (ie., download to local node and write to disk) a List of
       Asset's according to the rules for each Asset type.
       Cleans up and returns Failure if localization fails for *any* Asset.
       Returns a list of local targets inside a Success if everything succeeded.
    """
    # concurrentMap could easily support a timeout if we want to add a blanket T.O.
    outcomes = concurrentMap(localizeAsset, assets)
    if any(map(lambda oc: isinstance(oc, Failure), outcomes)):
        logFailures(outcomes, assets)
        # Clean up the successful downloads
        consume(filterMapSucceeded(lambda p: Path(p).unlink(), outcomes))
        return Failure(track("Unable to localize one or more assets"))
    else:
        return Success(list(map(lambda oc: oc.value, outcomes)))


def logFailures(outcomes:Sequence[Outcome[str, pathlike]], assets:Sequence[Asset]) -> None:
    for oc, nm in zip(outcomes, map(str, assets)):
        for reason in onFailure(oc):
            logging.fatal("Fatal error localizing {}: {}".format(nm, reason))


def localizeAsset(asset:Asset) -> Outcome[str, pathlike]:
    """Localize a single Asset
    """
    tempname = genUniqueAssetName(asset)

    return ( localizeAssetTyped(asset, tempname) >>
             (lambda name: decompressIfRequired(name, asset)) >>
             (lambda name: renameToLocalTarget(name, asset)) )

# These localizeAssetTyped patterns *must* return Success(target) if all
# went well. (That allows easier composition and chaining.) We start with
# a fake method so we can selectively attach new @addpattern versions to it
# based on available imports.
def localizeAssetTyped(asset:Asset, target:str) -> Outcome[str, pathlike]:
    if isinstance(asset, S3Asset):
        return localizeS3Asset(asset, target)
    elif isinstance(asset, UriAsset):
        return localizeUriAsset(asset, target)
    else:
        return Failure(track(f"Unknown Asset type {type(asset)}"))

# Boto is large, so don't assume s3 utils are available:
try:
    from . import s3utils
    def localizeS3Asset(asset:S3Asset, target:str) -> Outcome[str, pathlike]:
        """Localize an Asset stored in S3; assumes AWS credentials exist in the
        environment
        """
        return s3utils.downloadFile(asset.path, target)
except ImportError:
    pass # Won't be able to handle S3Assets


try:
    import requests
    def localizeUriAsset(asset:UriAsset, target:str) -> Outcome[str, pathlike]:
        """Localize a standard URI Asset
        """
        # TODO: do some stuff with requests.py
        return Success(target)
except ModuleNotFoundError:
    pass


def genUniqueAssetName(asset:Asset) -> str:
    """Generate unique name for asset while keeping the asset's extension intact
    """
    return "{}.{}".format(randomName(), getAssetRawExt(asset))


def randomName() -> str:
    """Generate a filesystem-appropriate name with low chance of collision
    """
    return secrets.token_hex(8)


def decompressIfRequired(fname:pathlike, asset:Asset) -> Outcome[str, pathlike]:
    """Invokes decompression if asset has been marked for decompression;
       successful Outcome will contain the filename that should be used
       for processing beyond this point
    """
    if asset.settings.decompression.decompress:
        return decompress(fname)
    else:
        return Success(fname)


def decompress(path:pathlike) -> Outcome[str, pathlike]:
    """Chooses and invokes a decompressor based on file extension of path
    """
    pth = Path(path)
    suff = pth.suffix
    if suff == ".zip":
        return decompressZip(path)
    elif suff == ".gz":
        return decompressGzip(path)
    else:
        return Failure(track(f"Unable to determine decompressor from file extension {suff}"))


def decompressZip(file:pathlike) -> Outcome[str, pathlike]:
    """Decompress ZipFile `file` into a unique dir
    """
    tmpdir = randomName()
    try:
        with zipfile.ZipFile(file) as z:
            z.extractall(path=tmpdir)
        Path(file).unlink() # Don't leave the uncompressed archive sitting around
        return Success(tmpdir)
    except Exception as err:
        if Path(tmpdir).exists(): # Clean up if things go wrong
            distutils.dir_util.remove_tree(tmpdir)
            Path(file).unlink()
        return Failure(track(f"Decompression error: {err}"))


def decompressGzip(file:pathlike) -> Outcome[str, pathlike]:
    """Decompress a .gz or .tgz file.
       ONLY decompresses; does NOT explode .tar.gz or .tgz!
    """
    try:
        target = Path(file).stem
        if Path(file).suffix == ".tgz":
            target += ".tar"
        with gzip.open(file, "rb") as src:
            with open(target, "wb+") as dst:
                shutil.copyfileobj(src, dst)
        return Success(target)
    except Exception as err:
        return Failure(track(f"decompressGzip failed with {err}"))


def renameToLocalTarget(fname:pathlike, asset:Asset) -> Outcome[str, pathlike]:
    """Rename an Asset to the requested localTarget
    """
    target = decideLocalTarget(asset)
    targetPath = Path(target)
    try:
        targetPath.parent.mkdir(parents=True, exist_ok=True)
        if targetPath.name is not ".":
            Path(fname).rename(targetPath)
        else:
            # FIXME: I don't like this. Python does not have a good library function
            # to recursively move files. Should write my own to avoid unnecessary disk
            # use in this copy all then delete all process.
            if Path(fname).is_dir():
                distutils.dir_util.copy_tree(str(fname), target)
                distutils.dir_util.remove_tree(str(fname))
            else:
                return Failure(track(f"Unable to rename a file to {targetPath}"))
        return Success(target)
    except Exception as err:
        return Failure(track(f"Error renaming to local target {target}: {err}"))


def getAssetRawExt(asset:Asset) -> str:
    """Returns complete file extension on the *raw* identifier contained in
       Asset; eg., https://my.domain.com/a_file.json.gz  ->  json.gz
    """
    leaf = decideLocalTargetTyped(asset)
    ext = ".".join( leaf.split(".")[1:] )
    return ext


def decideLocalTarget(asset:Asset) -> str:
    if asset.settings.localTarget:
        return asset.settings.localTarget
    else:
        return decideLocalTargetTyped(asset)


def decideLocalTargetTyped(asset:Asset) -> str:
    if isinstance(asset, S3Asset):
        return decideLocalTargetS3Asset(asset)
    elif isinstance(asset, UriAsset):
        return decideLocalTargetUriAsset(asset)
    else:
        return "thisShouldNotExist"


def decideLocalTargetS3Asset(asset:S3Asset) -> str:
    """Returns just the S3 key
    """
    return S3Path(asset.path).key


def decideLocalTargetUriAsset(asset:UriAsset) -> str:
    """Return everything in uri past the last '/'
    """
    return asset.uri.split('/')[-1]
