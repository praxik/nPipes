# -*- mode: python;-*-

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from ..message.header import Encoding, EncodingPlainText, EncodingGzB64, S3Asset, AssetSettings, Decompression, BodyInString, BodyInAsset
from ..assethandlers.assets import randomName
from ..assethandlers.s3utils import uploadData
from ..assethandlers.s3path import S3Path
from ..message.ezqconverter import toEzqOrJsonLines

from typing import Generator, List
import boto3
import hashlib
import gzip
from base64 import b64encode


def sendMessage(queuename:str, overflowPath:str, message:Message) -> Outcome[str, None]:
    """Sends *message* to SQS queue *queuename*

       If *message* is larger than max size permitted by SQS, the *Body* is
       sent to *overflowPath* in S3 and *message* is altered to reflect the
       change.

       **overflowPath** should be of the form "s3://bucket/my/prefix". The
       actual message body will then be written to
       "s3://bucket/my/prefix/some_random_name.gz"
    """
    try:
        sqs = boto3.resource("sqs")
        queue = sqs.get_queue_by_name(QueueName=queuename)
        messageAutoflowed = overflow(message, overflowPath)
        messageBody = toEzqOrJsonLines(overflow(message, overflowPath))
        # Probably want to maintain an md5 of the overflowed body in
        # the message as well so the receiving side can check that it
        # has everything.
        md5 = hashlib.md5(messageBody.encode("utf-8")).hexdigest()
        response = queue.send_message(MessageBody=messageBody)
        if response.get("MD5OfMessageBody") == md5:
            return Success(None)
        else:
            return Failure("Enqueued message MD5 does not match what was sent")
    except Exception as err:
        return Failure("Unable to send SQS message: {}".format(err))

def overflow(message:Message, overflowPath:str) -> Message:
    # If body is already in an asset, there's nothing we can do here.
    body = Message.body
    if isinstance(body, BodyInString):
        # SQS accepts messages up to 256kB (262,144 B), *including* the SQS
        # header data. The size of the SQS header is unspecified, but is
        # unlikely to be > 2,144B ... probably maybe. Hence the choice of 260000 here:
        overAmount = len(message.toJsonLines().encode()) - 260000
        if overAmount > 0:
            # Message is too big to fit in SQS. Try two things:
            # 1. gzip the base64encode the body string. If that gets us
            #    under the bar, then we go with that. NOTE: gzip, not plain zlib,
            #    so bytes can be written to file as proper .gz
            # 2. If the above fails, we take the gzip bytestring (no b64 stuff), send
            #    it to overflowPath in S3, then re-jigger the Message to reference
            #    a BodyInAsset.
            bodyBytes = body.string.encode()
            gzBodyBytes = gzip.compress(bodyBytes, compresslevel=9)
            b64BodyBytes = b64encode(gzBodyBytes)
            # So...did the compression get us under the threshold?
            if len(b64BodyBytes) < (overAmount - 23): # 23 is the add'l bytes occupied
                                                      # by the now-nec. encoding info
                newBody = BodyInString(b64BodyBytes.decode(), encoding=EncodingGzB64())
                return message._with([(".body", newBody)])
            else:
                # Have to overflow to S3
                fname = randomName()
                s3Path = S3Path(overflowPath).add(fname)
                uploadData(gzBodyBytes, s3Path)
                asset = S3Asset(s3Path, AssetSettings(id="AutoOverflow",
                                                      decompression=Decompression(True)))

                oldsteps = message.header.steps
                oldstep = oldsteps[0]
                newstep = oldstep._with([(".assets", oldstep.assets + [asset])])
                newsteps = [newstep] + oldsteps[1:]

                return message._with([(".header.steps", newsteps),
                                      (".header.body", BodyInAsset(assetId="AutoOverflow"))])
                # We don't check the message at this point to see if we're truly under
                # size now. That's because we're not going to put header information into
                # S3. If someone has dreamed up a workflow that results in a *Header* that
                # is 256kiB...good grief.
        else:
            return message
    else:
        return message
