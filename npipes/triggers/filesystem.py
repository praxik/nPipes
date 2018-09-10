# -*- mode: python;-*-

from pathlib import Path

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from ..assethandlers.assets  import randomName
from ..message.ezqconverter import toEzqOrJsonLines


def sendMessage(dir, message:Message) -> Outcome[str, None]:
    """Writes a uniquely-named file to the filesystem directory *dir*.
       This can be used to trigger another processor running a
       *ProducerFilesystem* on a shared filesystem, or anything else using
       a filesystem-based event mechanism.
    """
    try:
        messageStr = toEzqOrJsonLines(message)
        Path(dir).joinpath(randomName()).write_text(messageStr)
        return Success(None)
    except Exception as e:
        return Failure("TriggerFilesystem.sendMessage: {}".format(e))
