# -*- mode: python;-*-

from ..message.header import Message
from ..outcome import Outcome, Success, Failure


def sendMessageGet(uri, message:Message) -> Outcome[str, None]:
    pass


def sendMessagePost(uri, message:Message) -> Outcome[str, None]:
    pass
