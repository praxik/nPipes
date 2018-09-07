# -*- mode: python;-*-

from ..message.header import Message
from ..outcome import Outcome, Success, Failure

def sendMessage(topic, message:Message) -> Outcome:
    pass
