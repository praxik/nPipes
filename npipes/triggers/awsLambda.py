# -*- mode: python;-*-

from ..message.header import Message
from ..outcome import Outcome, Success, Failure


def sendMessage(name, message:Message) -> Outcome:
    # TODO: directly invoke the Lambda function; note that this is entirely
    # orthogonal to running *on* Lambda.
    pass
