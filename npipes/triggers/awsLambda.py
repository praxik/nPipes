# -*- mode: python;-*-

from ..message.message import Message
from ..outcome import Outcome, Success, Failure


def sendMessage(name, message) -> Outcome:
    # TODO: directly invoke the Lambda function; note that this is entirely
    # orthogonal to running *on* Lambda.
    pass