# -*- mode: python;-*-

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from ..message.ezqconverter import toEzqOrJsonLines

import boto3

def sendMessage(topic, message:Message) -> Outcome[str, None]:
    """Publishes message to topic.

    Does not check message size to ensure it will fit within SNS's
    restrictions.
    """
    sns = boto3.client("sns")
    resp = sns.publish(TopicArn=topic,
                       Message=toEzqOrJsonLines(message))
    # SNS doesn't return a success or failure code in the response, so
    # we always treat it as success  :|
    return Success(None)
