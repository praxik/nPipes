# -*- mode: python;-*-

from typing import Generator, List, Dict, Any
from dataclasses import dataclass

import boto3

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from .producer import Producer

def createProducer(cliArgs:List[str], producerArgs:Dict) -> Producer:
    return ProducerSqs(**producerArgs)


@dataclass(frozen=True)
class ProducerSqs(Producer):
    queueName:str
    maxNumberOfMessages:int=1

    def messages(self) -> Generator[Message, Outcome[Any, Any], None]:
        """Yields an (infinite) series of *Message*s by polling the specified
           queue. When caller *send*s back *Success*, the message is deleted
           from the queue; otherwise, the message is immediately made visible in
           the queue so further processing attempts can be made.

           WARNING: this function is intended to be used **only** with a *for* loop
           or *map* operation. DO NOT use the idiom of capturing the value returned
           by *generator.send(foo)*.
        """
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.queueName)
        fake_message = Message()

        sqsMsgs: List[boto3.SQS.Message] = []
        while True:
            while not sqsMsgs:
                sqsMsgs = queue.receive_messages(AttributeNames=['VisibilityTimeout'],
                                                 MaxNumberOfMessages=self.maxNumberOfMessages,
                                                 WaitTimeSeconds=20)
            while sqsMsgs: # Allows changing MaxNumberOfMessages to > 1 for batching
                sqsMsg = sqsMsgs.pop()
                with Message.fromStr(sqsMsg.body) as msg:
                    result = yield msg
                if isinstance(result, Success):
                    sqsMsg.delete()
                elif isinstance(result, Failure):
                    sqsMsg.change_visibility(VisibilityTimeout=0)

                yield fake_message
