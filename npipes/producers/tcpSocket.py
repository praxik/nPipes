# -*- mode: python;-*-

from typing import Generator, List, Dict, Any
from dataclasses import dataclass

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from .producer import Producer


def createProducer(cliArgs:List[str], producerArgs:Dict) -> Producer:
    return ProducerTcpSocket(**producerArgs)


@dataclass(frozen=True)
class ProducerTcpSocket(Producer):
    socket:str

    def messages(self) -> Generator[Message, Outcome[Any, Any], None]:
        """
           Please see the WARNING in parent class's docstring.
        """
        pass
        # fake_message = Message() # type: ignore # reason: mypy can't handle Coconut default args

        # while True:
        #     with Message.fromStr("") as msg:
        #         result = yield msg
        #     case result:
        #         match s is Success:
        #             # handle success
        #             pass
        #         match f is Failure:
        #             # handle failure
        #             pass

        #     # Required by intended usage semantics
        #     yield fake_message
