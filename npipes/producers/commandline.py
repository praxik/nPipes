# -*- mode: python;-*-

import argparse
import sys
from typing import Generator, List, Dict, Any

from dataclasses import dataclass

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from .producer import Producer


def createProducer(cliArgs:List[str], producerArgs:Dict) -> Producer:
    return ProducerCommandline(**producerArgs)


@dataclass(frozen=True)
class ProducerCommandline(Producer):
    argv:List[str]

    def messages(self) -> Generator[Message, Outcome[Any, Any], None]:
        """Yields a single *Message* which was passed in on the commandline.

           Please see the WARNING in parent class's docstring.
        """
        fake_message = Message() # type: ignore # reason: mypy can't handle Coconut default args

        with Message.fromStr(self._getCliMessage(self.argv)) as msg:
            yield msg

        # Required by intended usage semantics
        yield fake_message


    def _getCliMessage(self, argv: List[str]) -> str:
        """Returns a message from the commandline args
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--file',  action="store")
        parser.add_argument('--message', action="store")
        args = parser.parse_args(argv)

        if args.file:
            msg = self._readFile(args.file)
        elif args.message:
            msg = args.message
        else:
            # Read from stdin; allows this to be used in shell pipes
            # and with input file redir.
            msg = ""
            for line in sys.stdin:
                msg += line

        print(msg)
        return msg


    def _readFile(self, filename: str) -> str:
        with open(filename) as f:
            return f.read()
