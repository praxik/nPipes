# -*- mode: python;-*-

import time
from pathlib import Path
from typing import Generator, List, Dict, Any
from dataclasses import dataclass

from ..message.header import Message
from ..outcome import Outcome, Success, Failure, pureOutcome, liftOutcome
from .producer import Producer
from ..utils.typeshed import pathlike


def createProducer(cliArgs:List[str], producerArgs:Dict) -> Producer:
    return ProducerFilesystem(**producerArgs)


@dataclass(frozen=True)
class ProducerFilesystem(Producer):
    dir:pathlike
    removeSuccesses:bool=False
    removeFailures:bool=False
    refreshInterval:float=1.0
    quitWhenEmpty:bool=False

    def messages(self) -> Generator[Message, Outcome[Any, Any], None]:
        """Treats a filesystem directory as a queue, yielding the contents of
           each normal file as a message. Tracks processed files to avoid
           re-processing. "Polls" the "queue" indefinitely. Files are processed
           oldest-first, according to filesystem mtime.

	   When *removeSuccesses* is True, each message file is deleted after
           being processed successfully. Default: False.

           When *removeFailures* is True, each message file that **fails**
           during processing is removed. Default: False.

           Once all existing messages have been exhausted, "polls" *dir* every
           *refreshInterval* seconds for new messages.

           When *quitWhenEmpty* is True, only makes a single pass through the
           directory, does not "poll" for new messages after that, and exits
        """
        fake_message = Message() # type: ignore

        processed:List[Path] = []
        while True:

            files = ( pureOutcome(Path(self.dir).glob("*"))
                      >> liftOutcome(lambda g: filter(lambda f: f.is_file(), g))
                      >> liftOutcome(lambda fs: filter(lambda f: f not in processed, fs))
                      >> liftOutcome(lambda fs: sorted(fs, key=(lambda f: f.stat().st_mtime))) )
            assert(isinstance(files, Success))

            for file in files.value:
                with Message.fromStr(file.read_text()) as msg:
                    result = yield msg
                if isinstance(result, Success):
                    if self.removeSuccesses:
                        file.unlink()
                    else:
                        processed += [file]
                elif isinstance(result, Failure):
                    if self.removeFailures:
                        file.unlink()
                    else:
                        processed += [file]

                # Required by intended usage semantics
                yield fake_message

            if self.quitWhenEmpty:
                break

            time.sleep(self.refreshInterval)
