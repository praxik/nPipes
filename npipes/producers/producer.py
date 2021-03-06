# -*- mode: python;-*-

from base64 import b64decode
import json
from os import environ

from typing import Generator, List, Dict, Any
from ..outcome import Outcome, Success, Failure
from ..message.header import Message


# FIXME: What is this doing in here?
def envLower(key:str) -> Dict:
    bv = environ.get(key, "e30=")  # "e30=" is base64 "{}"
    return json.loads(b64decode(bv.encode()).decode())


class Producer:
    def messages(self) -> Generator[Message, Outcome[Any, Any], None]:
        """Yields an infinte sequence of *Message*s. Caller is expected to *send*
           either *Success* or *Failure* after each yielded *Message* to allow producers
           to perform any required cleanup associated with the *Message*.

           IMPLEMENTATION WARNING: This method MUST not be implemented as a "normal"
           generator. See comments in the function body for more information.

           USAGE WARNING: this function is intended to be used **only** with a *for* loop
           or *map* operation. DO NOT use the idiom of capturing the value returned
           by *generator.send(foo)*.
        """
        pass
        # # Example implementation:
        #
        # while MessagesAreStillAvailableOrWhatever:
        #     result = ( yield Message(...) )
        #     if type(result) == Success:
        #         # handle Success
        #     else:
        #         # handle Failure
        #
        #     yield Message()  # MUST yield an empty Message after receiving result
        #                      # from previous yield. Why? Because python's generator
        #                      # semantics always yield the next value in response to
        #                      # *send*, which does not work for the way we need to
        #                      # use these generators. Best to think of these as bi-directional
        #                      # streams rather than typical python generators.



# Each Producer submodule MUST have a free function named createProducer,
# with the following signature, and which returns a Producer of the specific
# type.
def createProducer(cliArgs:List[str], producerArgs:Dict) -> Producer:
    """Factory-style module load hook. Allows for the producer contained
       inside the module to be instantiated without having to know its
       name or other details. All we need to know from the outside is the
       name of the module.

       cliArgs is the list of arguments passed on the commandline. Most
       producers won't care about these.

       args is the Dict of arguments specified in the env var
       NPIPES_producerArgs. See the sqs subclass for a good example of
       using these.
    """
    pass
