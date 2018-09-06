# -*- mode: python;-*-

import os

from dataclasses import dataclass, field
from typing import Dict

from .serialize         import *
from .message.header    import Command

@dataclass(frozen=True)
class Configuration(Serializable):
    """Holds configuration information for npipes.processor

    Note:
        Serialized versions of this class have `NPIPES_` prepended
        to the property names to make them easier to store and parse
        in environment variables.

        The `pid` property is not serialized for obvious reasons. If
        complete reproducibility of Configuration is required, the
        Configuration._with(...) idiom from Serializable can be used.

    Args:
        command (Command): Default Command for the processor
        lockCommand (bool): If True, always use the default Command
            rather than the one indicated in the current Step
        commandValidator (str): Placeholder for future use; will be used
            to call a validation function for commands
        producer (str): Module name of the producer to use with this processor
            instance
        producerArgs (Dict[str, Any]): Dictionary of arguments for creating
            the producer
        pid (int): PID of the current process
    """
    command:Command       = field(default_factory=Command)
    lockCommand:bool      = True
    commandValidator:str  = ""
    producer:str          = ""
    producerArgs:Dict     = field(default_factory=dict)
    pid:int               = field(default_factory=os.getpid)

    def _toDict(self):
        return {"NPIPES_command"         : self.command._toDict(),
                "NPIPES_lockCommand"     : self.lockCommand,
                "NPIPES_commandValidator": self.commandValidator,
                "NPIPES_producer"        : self.producer,
                "NPIPES_producerArgs"    : self.producerArgs}

    def _fromDict(d):
        return Configuration(
                 command          = Command(d["NPIPES_command"]),
                 lockCommand      = d["NPIPES_lockCommand"],
                 commandValidator = d["NPIPES_commandValidator"],
                 producer         = d["NPIPES_producer"],
                 producerArgs     = d.get("NPIPES_producerArgs", {}))
