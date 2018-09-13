# -*- mode: python;-*-

from base64 import b64decode, b64encode
from distutils.util import strtobool
import os

from dataclasses import dataclass, field
from typing import Dict

from .serialize         import *
from .message.header    import Command



def strToB64Str(s:str):
    return b64encode(s.encode()).decode()


def b64StrToStr(s:str):
    return b64decode(s.encode()).decode()


@dataclass(frozen=True)
class Configuration(Serializable):
    """Holds configuration information for npipes.processor

    Note:
        Dict (and thus serialized) versions of this class have
        `NPIPES_` prepended to the property names to make them
        easier to store and parse in environment variables. All
        values are also converted to strings for this reason.

        command and producerArgs are serialized
        as base64-encoded json, again to aid in storing the
        values directly in an environment variable.

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

    # def _toDict(self):
    #     return {"NPIPES_command"         : self.command._toDict(),
    #             "NPIPES_lockCommand"     : self.lockCommand,
    #             "NPIPES_commandValidator": self.commandValidator,
    #             "NPIPES_producer"        : self.producer,
    #             "NPIPES_producerArgs"    : self.producerArgs}

    def _toDict(self):
        return {"NPIPES_command"         : b64encode(json.dumps(self.command._toDict()).encode()).decode(),
                "NPIPES_lockCommand"     : str(self.lockCommand),
                "NPIPES_commandValidator": self.commandValidator,
                "NPIPES_producer"        : self.producer,
                "NPIPES_producerArgs"    : strToB64Str(json.dumps(self.producerArgs)) }
                # b64encode(json.dumps(self.producerArgs).encode()).decode()}

    def _fromDict(d):
        cmd = d.get("NPIPES_command", {})
        if isinstance(cmd, str):
            ccmd = json.loads(b64StrToStr(cmd))
        elif isinstance(cmd, dict):
            ccmd = cmd
        else:
            raise TypeError(f"Expected Union[str, Dict] for NPIPES_command, but got {type(cmd)}")
        return Configuration(
                 command          = Command._fromDict(ccmd),
                 lockCommand      = bool(strtobool(d.get("NPIPES_lockCommand", "true"))),
                 commandValidator = d.get("NPIPES_commandValidator", ""),
                 producer         = d.get("NPIPES_producer", ""),
                 producerArgs     = (json.loads(b64StrToStr(d.get("NPIPES_producerArgs",
                                                                  strToB64Str("{}"))))) )
