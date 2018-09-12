#!/usr/bin/env python3
# -*- mode: python;-*-

import json
import logging
import os

from argparse  import ArgumentParser
from base64    import b64decode
from importlib import import_module
from pathlib   import Path

from .configuration      import Configuration
from .processor          import runMessageProducer
from .producers.producer import Producer


def getArgs():
    parser = ArgumentParser()
    parser.add_argument("--config", action="store", default=".npipesrc", type=Path)
    return parser.parse_args()


def getFileConfig(fname):
    configFile = Path(fname)
    if configFile.is_file():
        return json.loads(configFile.read_text())
    else:
        return {}


def getEnv():
    keys = ["NPIPES_command", "NPIPES_lockCommand",
            "NPIPES_commandValidator", "NPIPES_producer",
            "NPIPES_producerArgs"]
    env = {k:os.environ[k] for k in keys if k in os.environ}
    # typecast the special ones
    if "NPIPES_lockCommand" in env:
        lcs = env["NPIPES_lockCommand"].lower()
        env["NPIPES_lockCommand"] = True if lcs == "true" else False
    if "NPIPES_producerArgs" in env:
        pas = env["NPIPES_producerArgs"]
        env["NPIPES_producerArgs"] = json.loads(b64decode(pas).decode())

    return env


def main():
    # Log to stdout as per 12-factor application principles
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s|%(process)d|%(levelname)s|%(name)s: %(message)s")

    args = getArgs()

    # Configuration comes from two places: a file on disk and env vars.
    # Env vars *always* supersede.
    configHash = getFileConfig(args.config)
    configHash.update(getEnv())
    config = Configuration(**configHash)

    # - set up env   ?? Should we lift the full config into the environment...or not?

    producerModule = import_module(config.producer)
    producer = producerModule.createProducer(args, config.producerArgs)

    runMessageProducer(config, producer)


if __name__ == "__main__":
    main()
