# -*- mode: python;-*-

import unittest

import os
from itertools import chain
from pathlib import Path

from npipes.processor import *
# from npipes.message.message import *
from npipes.outcome import *
from npipes.message.header import *
from npipes.producers.filesystem import ProducerFilesystem

class ProcessorTestCase(unittest.TestCase):

    def test_runProcess(self):
        command = Command(["ls","tests/static"], outputChannel=OutputChannelStdout())
        res = runProcess(command, input=None, timeout=None)
        self.assertEqual(res.value, "1\n2\n3\n")

    def test_runCommand(self):
        command = Command(["ls","tests/static"], outputChannel=OutputChannelStdout())
        res = runCommand(command, "")
        self.assertEqual(res.value, "1\n2\n3\n")

    def test_scrapeOutput_OCStdout(self):
        command = Command([], outputChannel=OutputChannelStdout())
        self.assertEqual( scrapeOutput(command, "this").value, "this" )

    @unittest.skip("Implement later")
    def test_scrapeOutput_OCFile(self):
        pass


    def setupFancyCommand(self, bodystr):
        command = Command(["zoom", "${asset_b}", "${asset_a}", "${bodycontents}"])
        assets = [ S3Asset("s3://bucket/key_1", AssetSettings("asset_a")),
                   UriAsset("https://domain.com/image",
                            AssetSettings("asset_b", localTarget="urit")) ]
        commandExpectation = Command(["zoom", "urit", "key_1", bodystr])
        return (command, commandExpectation, assets)

    def test_chooseCommand_locked(self):
        config = Configuration()
        command = Command(["not","this","one"])
        co = chooseCommand(config, command)
        self.assertEqual(co, Command())

    def test_chooseCommand_unlocked(self):
        config = Configuration(lockCommand=False)
        command = Command(["yes","this","one"])
        co = chooseCommand(config, command)
        self.assertEqual(co, command)

    def test_expandCommand(self):
        bodystr = "This is the body"
        fancyCommand, fancyCommandExpectation, assets = self.setupFancyCommand(bodystr)
        # last 4 are bodyfile, headerfile, outputfile, pid
        co = expandCommand(fancyCommand, assets, bodystr, "", "", "", os.getpid())
        self.assertEqual(co, fancyCommandExpectation)

    @unittest.skip("Implement later")
    def test_triggerNextStep(self):
        pass

    def test_extractBodyInString(self):
        body = BodyInString("This is it")
        self.assertEqual(extractBodyInString(body), "This is it")

    def test_extractBodyInAsset(self):
        assets = [ UriAsset("https://fake.com",
                            AssetSettings("asset_a",
                                          localTarget="tests/other/asset_in_body.txt")),
                   UriAsset("http://what.no.tls",
                            AssetSettings("asset_b")) ]
        body = BodyInAsset("asset_a")
        text = extractBodyInAsset(body, assets)
        self.assertEqual(text, "Does\nit\npass?\n")

    @unittest.skip("Purely switching logic; always skip")
    def test_extractBody(self):
        pass

    def test_runMessageProducer(self):
        """Tests against a filesystem producer"""

        # prepare test dirs by deleting files in them
        testIn = "tests/fsp"
        testOut = "tests/fsp/results"
        for f in chain(Path(testIn).glob("*"), Path(testOut).glob("*")):
            if f.is_file():
                f.unlink()

        # use a filesystem producer to keep testing local
        producer = ProducerFilesystem("tests/fsp", quitWhenEmpty=True,
                                      removeSuccesses=True, removeFailures=True)
        # write some test messages; notice this setup requires TWO steps:
        # 1. perform a transform on the test data
        # 2. write the output of step 1 to another directory
        # We could bake step 2 into step 1 by redirecting stdout to a file in
        # another directory, but that is less desireable as it would not test
        # the Trigger mechanism implied by runMessageProducer.
        step1 = Step("step one", command=Command(["cat", "${bodyfile}"]))
        step2 = Step("terminus", trigger=TriggerFilesystem("tests/fsp/results"))
        header = Header(steps = [step1, step2])
        # Test messages are named as numbers 1 through 3, and contain
        # "Message x", where x is the same as the file name
        for x in range(1,4):
            msg = Message(header, BodyInString("Message {}".format(x)))
            Path("tests/fsp").joinpath("{}".format(x)).write_text(msg.toJsonLines())

        config = Configuration(lockCommand=False) # putting the command in the messages

        runMessageProducer(config, producer)

        # check result directory
        results = [Message.fromJsonLines(p.read_text()) for p in Path(testOut).glob("*")]

        # Compare list of contents to expected contents
        expectedMessages = list(map(lambda m: Message(Header(steps=[step2]),
                                                      BodyInString("Message {}".format(m))),
                                    range(1,4)))
        for msg in expectedMessages:
            self.assertTrue(msg in results)
        # self.assertEqual(results, expectedMessages)



if __name__ == '__main__':
    unittest.main()
