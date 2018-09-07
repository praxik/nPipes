# -*- mode: python;-*-

import unittest

from npipes.message.header import *
# from npipes.message.message import *
from npipes.assethandlers.s3utils import S3Path

class SerializeTestCase(unittest.TestCase):

    def test_toMinDict1(self):
        step1 = Step("step one", command=Command(["cat", "${bodyfile}"]))
        expected = {"id": "step one",
                    "command": { "arglist": ["cat", "${bodyfile}"]}}
        self.assertEqual(step1._toMinDict(), expected)
        self.assertEqual(step1, Step._fromDict(step1._toMinDict()))

    def test_toMinDict2(self):
        step2 = Step("terminus", trigger=TriggerFilesystem("tests/fsp/results"))
        expected = {"id": "terminus",
                    "trigger": {"type": "Filesystem",
                                "dir": "tests/fsp/results"}}
        self.assertEqual(step2._toMinDict(), expected)
        self.assertEqual(step2, Step._fromDict(step2._toMinDict()))

    def test_toMinDict3(self):
        s3 = S3Asset(S3Path("s3://bucket/key_1"), AssetSettings("asset_a"))
        expected = {"path": "s3://bucket/key_1",
                    "type": "S3",
                    "settings": {"id": "asset_a"}}
        self.assertEqual(s3._toMinDict(), expected)
        self.assertEqual(s3, Asset._fromDict(s3._toMinDict()))

    def test_toMinDict4(self):
        ur = UriAsset("https://domain.com/image",
                      AssetSettings("asset_b", localTarget="urit"))
        expected = {"uri": "https://domain.com/image",
                    "type": "Uri",
                    "settings": {"id": "asset_b",
                                 "localTarget": "urit"}}
        self.assertEqual(ur._toMinDict(), expected)
        self.assertEqual(ur, Asset._fromDict(ur._toMinDict()))

    def test_with(self):
        s = Step( id="list",
                  trigger=TriggerSqs( QueueName("lister_queue"), overflowPath="s3://junk"),
                  command=Command(["ls", "-Fal"],
                                  outputChannel=OutputChannelStdout()),
                  description="Lists files in the local dir" )

        newpath = "foo"
        newdesc = "A new description"
        newargs = ["different", "args", "now"]

        t = s._with([(".command.outputChannel", OutputChannelFile(FilePath(newpath))),
                     (".description", newdesc),
                     (".command.arglist", newargs)])
        self.assertNotEqual(s, t)
        self.assertEqual(t.command.outputChannel.filepath, newpath)
        self.assertEqual(t.description, newdesc)
        self.assertEqual(t.command.arglist, newargs)


if __name__ == '__main__':
    unittest.main()
