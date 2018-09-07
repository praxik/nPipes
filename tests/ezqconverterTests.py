# -*- mode: python;-*-

import unittest
import time
# from npipes.message.ezqconverter import *
from npipes.message.header import Message, convertFromEZQ

class EZQConverterTestCase(unittest.TestCase):

    def test_convertHeaderOnly(self):
        with open("tests/ezq/ezq.ho") as msgfile:
            msgstr = msgfile.read()
            with convertFromEZQ(Message, msgstr) as msg:
                print(msg._toMinDict())


    def test_convertFullMessage(self):
        with open("tests/ezq/ezq.msg") as msgfile:
            msgstr = msgfile.read()
            with convertFromEZQ(Message, msgstr) as msg:
                print(msg._toMinDict())


    def test_convertBigMessage(self):
        with open("tests/ezq/ezq.msg.bigger") as msgfile:
            msgstr = msgfile.read()
            with convertFromEZQ(Message, msgstr) as msg:
                print(msg._toMinDict())


if __name__ == '__main__':
    unittest.main()
