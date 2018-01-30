from processor import *
from message import *
from outcome import *

print("run")
command = Command(["ls","tests/static"], outputChannel=OutputChannelStdout())
res = run(command, input=None, timeout=None)
if (res.value == b"1\n2\n3\n"):
    print("Pass")
else:
    print("Fail: {}".format(res.value))


res >> (lambda x: scrapeOutput(command, x)) >> print


print("runCommand")
r2 = runCommand(command, "")
if (r2.value == b"1\n2\n3\n"):
    print("Pass")
else:
    print("Fail: {}".format(r2.value))
