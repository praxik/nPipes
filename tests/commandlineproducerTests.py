import npipes.producers.commandline

message_str = '{"encoding":{"type":"plaintext"},"steps":[]}\n{"type":"string","string":"This is the body electric"}'
pr = npipes.producers.commandline.ProducerCommandline(["--message", message_str])
# This version just waits for input on stdin. Try it with
# {}
# {}
# [C-d]
# as the input`
#pr = npipes.producers.commandline.ProducerCommandline([])
for m in pr.messages():
    print(m)
