
from message import *


s = Step( id="list",
          trigger=TriggerSQS( Queuename("lister_queue")),
          command=Command(["ls", "-Fal"],
                          outputChannel=OutputChannelStdout()),
          description="Lists files in the local dir" )


t = s._with([(".command.outputChannel", OutputChannelFile(Filepath("foo"))),
             (".description", "A new description"),
(".command.arglist", ["different", "args", "now"])])

print(t)
