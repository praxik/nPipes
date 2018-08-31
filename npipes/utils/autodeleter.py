from contextlib import contextmanager, ExitStack
from pathlib import Path

@contextmanager
def autoDeleteFile(file):
    """Context manager that deletes a single file when the context ends
    """
    try:
        yield file
    finally:
        if Path(file).is_file():
            Path(file).unlink()


class AutoDeleter(ExitStack):
    """Stack-based context manager that allows incrementally adding files
       to a single logical context. Useful for working with temporary files
       on disk that should be removed at the end of a computation.

       Ex:
       with AutoDeleter() as deleter:
           deleter.add(file_1)
           # ...
           deleter.add(file_2)
           # ...

       # Both file_1 and file_2 automatically get deleted here
    """
    def add(self, file):
        return self.enter_context(autoDeleteFile(file))
