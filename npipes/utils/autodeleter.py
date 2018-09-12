from contextlib import contextmanager, ExitStack
from pathlib import Path
from typing import Iterator

from npipes.utils.typeshed import pathlike

@contextmanager
def autoDeleteFile(path:pathlike) -> Iterator[pathlike]:
    """Context manager that deletes a single file when the context ends
    """
    try:
        yield path
    finally:
        if Path(path).is_file():
            Path(path).unlink()


class AutoDeleter(ExitStack):
    """Stack manager for auto-deleting files; allows files to be added incrementally.

    Useful for working with temporary files on disk that should be
    removed at the end of a computation.

    Ex:
    with AutoDeleter() as deleter:
        deleter.add(file_1)
        # ...
        deleter.add(file_2)
        # ...
        file_3 = deleter.add("some_file.txt")

    # file_1, file_2, and file_3 are deleted here automatically
    """
    def add(self, path:pathlike) -> pathlike:
        """Returns path after adding it to the auto-deletion context.
        """
        return self.enter_context(autoDeleteFile(path))
