# -*- mode: python;-*-


from typing import Any, Callable, TypeVar, Union, Generic, Optional, Sequence, Iterator, cast
from contextlib import contextmanager

T = TypeVar("T", bound="Outcome")
L = TypeVar("L")
R = TypeVar("R")
C = TypeVar("C")

class Outcome(Generic[L, R]):
    def __init__(self, v:Union[L, R]) -> None:
        self.value = v

    def bind(self: T, f: Callable[[R], T]) -> T:
        if isinstance(self, Success):
            return f(self.value)
        else: # isinstance Failure
            return self

    def __rshift__(self: T, f: Callable[[R], T]) -> T:
        return self.bind(f)


class Failure(Outcome[L, R]):
    @property
    def reason(self) -> Union[L, R]:
        return self.value


class Success(Outcome[L, R]):
    pass


def onFailure(oc:Outcome[L, R]) -> Iterator[L]:
    # Can't use `if failed(oc):` because mypy can't infer type of `oc` :|
    if isinstance(oc, Failure):
        yield oc.reason


def onSuccess(oc:Outcome[L, R]) -> Iterator[R]:
    if isinstance(oc, Success):
        yield oc.value


def failed(oc:Outcome[L, R]) -> bool:
    if isinstance(oc, Failure):
        return True
    else:
        return False


def succeeded(oc:Outcome[L, R]) -> bool:
    if isinstance(oc, Success):
        return True
    else:
        return False


def mapFailed(f: Callable[[L], C], ocs:Sequence[Outcome[L, R]]) -> Iterator[C]:
    for oc in ocs:
        for reason in onFailure(oc):
            yield f(reason)


def mapSucceeded(f: Callable[[R], C], ocs:Sequence[Outcome[L, R]]) -> Iterator[C]:
    for oc in ocs:
        for value in onSuccess(oc):
            yield f(value)


# def do1() -> Outcome[str, int]:
#     return Success(1)

# def do2(a:int) -> Outcome[str, int]:
#     return Success(a)
#     # return Success(a)

# def do3(a:int) -> Outcome[str, int]:
#     return Failure("Error!")


# print(do1().value)
# print(do2(3).value)
# print(do2("three").value)

# t = do3(3)
# if isinstance(t, Failure):
#     print(t.reason)
# else:
#     print("Huh. This shouldn't be called.")

# print((do1() >> do2).value)





# for reason in onFailure(do3(3)):
#     print(reason)

# for value in onSuccess(do1()):
#     print(value)

# for reason in onFailure(do1()):
#     print("Should be unreachable 1")
#     print(reason)

# for value in onSuccess(do3(2)):
#     print("Should be unreachable 2")
#     print(value)


OCSI = Outcome[str, int]

# Shows how to cast "raw" Success or Failure so mypy can check them correctly
print(list(mapFailed(print, [
    cast(OCSI, Failure("F1")),
    cast(OCSI, Success(999)),
    cast(OCSI, Failure("F2"))])))
