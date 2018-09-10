# -*- mode: python;-*-

# TODO: split this into its own package

from typing import Any, Callable, TypeVar, Union, Generic, Optional, Sequence, Iterator, cast
from contextlib import contextmanager

L = TypeVar("L", covariant=True)
R = TypeVar("R", covariant=True)
T = TypeVar("T", bound="Outcome(Generic[L, R])", covariant=True)
U = TypeVar("U", bound="Outcome(Generic[L, Any])", covariant=True)
C = TypeVar("C")

class Outcome(Generic[L, R]):
    """Base class describing outcome of a computation: Success or Failure.

       Functions returning an Outcome can be chained together so that the
       chain stops at the first step that returns Failure. This can be used
       to provide linear control flow for long sequences of operations that
       might fail at any step.

       The type annotations and `cast`s in the following examples are only
       required if using `mypy` or another typechecker. They can be omitted
       otherwise. The examples are a bit contrived, as normally one would be
       using functions that *return* an Outcome, rather than simply wrapping
       a value directly in Success or Failure.
       Examples:

       # Since each step returns Success, the whole chain runs to the end:
       a:Outcome[str, int] = ( Success(1)
                               >> (lambda x: Success(x+1))
                               >> (lambda y: Success(y *3)) )
       print(isinstance(a, Success))
       # => True
       for value in onSuccess(a):
           print(value)
       # => 6

       # The second step returns Failure, so the computation stops there. Note
       # the `cast` operation; it's entirely to help the typechecker understand
       # our intent here.
       b:Outcome[str, int] = ( cast(Outcome[str, int], Success(1))
                               >> (lambda _: Failure("foo" ))
                               >> (lambda _: Success(0)) )
       print(isinstance(b, Success))
       # => False
       print(isinstance(b, Failure))
       # => True
       for reason in onFailure(b):
           print(reason)
       # => "foo"
    """
    def __init__(self) -> None:
        self.err:L
        self.val:R

    def then(self, f: Callable[[R], 'Outcome[L, Any]']) -> 'Outcome[L, Any]':
        """Monadic bind; chains together computations that consume plain data
           and emit an Outcome. In case of Failure, simply returns Failure.
           In case of Success, applies `f` to the value held by `self`.

           Example:
           # Let a, b, c, and d be functions that consume an int and return
           # either Success or Failure:

           a(0).then(b).then(c).then(d)

           Computation will halt at the first function that returns a Failure.

           `then` is especially useful when combined with partial
           application (see functool.partial) or lambdas.
        """
        if isinstance(self, Success):
            return f(self.val)
        else: # isinstance Failure
            return self

    def __rshift__(self, f: Callable[[R], 'Outcome[L, Any]']) -> 'Outcome[L, Any]':
        """Infix version of `then` using `>>` operator. See documenation for
           `then` for more informaiton.

           Example from `then`, rewritten with `>>`:

           a(0) >> b >> c >> d
        """
        return self.then(f)


class Failure(Outcome[L, R]):
    """Intended for use as the return type of a computation to indicate
       failure to complete the computation. Property `reason` should explain
       the problem. `reason` need not be a string; one could, for example, pass
       `Exceptions` in the Failure.

       Example:

       f = Failure("This failed because it couldn't succeed!")
       f.reason
       # => "This failed because it couldn't succeed!"
    """
    def __init__(self, reason:L) -> None:
        self.err = reason

    @property
    def reason(self) -> L:
        return self.err

    def __repr__(self) -> str:
        return "(Failure, type={}, reason={})".format(type(self.reason), self.reason)


class Success(Outcome[L, R]):
    """Intended for use as the return type of a computation to indicate
       Successful completion. Property `value` contains the "result" of the
       computation.

       Example:

       s = Success(1234)
       s.value
       # => 1234
    """
    def __init__(self, v:R) -> None:
        self.val = v

    @property
    def value(self) -> R:
        return self.val

    def __repr__(self) -> str:
        return "(Success, type={}, value={})".format(type(self.value), self.value)


def onFailure(oc:Outcome[L, R]) -> Iterator[L]:
    """Yields the reason of the failure, iff the thing failed
    """
    # Can't use `if failed(oc):` because mypy can't infer type of `oc` :|
    if isinstance(oc, Failure):
        yield oc.reason


def onSuccess(oc:Outcome[L, R]) -> Iterator[R]:
    """Yields the value of success, iff the thing succeeded
    """
    if isinstance(oc, Success):
        yield oc.value


def failed(oc:Outcome[L, R]) -> bool:
    """Did the operation fail?
    """
    if isinstance(oc, Failure):
        return True
    else:
        return False


def succeeded(oc:Outcome[L, R]) -> bool:
    """Did the operation succeed?
    """
    if isinstance(oc, Success):
        return True
    else:
        return False


def filterMapFailed(f: Callable[[L], C], ocs:Sequence[Outcome[L, R]]) -> Iterator[C]:
    """Map a function *f* over a sequence of Outcomes, applying the function
       only if the Outcome is a Failure, and **omitting** it from the resulting
       sequence if it is a Success.
    """
    for oc in ocs:
        for reason in onFailure(oc):
            yield f(reason)


def filterMapSucceeded(f: Callable[[R], C], ocs:Sequence[Outcome[L, R]]) -> Iterator[C]:
    """Map a function *f* over a sequence of Outcomes, applying the function
       only if the Outcome is a Success, and **omitting** it from the resulting
       sequence if it is a Failure.
    """
    for oc in ocs:
        for value in onSuccess(oc):
            yield f(value)


# def do1() -> Outcome[str, int]:
#     return Success(1)

# def do2(a:int) -> Outcome[str, int]:
#     return Success(a)

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


# OCSI = Outcome[str, int]

# # Shows how to cast "raw" Success or Failure so mypy can check them correctly
# print(list(mapFailed(print, [
#     cast(OCSI, Failure("F1")),
#     cast(OCSI, Success(999)),
#     cast(OCSI, Failure("F2"))])))

# def floaty() -> Outcome[str, float]:
#     return Success(2.1)

# def inty(i:int) -> Outcome[str, int]:
#     return Success(i)


# # Does not typecheck because a float cannot be sent into a function expecting an int
# # WARNING: the opposite is NOT true! If you try to send an int into a function expecting
# # a float, mypy does not complain!
# floaty() >> inty

# Typechecks because the lambda in the middle does the required cast:
# print( floaty()
#        .then((lambda x: cast(Outcome[str, int], Success(int(x)))))
#        .then(inty) )


# def floater(v:float) -> float:
#     return v * 2.1

# a:int = 3

# # You'd think this wouldn't typecheck...but it does. Mypy silently promotes `int` to `float`
# print(floater(a))

# print(Success(23))

# a:Outcome[str, int] = Success(1) >> (lambda x: Success(x+1)) >> (lambda y: Success(y *3))
# print(isinstance(a, Success))
# for value in onSuccess(a):
#     print(value)

# b:Outcome[str, int] = cast(Outcome[str, int], Success(1)) >> (lambda _: Failure("foo" )) >> (lambda _: Success(0))
# print(isinstance(b, Success))
# # => False
# print(isinstance(b, Failure))
# # => True
# for reason in onFailure(b):
#     print(reason)
