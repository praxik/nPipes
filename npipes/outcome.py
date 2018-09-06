# -*- mode: python;-*-

# FIXME: The type annotations are really doing anything in here.
# It all essentially collapses down to Any's all around. Needs more work.
# This has been done in oc.py, but requires further testing with mypy.

from typing import Any, Callable, TypeVar, Union, Iterator, Sequence

T = TypeVar("T", bound="Outcome")

class Outcome:
    """Base class describing outcome of a computation: Success or Failure.

       Functions returning an Outcome can be chained together so that the
       chain stops at the first step that returns Failure. This can be used
       in place of exceptions to provide more linear control flow.

       Examples:

       # Since each step returns Success, the whole chain runs to the end:
       a = Success(1) >> (lambda x: Success(x+1)) >> (lambda y: Success(y *3))
       isinstance(a, Success)
       # => True
       a.value
       # => 6

       # The second step returns Failure, so the computation stops there:
       b = Success(1) >> (lambda _: Failure("foo" )) >> (lambda _: Success(0))
       isinstance(b, Success)
       # => False
       isinstance(b, Failure)
       # => True
       b.reason
       # => "foo"


    """
    def __init__(self, v: Any=None) -> None:
        self.value = v

    # Monadic bind
    def bind(self: T, f: Callable) -> T:
        """Monadic bind; chains together computations that consume plain data
           and emit an Outcome. In case of Failure, simply returns Failure.
           In case of Success, applies `f` to the value held by `self`.

           Example:
           # Let a, b, c, and d be functions that consume an int and return
           # either Success or Failure:

           a(0).bind(b).bind(c).bind(d)

           Computation will halt at the first function that returns a Failure.

           Bind is significantly more powerful when combined with partial
           application (see functool.partial).
        """
        if isinstance(self, Success):
            return f(self.value)
        else: # isinstance Failure
            return self

    # Override `>>` operator for use as infix bind
    def __rshift__(self: T, f: Callable) -> T:
        """Infix version of bind using `>>` operator. See documenation for
           `bind` for more informaiton.

           Example from `bind`, rewritten with `>>`:

           a(0) >> b >> c >> d
        """
        return self.bind(f)

class Failure(Outcome):
    """Intended for use as the return type of a computation to indicate
       failure to complete the computation. Property `reason` should explain
       the problem.

       Example:

       f = Failure("This failed because it couldn't succeed!")
       f.reason
       # => "This failed because it couldn't succeed!"
    """
    @property
    def reason(self):
        return self.value

class Success(Outcome):
    """Intended for use as the return type of a computation to indicate
       Successful completion. Property `value` contains the "result" of the
       computation.

       Example:

       s = Success(1234)
       s.value
       # => 1234
    """
    pass


def failed(oc:Outcome) -> bool:
    """Did the operation fail?
    """
    if isinstance(oc, Failure):
        return True
    else:
        return False


def succeeded(oc:Outcome) -> bool:
    """Did the operation succeed?
    """
    if isinstance(oc, Success):
        return True
    else:
        return False


def onFailure(oc:Outcome) -> Iterator[Any]:
    """Yields the reason of the failure, iff the thing failed
    """
    # Can't use `if failed(oc):` because mypy can't infer type of `oc` :|
    if isinstance(oc, Failure):
        yield oc.reason


def onSuccess(oc:Outcome) -> Iterator[Any]:
    """Yields the value of success, iff the thing succeeded
    """
    if isinstance(oc, Success):
        yield oc.value


def filterMapFailed(f: Callable, ocs:Sequence[Outcome]) -> Iterator[Any]:
    """Map a function *f* over a sequence of Outcomes, applying the function
       only if the Outcome is a Failure, and **omitting** it from the resulting
       sequence if it is a Success.
    """
    for oc in ocs:
        for reason in onFailure(oc):
            yield f(reason)


def filterMapSucceeded(f: Callable, ocs:Sequence[Outcome]) -> Iterator[Any]:
    """Map a function *f* over a sequence of Outcomes, applying the function
       only if the Outcome is a Success, and **omitting** it from the resulting
       sequence if it is a Failure. This is kind of "bad" behavior for a *map*,
       but it's convenient.
    """
    for oc in ocs:
        for value in onSuccess(oc):
            yield f(value)
