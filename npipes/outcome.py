# -*- mode: python;-*-

from typing import Any, Callable, TypeVar

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
        """Monadic bind; chains together computations that comsume plain data
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
