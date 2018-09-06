from itertools import *
import collections

# This function comes from
# https://docs.python.org/3/library/itertools.html#itertools-recipes

def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is None, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)


################################################################################

# NOT from itertools-recipes:

def each(f, iterator, n=None):
    """Like map, but for side effects only, and allows stopping after
       n items. (forM in Haskell)
    """
    consume(map(f, iterator), n)
