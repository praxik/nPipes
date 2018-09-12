import inspect

def track(s:str) -> str:
    """Prepends s with the filename and linenumber of call site
    """
    frame = inspect.currentframe()
    if frame:
        prev = frame.f_back
        if prev:
            return f"[{prev.f_code.co_filename}:{prev.f_lineno}] {s}"
    return s


# # Using outcome:
# def track(s:str) -> str:
#     """Prepends s with the filename and linenumber of call site
#     """
#     for prevFrame in onSuccess(pureOutcome(inspect.currentframe())
#                            >> liftOutcome(lambda fr: fr.f_back)):
#         return f"[{prevFrame.f_code.co_filename}:{prevFrame.f_lineno}] {s}"
#     return s


# # Using pymaybe:
# def track(s:str) -> str:
#     """Prepends s with the filename and linenumber of call site
#     """
#     prevFrame = maybe(inspect.currentframe()).f_back
#     if prevFrame:
#         return f"[{prevFrame.f_code.co_filename}:{prevFrame.f_lineno}] {s}"
#     return s
