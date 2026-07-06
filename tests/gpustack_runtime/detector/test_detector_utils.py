from __future__ import annotations

import sys
from typing import ClassVar

import pytest

from gpustack_runtime.detector.__utils__ import clone_exception


# --------------------------------------------------------------------------- #
# Exception classes that mimic the real binding error types, to exercise the   #
# constructor shapes clone_exception has to survive.                           #
# --------------------------------------------------------------------------- #
class RocmLikeError(Exception):
    """Custom __init__ that does NOT call super().__init__ (like ROCMSMIError)."""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"rocm {self.value}"


class DcmiLikeError(Exception):
    """Custom __new__(cls, value) that requires a positional arg (like DCMIError)."""

    _mapping: ClassVar[dict] = {}

    def __new__(cls, value):
        target = DcmiLikeError._mapping.get(value, cls)
        obj = Exception.__new__(target)
        obj.value = value
        return obj

    def __str__(self):
        return f"dcmi {self.value}"


class AmdSmiLikeError(Exception):
    """super().__init__() with no args -> args == (); state lives in __dict__.

    This is the case that `type(exc)(*exc.args)` CANNOT reconstruct.
    """

    def __init__(self, err_code):
        super().__init__()
        self.err_code = err_code
        self.err_info = f"info-{err_code}"

    def __str__(self):
        return f"Error code: {self.err_code} | {self.err_info}"


def _tb_len(exc: BaseException) -> int:
    n = 0
    tb = exc.__traceback__
    while tb:
        n += 1
        tb = tb.tb_next
    return n


ALL_CASES = [
    RocmLikeError(-99997),
    DcmiLikeError(-8007),
    AmdSmiLikeError(43),
    ValueError("boom"),
    RuntimeError("multi", "arg", 3),
]


# --------------------------------------------------------------------------- #
# Correctness: the clone must be usable in place of the original.              #
# --------------------------------------------------------------------------- #
def test_clone_preserves_type():
    for exc in ALL_CASES:
        clone = clone_exception(exc)
        assert type(clone) is type(exc)


def test_clone_is_a_distinct_object():
    for exc in ALL_CASES:
        clone = clone_exception(exc)
        assert clone is not exc


def test_clone_preserves_args_and_attributes_and_str():
    for exc in ALL_CASES:
        clone = clone_exception(exc)
        assert clone.args == exc.args
        assert clone.__dict__ == exc.__dict__
        # str() relies on the preserved state; it must render identically.
        assert str(clone) == str(exc)


def test_clone_still_matches_except_clause():
    # A caller doing `except RocmLikeError` must still catch the clone.
    with pytest.raises(RocmLikeError) as info:
        raise clone_exception(RocmLikeError(-1))
    assert info.value.value == -1


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Exception.add_note() was added in Python 3.11",
)
def test_clone_preserves_notes():
    # __notes__ (add_note, 3.11+) lives in __dict__, so it is carried over by
    # the __dict__ copy without any special handling.
    exc = ValueError("boom")
    exc.add_note("first note")
    exc.add_note("second note")
    clone = clone_exception(exc)
    assert clone.__notes__ == ["first note", "second note"]


def test_clone_handles_amdsmi_style_empty_args():
    # The whole reason we don't use `type(exc)(*exc.args)`: it blows up here.
    exc = AmdSmiLikeError(7)
    assert exc.args == ()
    clone = clone_exception(exc)
    assert clone.err_code == 7
    assert clone.err_info == "info-7"


# --------------------------------------------------------------------------- #
# The point of the helper: cloning + re-raising must NOT grow the cached       #
# exception's traceback (this is the gpustack/gpustack#5342 regression).       #
# --------------------------------------------------------------------------- #
def test_reraising_clones_does_not_grow_cached_traceback():
    # Seed a traceback on the "cached" exception, like a first failed init.
    with pytest.raises(RocmLikeError) as info:
        raise RocmLikeError(-99997)
    cached = info.value

    baseline = _tb_len(cached)

    raised = []
    for _ in range(500):
        with pytest.raises(RocmLikeError) as info:
            raise clone_exception(cached) from None
        raised.append(info.value)

    # The cached object's traceback is untouched by the re-raises.
    assert _tb_len(cached) == baseline
    # Every raise produced a fresh object, never the cached one.
    assert all(r is not cached for r in raised)


def test_contrast_reraising_same_object_would_grow_traceback():
    # Demonstrates the bug the helper fixes: re-raising the SAME object grows
    # its traceback unbounded. (Sanity check that our test measures the right
    # thing; not testing clone_exception here.)
    with pytest.raises(RocmLikeError) as info:
        raise RocmLikeError(-99997)
    cached = info.value

    before = _tb_len(cached)
    for _ in range(50):
        with pytest.raises(RocmLikeError):
            raise cached
    assert _tb_len(cached) > before


def test_clone_severs_cause_and_context_when_raised_from_none():
    with pytest.raises(ValueError, match="x") as info:
        raise clone_exception(ValueError("x")) from None
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True
