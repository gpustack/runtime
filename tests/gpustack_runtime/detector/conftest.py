from __future__ import annotations

import pytest

from gpustack_runtime.detector.__types__ import _reset_identity_cache


@pytest.fixture(autouse=True)
def _identity_cache():
    """
    Drop the cached device identities around every test.

    The identity cache is process wide, so a detector that detected an
    identity in one test would serve it in the next one, even when the two
    tests detect different devices.
    """
    _reset_identity_cache()
    yield
    _reset_identity_cache()
