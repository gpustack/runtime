import pytest
from fixtures import load

from gpustack_runtime.deployer.__utils__ import render_image


@pytest.mark.parametrize(
    "name, kwargs, expected",
    load(
        "test_render_image.json",
    ),
)
def test_render_image(name, kwargs, expected):
    actual = render_image(**kwargs)
    assert actual == expected, (
        f"case {name} expected {expected}, but got {actual} for kwargs: {kwargs}"
    )
