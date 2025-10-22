import pytest
from fixtures import load

from gpustack_runtime.deployer.__utils__ import compare_versions, correct_runner_image


@pytest.mark.parametrize(
    "name, kwargs, expected",
    load(
        "test_compare_versions.json",
    ),
)
def test_compare_versions(name, kwargs, expected):
    actual = compare_versions(**kwargs)
    assert actual == expected, (
        f"case {name} expected {expected}, but got {actual} for kwargs: {kwargs}"
    )


@pytest.mark.parametrize(
    "name, kwargs, expected",
    load(
        "test_correct_runner_image.json",
    ),
)
def test_correct_runner_image(name, kwargs, expected):
    actual = correct_runner_image(**kwargs)
    assert list(actual) == expected, (
        f"case {name} expected {expected}, but got {actual} for kwargs: {kwargs}"
    )
