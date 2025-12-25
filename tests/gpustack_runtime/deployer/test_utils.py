import pytest
from fixtures import load

from gpustack_runtime.deployer.__utils__ import (
    compare_versions,
    correct_runner_image,
    make_image_with,
    replace_image_with,
)


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


@pytest.mark.parametrize(
    "name, kwargs, expected",
    load(
        "test_replace_image_with.json",
    ),
)
def test_replace_image_with(name, kwargs, expected):
    actual = replace_image_with(**kwargs)
    assert actual == expected, (
        f"case {name} expected {expected}, but got {actual} for kwargs: {kwargs}"
    )


@pytest.mark.parametrize(
    "name, kwargs, expected",
    load(
        "test_make_image_with.json",
    ),
)
def test_make_image_with(name, kwargs, expected):
    actual = make_image_with(**kwargs)
    assert actual == expected, (
        f"case {name} expected {expected}, but got {actual} for kwargs: {kwargs}"
    )
