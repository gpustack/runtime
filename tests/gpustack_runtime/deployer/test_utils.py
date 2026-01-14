import pytest
from fixtures import load

from gpustack_runtime.deployer.__utils__ import (
    compare_versions,
    correct_runner_image,
    load_yaml_or_json,
)
from tests.gpustack_runtime.deployer.fixtures import resolve_load_path


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
        "test_load_yaml_or_json.json",
    ),
)
def test_load_yaml_or_json(name, kwargs, expected):
    kwargs["path"] = resolve_load_path(kwargs["path"])
    actual = load_yaml_or_json(**kwargs)
    assert actual == expected, (
        f"case {name} expected {expected}, but got {actual} for kwargs: {kwargs}"
    )
