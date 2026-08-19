from types import SimpleNamespace

import pytest

from gpustack_runtime.deployer.__types__ import (
    DEFAULT_TERMINATION_GRACE_PERIOD_SECONDS,
    Container,
    ContainerProfileEnum,
    Deployer,
    WorkloadPlan,
)


def _plan(**kwargs) -> WorkloadPlan:
    return WorkloadPlan(
        name="test",
        labels={},
        containers=[
            Container(
                name="run",
                image="busybox:1.37",
                profile=ContainerProfileEnum.RUN,
            ),
        ],
        **kwargs,
    )


def _recorder() -> SimpleNamespace:
    calls = []
    return SimpleNamespace(
        calls=calls,
        _delete=lambda *args, **kwargs: calls.append((args, kwargs)),
    )


def test_workload_plan_defaults_the_termination_grace_period():
    # An unset termination grace period falls back to the package default.
    plan = _plan()
    plan.validate_and_default()

    assert DEFAULT_TERMINATION_GRACE_PERIOD_SECONDS == 15
    assert plan.termination_grace_period_seconds == 15


def test_workload_plan_keeps_a_zero_termination_grace_period():
    # Zero means "kill immediately", it must survive defaulting.
    plan = _plan(termination_grace_period_seconds=0)
    plan.validate_and_default()

    assert plan.termination_grace_period_seconds == 0


def test_workload_plan_defaults_a_none_termination_grace_period():
    # A JSON payload carrying an explicit null degrades to the default.
    plan = _plan(termination_grace_period_seconds=None)
    plan.validate_and_default()

    assert plan.termination_grace_period_seconds == 15


def test_workload_plan_rejects_a_negative_termination_grace_period():
    plan = _plan(termination_grace_period_seconds=-1)

    with pytest.raises(ValueError, match="grace period"):
        plan.validate_and_default()


def test_deployer_delete_forwards_the_grace_period():
    # The delete facade hands the grace period down to the deployer implementation.
    rec = _recorder()

    Deployer.delete(rec, name="test", grace_period_seconds=5, async_mode=False)

    assert rec.calls == [(("test", None, 5), {})]


def test_deployer_delete_defaults_the_grace_period_to_none():
    # Without an explicit override, the implementation resolves the grace period itself.
    rec = _recorder()

    Deployer.delete(rec, name="test", async_mode=False)

    assert rec.calls == [(("test", None, None), {})]
