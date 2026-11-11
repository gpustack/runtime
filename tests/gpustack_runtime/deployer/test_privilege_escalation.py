from __future__ import annotations as __future_annotations__

import kubernetes
import pytest

from gpustack_runtime.deployer.kubernetes import _set_privilege_escalation


@pytest.mark.parametrize(
    "security, expected",
    [
        (None, False),
        (kubernetes.client.V1SecurityContext(privileged=False), False),
        (kubernetes.client.V1SecurityContext(privileged=True), True),
        (
            kubernetes.client.V1SecurityContext(
                capabilities=kubernetes.client.V1Capabilities(add=["SYS_ADMIN"]),
            ),
            True,
        ),
        (
            kubernetes.client.V1SecurityContext(
                capabilities=kubernetes.client.V1Capabilities(add=["IPC_LOCK"]),
                run_as_user=1000,
            ),
            False,
        ),
    ],
)
def test_privilege_escalation_matches_final_security_context(security, expected):
    container = kubernetes.client.V1Container(name="model", security_context=security)
    _set_privilege_escalation(container)
    assert container.security_context.allow_privilege_escalation is expected
    if security is not None:
        assert container.security_context is security
        assert container.security_context.run_as_user == security.run_as_user
