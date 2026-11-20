# Builds the Pod through the deployer's own creation path, without a live
# Kubernetes cluster.
# ruff: noqa: SLF001

import kubernetes.client
import kubernetes.client.exceptions
import pytest

from gpustack_runtime.deployer.__types__ import (
    Container,
    ContainerProfileEnum,
    Deployer,
)
from gpustack_runtime.deployer.kubernetes import (
    KubernetesDeployer,
    KubernetesWorkloadPlan,
)


def _create_pod(monkeypatch, annotations):
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kubernetes.get_resource_injection_policy",
        lambda *_args: "env",
    )
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kubernetes._resolve_runtime_class_name",
        lambda *_args: None,
    )

    class _FakeCoreV1Api:
        def __init__(self, client=None):
            pass

        def read_namespaced_pod(self, name, namespace):
            raise kubernetes.client.exceptions.ApiException(status=404)

        def create_namespaced_pod(self, namespace, body):
            return body

    monkeypatch.setattr(kubernetes.client, "CoreV1Api", _FakeCoreV1Api)

    deployer = object.__new__(KubernetesDeployer)
    Deployer.__init__(deployer, "test")
    deployer._materials = {}
    deployer._client = None
    deployer._node_name = None
    deployer._image_pull_secret = None
    deployer._mutate_create_pod = lambda pod: pod

    workload = KubernetesWorkloadPlan(
        name="test",
        namespace="default",
        annotations=annotations,
        containers=[
            Container(
                name="run",
                image="busybox:1.37",
                profile=ContainerProfileEnum.RUN,
            ),
        ],
    )
    workload.validate_and_default()
    return workload, deployer._create_pod(workload, {})


@pytest.mark.parametrize(
    "annotations",
    [None, {"device.gpustack.ai/accelerator.preferred-id": "GPU-0,GPU-1"}],
)
def test_kubernetes_pod_carries_the_plan_annotations(monkeypatch, annotations):
    workload, pod = _create_pod(monkeypatch, annotations)

    for key, value in (annotations or {}).items():
        assert pod.metadata.annotations[key] == value
    # The deployer's own container annotations never leak back into the plan.
    assert workload.annotations == annotations
