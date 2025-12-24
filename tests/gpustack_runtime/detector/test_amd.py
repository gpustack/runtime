import pytest

from gpustack_runtime.detector.amd import AMDDetector


@pytest.mark.skipif(
    not AMDDetector.is_supported(),
    reason="AMD GPU not detected",
)
def test_detect():
    det = AMDDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not AMDDetector.is_supported(),
    reason="AMD GPU not detected",
)
def test_get_topology():
    det = AMDDetector()
    topo = det.get_topology()
    print(topo)
