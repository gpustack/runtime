import pytest

from gpustack_runtime.detector.ascend import AscendDetector


@pytest.mark.skipif(
    not AscendDetector.is_supported(),
    reason="Ascend GPU not detected",
)
def test_detect():
    det = AscendDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not AscendDetector.is_supported(),
    reason="Ascend GPU not detected",
)
def test_get_topology():
    det = AscendDetector()
    topo = det.get_topology()
    print(topo)
