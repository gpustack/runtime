import pytest

from gpustack_runtime.detector.metax import MetaXDetector


@pytest.mark.skipif(
    not MetaXDetector.is_supported(),
    reason="MetaX GPU not detected",
)
def test_detect():
    det = MetaXDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not MetaXDetector.is_supported(),
    reason="MetaX GPU not detected",
)
def test_get_topology():
    det = MetaXDetector()
    topo = det.get_topology()
    print(topo)
