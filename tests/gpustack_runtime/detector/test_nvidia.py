import pytest

from gpustack_runtime.detector.nvidia import NVIDIADetector


@pytest.mark.skipif(
    not NVIDIADetector.is_supported(),
    reason="NVIDIA GPU not detected",
)
def test_detect():
    det = NVIDIADetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not NVIDIADetector.is_supported(),
    reason="NVIDIA GPU not detected",
)
def test_get_topology():
    det = NVIDIADetector()
    topo = det.get_topology()
    print(topo)
