# GPUStack Runtime

[![License](https://img.shields.io/github/license/gpustack/runtime?logo=apache&label=License)](./LICENSE)
[![PyPI](https://img.shields.io/pypi/v/gpustack-runtime?logo=pypi&label=PyPI)](https://pypi.org/project/gpustack-runtime/)
[![Latest Release](https://img.shields.io/github/v/release/gpustack/runtime?logo=semanticrelease&label=Release&include_prereleases)](https://github.com/gpustack/runtime/releases/latest)
[![Docker Pulls](https://img.shields.io/docker/pulls/gpustack/runtime?logo=docker&logoColor=fff&label=Docker%20Pulls)](https://hub.docker.com/r/gpustack/runtime)
[![Ask DeepWiki](https://img.shields.io/badge/Ask_DeepWiki-purple?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAACXBIWXMAAAPoAAAD6AG1e1JrAAACIUlEQVRYw+2XP2gUQRTGv7d3JhYWQawkIBaCVWzETos0gmAnVoKNTSoLK0E7EQQrC20sBVFBtLPQRgTBtBaKjYgIQUSxiJq7fT+LvCGPJZdNLt4dQj5Y3u7sznzfzLw/s9IO/icANiniCujEfWdsQgBLxAbsbraPZcmBfcAt4DNwKQsZNfEUsAB8YRV12LfA6ZHuedg7AO7ed/c+UBcbQhbiu+6wXIM6EnZWkszMJe2Ke0n6I2la0v51hFeSMLN6OwIK6jJwEBdUYb2MAxRSz6sY4geiahFgLe1Hwq6YWe3us8AN4FQQU4QM64SPY69Xyr43fADgNjAPXHb3peSsD4FDQ0VLEjAHvIhBPS6AHvA1bBO9JPAXcHFbIRtJ5yzwIQZ9CZwAusDJENIkLqsG8DpNyIZJwSUk9wLHgakcesC1JCCjiHm1kYDNOEjptCxpSVK/8f73OFLxPLAYM3oCzEX7MXf/lJbc/8kWpA4HgQdpYI9IWAbeh83whi84cHXL+58EPBoQhnmm94EzwE3gZ2p/Dhzdbhg+TaRr01x7ftb4/jBwFzjXrCvDpmLW9crVLNeR9CaapoGemb2TdCERW1tNaBNQ8jktwmozqwtpqRNtdWAjARYk39IS12ZWRX7vmJmA71nQZgi36gN7gCvAj5xc3P0jcH7kx7Ik5IC73wsh14GZsZySow5002l4Jr0b6+m4eSyvpAn8lEzsx2QHo8RfUrlN+uPq4ksAAAAASUVORK5CYII=)](https://deepwiki.com/gpustack/runtime)

GPUStack Runtime offers a unified interface for detecting GPU resources and managing GPU workloads.

## Features

- Detect a wide range of GPU and accelerator resources:

  - AMD GPU
  - Ascend NPU
  - Cambricon MLU
  - Hygon DCU
  - Iluvatar GPU
  - MetaX GPU
  - Moore Threads GPU
  - NVIDIA GPU
  - T-Head PPU

- Manage GPU workloads on the following platforms:

  - Docker
  - Kubernetes
  - Podman (>=4.9, experimental support via the `CONTAINER_HOST=http+unix:///path/to/podman/socket` environment variable)

Contributions to support additional GPU resources are welcome!

## Installation

```bash
pip install gpustack-runtime
```

## Documentation

The API reference renders from docstrings. Build the site locally with `make docs` and open
`site/index.html`.

## Contributing

Contributions are welcome. Commits must carry a `Signed-off-by` line certifying the
[Developer Certificate of Origin](./DCO).

Using GPUStack Runtime in production? Add your company or project to [ADOPTERS.md](./ADOPTERS.md)
via pull request.

## License

Copyright (c) 2026 The GPUStack Authors. Licensed under the
[Apache License 2.0](./LICENSE).
