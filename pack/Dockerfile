# Package logic:
# 1. runtime target:
#    - Install tools.
#    - Upgrade GCC if needed.
#    - Install C buildkit.
#    - Upgrade Python if needed.
#    - Install Python buildkit.
#    - Install Platform toolkit.
# 2. gpustack-runtime target.
#    - Fetch the ROCm base image only for later libraries vendoring.
#    - Install gpustack-runtime package from the mounted source code.
#    - Setup entrypoint to gpustack-runtime command.

# Argument usage:
# - PYTHON_VERSION: Version of Python to use.
# - GPUSTACK_RUNTIME_BASE_IMAGE: Base image for the gpustack-runtime stage.
# - GPUSTACK_RUNTIME_ROCM_VERSION: Version of ROCm to vendor libraries from, update this if project dependencies has changed.
ARG PYTHON_VERSION=3.11
ARG GPUSTACK_RUNTIME_BASE_IMAGE=runtime
ARG GPUSTACK_RUNTIME_ROCM_VERSION=6.2.4

FROM ubuntu:22.04@sha256:3c61d3759c2639d4b836d32a2d3c83fa0214e36f195a3421018dbaaf79cbe37f AS runtime
SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

## Install Tools

ENV DEBIAN_FRONTEND=noninteractive \
    LANG='en_US.UTF-8' \
    LANGUAGE='en_US:en' \
    LC_ALL='en_US.UTF-8'

RUN <<EOF
    # Tools

    # Refresh
    apt-get update -y && apt-get install -y --no-install-recommends \
        software-properties-common apt-transport-https \
        ca-certificates gnupg2 lsb-release gnupg-agent \
      && apt-get update -y \
      && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
      && apt-get update -y

    # Install
    apt-get install -y --no-install-recommends \
        ca-certificates build-essential binutils bash openssl \
        curl wget aria2 \
        git git-lfs \
        unzip xz-utils \
        tzdata locales \
        iproute2 iputils-ping ifstat net-tools dnsutils pciutils ipmitool \
        procps sysstat htop \
        tini vim jq bc tree

    # Update locale
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

    # Update timezone
    rm -f /etc/localtime \
        && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
        && echo "Asia/Shanghai" > /etc/timezone \
        && dpkg-reconfigure --frontend noninteractive tzdata

    # Cleanup
    rm -rf /var/tmp/* \
        && rm -rf /tmp/* \
        && rm -rf /var/cache/apt
EOF

## Upgrade GCC if needed

RUN <<EOF
    # GCC

    # Upgrade GCC if the Ubuntu version is lower than 21.04.
    source /etc/os-release
    if (( $(echo "${VERSION_ID} >= 21.04" | bc -l) )); then
        echo "Skipping GCC upgrade for ${VERSION_ID}..."
        exit 0
    fi

    # Install
    apt-get install -y --no-install-recommends \
        gcc-11 g++-11 gfortran-11 gfortran

    # Update alternatives
    if [[ -f /etc/alternatives/gcov-dump ]]; then update-alternatives --remove-all gcov-dump; fi; update-alternatives --install /usr/bin/gcov-dump gcov-dump /usr/bin/gcov-dump-11 10
    if [[ -f /etc/alternatives/lto-dump ]]; then update-alternatives --remove-all lto-dump; fi; update-alternatives --install /usr/bin/lto-dump lto-dump /usr/bin/lto-dump-11 10
    if [[ -f /etc/alternatives/gcov ]]; then update-alternatives --remove-all gcov; fi; update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 10
    if [[ -f /etc/alternatives/gcc ]]; then update-alternatives --remove-all gcc; fi; update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 10
    if [[ -f /etc/alternatives/gcc-nm ]]; then update-alternatives --remove-all gcc-nm; fi; update-alternatives --install /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-11 10
    if [[ -f /etc/alternatives/cpp ]]; then update-alternatives --remove-all cpp; fi; update-alternatives --install /usr/bin/cpp cpp /usr/bin/cpp-11 10
    if [[ -f /etc/alternatives/g++ ]]; then update-alternatives --remove-all g++; fi; update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 10
    if [[ -f /etc/alternatives/gcc-ar ]]; then update-alternatives --remove-all gcc-ar; fi; update-alternatives --install /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-11 10
    if [[ -f /etc/alternatives/gcov-tool ]]; then update-alternatives --remove-all gcov-tool; fi; update-alternatives --install /usr/bin/gcov-tool gcov-tool /usr/bin/gcov-tool-11 10
    if [[ -f /etc/alternatives/gcc-ranlib ]]; then update-alternatives --remove-all gcc-ranlib; fi; update-alternatives --install /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-11 10
    if [[ -f /etc/alternatives/gfortran ]]; then update-alternatives --remove-all gfortran; fi; update-alternatives --install /usr/bin/gfortran gfortran /usr/bin/gfortran-11 10

    # Cleanup
    rm -rf /var/tmp/* \
        && rm -rf /tmp/* \
        && rm -rf /var/cache/apt
EOF

## Install C buildkit

RUN <<EOF
    # C buildkit

    # Install
    apt-get install -y --no-install-recommends \
        make ninja-build pkg-config ccache
    curl --retry 3 --retry-connrefused -fL "https://github.com/Kitware/CMake/releases/download/v3.31.7/cmake-3.31.7-linux-$(uname -m).tar.gz" | tar -zx -C /usr --strip-components 1

    # Install dependencies
    apt-get install -y --no-install-recommends \
        perl-openssl-defaults perl yasm \
        zlib1g zlib1g-dev libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev \
        openssl libssl-dev libsqlite3-dev lcov libomp-dev \
        libblas-dev liblapack-dev libopenblas-dev libblas3 liblapack3 libhdf5-dev \
        libxml2 libxslt1-dev libgl1-mesa-glx libgmpxx4ldbl \
        libncurses5-dev libreadline6-dev libsqlite3-dev \
        liblzma-dev lzma lzma-dev tk-dev uuid-dev libmpdec-dev \
        ffmpeg libjpeg-dev libpng-dev libtiff-dev libwebp-dev \
        libnuma-dev libjemalloc-dev

    # Cleanup
    rm -rf /var/tmp/* \
        && rm -rf /tmp/* \
        && rm -rf /var/cache/apt
EOF

## Upgrade Python if needed

ARG PYTHON_VERSION

ENV PYTHON_VERSION=${PYTHON_VERSION}

RUN <<EOF
    # Python

    if (( $(echo "$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2) == ${PYTHON_VERSION}" | bc -l) )); then
        echo "Skipping Python upgrade for ${PYTHON_VERSION}..."
        if [[ -z "$(ldconfig -v 2>/dev/null | grep libpython${PYTHON_VERSION})" ]]; then
            PYTHON_LIB_PREFIX=$(python3 -c "import sys; print(sys.base_prefix);")
            echo "${PYTHON_LIB_PREFIX}/lib" >> /etc/ld.so.conf.d/python3.conf
            echo "${PYTHON_LIB_PREFIX}/lib64" >> /etc/ld.so.conf.d/python3.conf
            EXPORT_PYTHON_LIB="export LD_LIBRARY_PATH=${PYTHON_LIB_PREFIX}/lib:${PYTHON_LIB_PREFIX}/lib64:\${LD_LIBRARY_PATH}"
            echo "${EXPORT_PYTHON_LIB}" >> /etc/profile
            echo "${EXPORT_PYTHON_LIB}" >> ~/.bashrc
        fi
        exit 0
    fi

    # Add deadsnakes PPA for Python versions
    for i in 1 2 3; do
        add-apt-repository -y ppa:deadsnakes/ppa && break || { echo "Attempt $i failed, retrying in 5s..."; sleep 5; }
    done
    apt-get update -y

    # Install
    apt-get install -y --no-install-recommends \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-dev \
        python${PYTHON_VERSION}-venv \
        python${PYTHON_VERSION}-distutils \
        python${PYTHON_VERSION}-lib2to3 \
        python${PYTHON_VERSION}-gdbm \
        python${PYTHON_VERSION}-tk \
        libibverbs-dev

    # Update alternatives
    if [[ -f /etc/alternatives/python3 ]]; then update-alternatives --remove-all python3; fi; update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1
    if [[ -f /etc/alternatives/python ]]; then update-alternatives --remove-all python; fi; update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1
    curl -sS "https://bootstrap.pypa.io/get-pip.py" | python${PYTHON_VERSION}
    if [[ -f /etc/alternatives/2to3 ]]; then update-alternatives --remove-all 2to3; fi; update-alternatives --install /usr/bin/2to3 2to3 /usr/bin/2to3${PYTHON_VERSION} 1 || true
    if [[ -f /etc/alternatives/pydoc3 ]]; then update-alternatives --remove-all pydoc3; fi; update-alternatives --install /usr/bin/pydoc3 pydoc3 /usr/bin/pydoc${PYTHON_VERSION} 1 || true
    if [[ -f /etc/alternatives/idle3 ]]; then update-alternatives --remove-all idle3; fi; update-alternatives --install /usr/bin/idle3 idle3 /usr/bin/idle${PYTHON_VERSION} 1 || true
    if [[ -f /etc/alternatives/python3-config ]]; then update-alternatives --remove-all python3-config; fi; update-alternatives --install /usr/bin/python3-config python3-config /usr/bin/python${PYTHON_VERSION}-config 1 || true

    # Cleanup
    rm -rf /var/tmp/* \
        && rm -rf /tmp/* \
        && rm -rf /var/cache/apt
EOF

## Install Python buildkit

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PIPX_HOME=/root/.local/share/pipx \
    PIPX_LOCAL_VENVS=/root/.local/share/pipx/venvs \
    UV_NO_CACHE=1 \
    UV_HTTP_TIMEOUT=500 \
    UV_INDEX_STRATEGY="unsafe-best-match"

RUN <<EOF
    # Buildkit

    cat <<EOT >/tmp/requirements.txt
build
cmake<4
ninja<1.11
setuptools<80
setuptools-scm
packaging<25
wheel
pybind11<3
Cython
psutil
pipx
uv
EOT
    pip install -r /tmp/requirements.txt

    # Cleanup
    rm -rf /var/tmp/* \
        && rm -rf /tmp/*
EOF

# Vendor ROCm libraries from ROCm base image,
# now only linux/amd64 is supported.
# Must build on linux/amd64 platform.
FROM --platform=${BUILDPLATFORM} rocm/dev-ubuntu-22.04:${GPUSTACK_RUNTIME_ROCM_VERSION} AS rocm-base

# Stage GPUStack Runtime
#
# Example build command:
#   docker build --progress=plain --platform=linux/amd64 --file=pack/Dockerfile --tag=gpustack/runtime:linux-amd64 --target=gpustack-runtime .
#

FROM ${GPUSTACK_RUNTIME_BASE_IMAGE} AS gpustack-runtime
SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

## Install


RUN --mount=type=bind,target=/workspace/runtime,rw <<EOF
   # Install gpustack-runtime
    echo "Installing GPUStack runtime in the system Python environment..."
    export UV_SYSTEM_PYTHON=1
    export UV_PRERELEASE=allow
    export UV_NO_CACHE=1
    uv pip install \
        /workspace/runtime
    uv pip tree
EOF

## Entrypoint

## Active all AMD devices detection,
## works with (default) ROCm container runtime and privileged mode.
## See https://rocm.docs.amd.com/projects/amdsmi/en/latest/reference/amdsmi-py-api.html.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## - Mount /opt/rocm from the host to detect the correct ROCm version.
## - Mount /usr/share/misc from the host to detect the correct device name.
## E.g. docker run --privileged --runtime amd gpustack/runtime:main gpustack-runtime detect --format json
RUN --mount=type=bind,from=rocm-base,source=/opt/rocm/share,target=/opt/rocm/share,rw <<EOF
    # Reinstall amd-smi
    echo "Installing GPUStack runtime in the system Python environment..."
    export UV_SYSTEM_PYTHON=1
    export UV_PRERELEASE=allow
    export UV_NO_CACHE=1
    uv pip install --no-build-isolation \
        /opt/rocm/share/amd_smi
    uv pip tree
EOF
ENV AMD_VISIBLE_DEVICES="0" \
    ROCM_HOME="/opt/rocm"

## Active all Ascend devices detection,
## works with (default) Ascend container runtime and privileged mode.
## See https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-common/devmanager/dcmi/dcmi_interface_api.h.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## - Mount /usr/local/Ascend/ascend-toolkit to detect the correct CANN version and SoC name.
## E.g. docker run --rm -it --privileged gpustack/runtime:main gpustack-runtime detect --format json
ENV ASCEND_VISIBLE_DEVICES="0" \
    ASCEND_HOME_PATH="/usr/local/Ascend/ascend-toolkit/latest" \
    LD_LIBRARY_PATH="/usr/local/Ascend/driver/lib64/common:/usr/local/Ascend/driver/lib64/driver:/usr/local/Ascend/ascend-toolkit/latest/runtime/lib64:${LD_LIBRARY_PATH}"

## Active all Cambricon devices detection,
## works with (default) Cambricon container runtime and privileged mode.
## See https://github.com/Cambricon/cambricon-k8s-device-plugin/blob/master/device-plugin/pkg/cndev/include/cndev.h,
##     https://github.com/Cambricon/cambricon-k8s-device-plugin/blob/master/device-plugin/pkg/cntopo/include/cntopo.h.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## - Mount /usr/bin/cnmon to detect the correct devices. [TODO, TBD] maybe we can mount /usr/local/neuware to detect the correct Neuware version futher.
## E.g. docker run --rm -it --privileged gpustack/runtime:main gpustack-runtime detect --format json
ENV CAMBRICON_VISIBLE_DEVICES="0"

## Active all Hygon devices detection,
## works with (default) Hygon container runtime and privileged mode.
## See https://github.com/Project-HAMi/dcu-dcgm/blob/master/pkg/dcgm/include/rocm_smi.h.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## E.g. docker run --rm -it --privileged gpustack/runtime:main gpustack-runtime detect --format json
ENV HYGON_VISIBLE_DEVICES="0"

## Active all Iluvatar devices detection,
## works with (default) Iluvatar container runtime and privileged mode.
## See https://gitee.com/deep-spark/ix-device-plugin/blob/master/vendor/gitee.com/deep-spark/go-ixml/pkg/ixml/api.h.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## - Mount /run/udev from the host to detect the correct devices' udev info.
## - Mount /usr/bin/ixsmi to detect the correct devices. [TODO, TBD] maybe we can mount /usr/local/corex to detect the correct CoreX version futher.
## E.g. docker run --rm -it --privileged gpustack/runtime:main gpustack-runtime detect --format json
ENV ILUVATAR_VISIBLE_DEVICES="0"

## Active all MetaX devices detection,
## works with (default) MetaX container runtime and privileged mode.
## See https://developer.metax-tech.com/api/client/document/preview/626/k8s/03_component.html#container-runtime.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## - Mount /opt/maca from the host to support device detecting.
## - Mount /opt/mxdriver/ from the host to support device detecting.
## E.g. docker run --rm -it --privileged -v /opt/mxdriver:/opt/mxdriver -v /opt/maca:/opt/maca gpustack/runtime:main gpustack-runtime detect --format json
ENV METAX_VISIBLE_DEVICES="0" \
    LD_LIBRARY_PATH="/opt/maca/lib:/opt/maca/ompi/lib:/opt/maca/ucx/lib:/opt/mxdriver/lib:${LD_LIBRARY_PATH}"

## Active all MThread devices detection,
## works with (default) MThread container runtime and privileged mode.
## See https://docs.mthreads.com/cloud-native/cloud-native-doc-online/install_guide.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## E.g. docker run --rm -it --privileged gpustack/runtime:main gpustack-runtime detect --format json
ENV MTHREADS_VISIBLE_DEVICES="0" \
    MTHREADS_DRIVER_CAPABILITIES="compute,utility"

## Active all NVIDIA devices detection,
## works with (default) NVIDIA container runtime and privileged mode.
## See https://docs.nvidia.com/deploy/nvml-api/nvml-api-reference.html#nvml-api-reference.
## Options:
## - Mount /sys from the host to detect the correct devices' PCI info.
## E.g. docker run --rm -it --privileged gpustack/runtime:main gpustack-runtime detect --format json
ENV NVIDIA_DISABLE_REQUIRE="true" \
    NVIDIA_VISIBLE_DEVICES="0" \
    NVIDIA_DRIVER_CAPABILITIES="compute,utility"

WORKDIR /
ENTRYPOINT [ "tini", "--" ]
