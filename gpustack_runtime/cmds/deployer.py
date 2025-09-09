from __future__ import annotations

import contextlib
import json
import os
import platform
import time
from argparse import REMAINDER
from typing import TYPE_CHECKING

from gpustack_runner import container_backend_visible_devices_env, list_service_runners

from gpustack_runtime.detector import detect_backend, detect_devices

from ..deployer import (  # noqa: TID252
    Container,
    ContainerEnv,
    ContainerExecution,
    ContainerMount,
    WorkloadPlan,
    WorkloadStatus,
    create_workload,
    delete_workload,
    get_workload,
    list_workloads,
    logs_workload,
)
from .__types__ import SubCommand

if TYPE_CHECKING:
    from argparse import Namespace, _SubParsersAction


class CreateRunnerWorkloadSubCommand(SubCommand):
    """
    Command to create a runner workload deployment.
    """

    backend: str
    service: str
    version: str
    name: str
    volume: str
    extra_args: list[str]

    @staticmethod
    def register(parser: _SubParsersAction):
        deploy_parser = parser.add_parser(
            "create-runner",
            help="create a runner workload deployment",
        )

        deploy_parser.add_argument(
            "--backend",
            type=str,
            help="backend to use (default: detect from current environment)",
        )

        deploy_parser.add_argument(
            "service",
            type=str,
            help="service of the runner",
        )

        deploy_parser.add_argument(
            "version",
            type=str,
            help="version of the runner",
        )

        deploy_parser.add_argument(
            "volume",
            type=str,
            help="volume to mount",
        )

        deploy_parser.add_argument(
            "extra_args",
            nargs=REMAINDER,
            help="extra arguments for the runner",
        )

        deploy_parser.set_defaults(func=CreateRunnerWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.backend = args.backend or detect_backend()
        self.service = args.service
        self.version = args.version
        self.name = f"{args.service}-{args.version}".lower().replace(".", "-")
        self.volume = args.volume
        self.extra_args = args.extra_args

        if not self.name or not self.volume:
            msg = "The name and volume arguments are required."
            raise ValueError(msg)

    def run(self):
        arch_name = platform.machine()
        match arch_name:
            case "amd64" | "x86_64" | "AMD64":
                arch_name = "amd64"
            case "arm64" | "aarch64" | "ARM64":
                arch_name = "arm64"
            case _:
                msg = f"Unsupported architecture: {arch_name}"
                raise ValueError(msg)

        runners = list_service_runners(
            backend=self.backend,
            service=self.service,
            service_version_prefix=self.version,
            platform=f"linux/{arch_name}",
        )
        if not runners:
            msg = (
                f"No runners found for service '{self.service}' "
                f"with version prefix '{self.version}' "
                f"and backend '{self.backend}'."
            )
            raise ValueError(msg)

        runner = None
        if len(runners[0].versions[0].backends[0].versions) > 1:
            rt_v = "0.0"
            if devs := detect_devices():
                rt_v = devs[0].runtime_version
            for v in runners[0].versions[0].backends[0].versions:
                if v.version > rt_v:
                    continue
                runner = v.variants[0].platforms[0]
                break
        if not runner:
            runner = (
                runners[0].versions[0].backends[0].versions[0].variants[0].platforms[0]
            )

        print(f"Using runner image: {runner.docker_image}")
        envs = []
        if self.backend:
            envs.append(
                ContainerEnv(
                    name=container_backend_visible_devices_env(self.backend),
                    value="all",
                ),
            )
        envs.extend(
            [
                ContainerEnv(
                    name=name,
                    value=value,
                )
                for name, value in os.environ.items()
                if not name.startswith(
                    (
                        "PATH",
                        "HOME",
                        "LANG",
                        "PWD",
                        "SHELL",
                        "LOG",
                        "XDG",
                        "SSH",
                        "LC",
                        "LS",
                        "_",
                        "USER",
                        "TERM",
                        "LESS",
                        "SHLVL",
                        "DBUS",
                        "OLDPWD",
                        "MOTD",
                        "LD",
                        "LIB",
                        "GPUSTACK_",
                    ),
                )
            ],
        )
        mounts = [
            ContainerMount(
                path=self.volume,
            ),
        ]
        execution = None
        if self.extra_args:
            execution = ContainerExecution(
                command=self.extra_args,
            )
        plan = WorkloadPlan(
            name=self.name,
            host_network=True,
            containers=[
                Container(
                    image=runner.docker_image,
                    name="default",
                    envs=envs,
                    mounts=mounts,
                    execution=execution,
                ),
            ],
        )
        create_workload(plan)
        print(f"Created workload '{self.name}'.")

        try:
            print("\033[2J\033[H", end="")
            logs_stream = logs_workload(self.name, tail=-1, follow=True)
            with contextlib.closing(logs_stream) as logs:
                for line in logs:
                    print(line.decode("utf-8").rstrip())
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


class CreateWorkloadSubCommand(SubCommand):
    """
    Command to create a workload deployment.
    """

    backend: str
    name: str
    image: str
    volume: str
    extra_args: list[str]

    @staticmethod
    def register(parser: _SubParsersAction):
        deploy_parser = parser.add_parser(
            "create",
            help="create a workload deployment",
        )

        deploy_parser.add_argument(
            "--backend",
            type=str,
            help="backend to use (default: detect from current environment)",
        )

        deploy_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        deploy_parser.add_argument(
            "image",
            type=str,
            help="image to deploy (should be a valid Docker image)",
        )

        deploy_parser.add_argument(
            "volume",
            type=str,
            help="volume to mount",
        )

        deploy_parser.add_argument(
            "extra_args",
            nargs=REMAINDER,
            help="extra arguments for the workload",
        )

        deploy_parser.set_defaults(func=CreateWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.backend = args.backend or detect_backend()
        self.name = args.name
        self.image = args.image
        self.volume = args.volume
        self.extra_args = args.extra_args

        if not self.name or not self.image or not self.volume:
            msg = "The name, image, and volume arguments are required."
            raise ValueError(msg)

    def run(self):
        envs = []
        if self.backend:
            envs.append(
                ContainerEnv(
                    name=container_backend_visible_devices_env(self.backend),
                    value="all",
                ),
            )
        envs.extend(
            [
                ContainerEnv(
                    name=name,
                    value=value,
                )
                for name, value in os.environ.items()
                if not name.startswith(
                    (
                        "PATH",
                        "HOME",
                        "LANG",
                        "PWD",
                        "SHELL",
                        "LOG",
                        "XDG",
                        "SSH",
                        "LC",
                        "LS",
                        "_",
                        "USER",
                        "TERM",
                        "LESS",
                        "SHLVL",
                        "DBUS",
                        "OLDPWD",
                        "MOTD",
                        "LD",
                        "LIB",
                        "GPUSTACK_",
                    ),
                )
            ],
        )
        mounts = [
            ContainerMount(
                path=self.volume,
            ),
        ]
        execution = None
        if self.extra_args:
            execution = ContainerExecution(
                command=self.extra_args,
            )
        plan = WorkloadPlan(
            name=self.name,
            host_network=True,
            containers=[
                Container(
                    image=self.image,
                    name="default",
                    envs=envs,
                    mounts=mounts,
                    execution=execution,
                ),
            ],
        )
        create_workload(plan)
        print(f"Created workload '{self.name}'.")

        try:
            print("\033[2J\033[H", end="")
            logs_stream = logs_workload(self.name, tail=-1, follow=True)
            with contextlib.closing(logs_stream) as logs:
                for line in logs:
                    print(line.decode("utf-8").rstrip())
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


class DeleteWorkloadSubCommand(SubCommand):
    """
    Command to delete a workload deployment.
    """

    name: str

    @staticmethod
    def register(parser: _SubParsersAction):
        delete_parser = parser.add_parser(
            "delete",
            help="delete a workload deployment",
        )

        delete_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        delete_parser.set_defaults(func=DeleteWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.name = args.name

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        st = delete_workload(self.name)
        if st:
            print(f"Deleted workload '{self.name}'.")
        else:
            print(f"Workload '{self.name}' not found.")


class GetWorkloadSubCommand(SubCommand):
    """
    Command to get the status of a workload deployment.
    """

    name: str
    json: bool = False
    watch: int = 0

    @staticmethod
    def register(parser: _SubParsersAction):
        get_parser = parser.add_parser(
            "get",
            help="get the status of a workload deployment",
        )

        get_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        get_parser.add_argument(
            "--json",
            action="store_true",
            help="output in JSON format",
        )

        get_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="continuously watch for the workload in intervals of N seconds",
        )

        get_parser.set_defaults(func=GetWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.name = args.name
        self.json = args.json
        self.watch = args.watch

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        try:
            while True:
                sts: list[WorkloadStatus] = [get_workload(self.name)]
                print("\033[2J\033[H", end="")
                if self.json:
                    print(format_workloads_json(sts))
                else:
                    print(format_workloads_table(sts))
                if not self.watch:
                    break
                time.sleep(self.watch)
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


class ListWorkloadsSubCommand(SubCommand):
    """
    Command to list all workload deployments.
    """

    labels: dict[str, str] | None = None
    json: bool = False
    watch: int = 0

    @staticmethod
    def register(parser: _SubParsersAction):
        list_parser = parser.add_parser(
            "list",
            help="list all workload deployments",
        )

        list_parser.add_argument(
            "--labels",
            type=lambda s: dict(item.split("=") for item in s.split(",")),
            required=False,
            help="filter workloads by labels (key=value pairs separated by commas)",
        )

        list_parser.add_argument(
            "--json",
            action="store_true",
            help="output in JSON format",
        )

        list_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="continuously watch for workloads in intervals of N seconds",
        )

        list_parser.set_defaults(func=ListWorkloadsSubCommand)

    def __init__(self, args: Namespace):
        self.labels = args.labels
        self.json = args.json
        self.watch = args.watch

    def run(self):
        try:
            while True:
                sts: list[WorkloadStatus] = list_workloads(self.labels)
                print("\033[2J\033[H", end="")
                if self.json:
                    print(format_workloads_json(sts))
                else:
                    print(format_workloads_table(sts))
                if not self.watch:
                    break
                time.sleep(self.watch)
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


class LogsWorkloadSubCommand(SubCommand):
    """
    Command to get the logs of a workload deployment.
    """

    name: str
    tail: int = 100
    follow: bool = False

    @staticmethod
    def register(parser: _SubParsersAction):
        logs_parser = parser.add_parser(
            "logs",
            help="get the logs of a workload deployment",
        )

        logs_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        logs_parser.add_argument(
            "--tail",
            type=int,
            default=-1,
            help="number of lines to show from the end of the logs (default: -1)",
        )

        logs_parser.add_argument(
            "--follow",
            "-f",
            action="store_true",
            help="follow the logs in real-time",
        )

        logs_parser.set_defaults(func=LogsWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.name = args.name
        self.tail = args.tail
        self.follow = args.follow

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        try:
            print("\033[2J\033[H", end="")
            logs_stream = logs_workload(self.name, tail=self.tail, follow=self.follow)
            with contextlib.closing(logs_stream) as logs:
                for line in logs:
                    print(line.decode("utf-8").rstrip())
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


def format_workloads_json(sts: list[WorkloadStatus]) -> str:
    return json.dumps([st.to_dict() for st in sts], indent=2)


def format_workloads_table(sts: list[WorkloadStatus], width: int = 100) -> str:
    if not sts:
        return "No workloads found."

    headers = ["Name", "State", "Created At"]
    col_widths = [
        len(str(getattr(st, attr.lower().replace(" ", "_"))))
        for st in sts
        for attr in headers
    ]
    col_widths = [max(w, len(h)) for w, h in zip(col_widths, headers, strict=False)]

    total_width = sum(col_widths) + len(col_widths) * 3 + 1
    if total_width > width:
        scale = (width - len(col_widths) * 3 - 1) / sum(col_widths)
        col_widths = [int(w * scale) for w in col_widths]

    lines = []
    header_line = (
        "| "
        + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths, strict=False))
        + " |"
    )
    separator_line = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    lines.append(separator_line)
    lines.append(header_line)
    lines.append(separator_line)

    for st in sts:
        row = [
            st.name.ljust(col_widths[0]),
            st.state.ljust(col_widths[1]),
            st.created_at.ljust(col_widths[2]),
        ]
        line = "| " + " | ".join(row) + " |"
        lines.append(line)

    lines.append(separator_line)
    return "\n".join(lines)
