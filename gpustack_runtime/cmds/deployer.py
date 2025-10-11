from __future__ import annotations

import contextlib
import json
import os
import sys
import time
from argparse import REMAINDER
from typing import TYPE_CHECKING

from .. import envs
from ..deployer import (
    Container,
    ContainerCheck,
    ContainerCheckTCP,
    ContainerEnv,
    ContainerExecution,
    ContainerMount,
    ContainerPort,
    ContainerResources,
    WorkloadPlan,
    WorkloadStatus,
    WorkloadStatusStateEnum,
    create_workload,
    delete_workload,
    exec_workload,
    get_workload,
    list_workloads,
    logs_workload,
)
from ..detector import supported_backends
from .__types__ import SubCommand

if TYPE_CHECKING:
    from argparse import Namespace, _SubParsersAction

_IGNORE_ENVS = (
    "PATH",
    "HOME",
    "LANG",
    "PWD",
    "SHELL",
    "LOG",
    "XDG",
    "XPC",
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
    "PS1",
    "PY",
    "VIRTUAL_ENV",
    "CONDA",
    "PAGE",
    "ZSH",
    "COMMAND_MODE",
    "TMPDIR",
    "GPUSTACK_",
)


class CreateRunnerWorkloadSubCommand(SubCommand):
    """
    Command to create a runner workload deployment.
    """

    backend: str
    device: str
    port: int
    service: str
    version: str
    namespace: str
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
            choices=supported_backends(),
        )

        deploy_parser.add_argument(
            "--device",
            type=str,
            help="device to use, multiple devices join by comma (default: all devices)",
            default="all",
        )

        deploy_parser.add_argument(
            "--port",
            type=int,
            help="port to expose (default: 80)",
            default=80,
        )

        deploy_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the runner",
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
        self.backend = args.backend
        self.device = args.device
        self.port = args.port
        self.service = args.service
        self.version = args.version
        self.namespace = args.namespace
        self.name = f"{args.service}-{args.version}".lower().replace(".", "-")
        self.volume = args.volume
        self.extra_args = args.extra_args

        if not self.name or not self.volume:
            msg = "The name and volume arguments are required."
            raise ValueError(msg)

    def run(self):
        env = [
            ContainerEnv(
                name=name,
                value=value,
            )
            for name, value in os.environ.items()
            if not name.startswith(_IGNORE_ENVS)
        ]
        if self.backend:
            resources = ContainerResources(
                **{
                    v: self.device
                    for k, v in envs.GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY.items()
                    if k == self.backend
                },
            )
        else:
            resources = ContainerResources(
                **{
                    envs.GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY: self.device,
                },
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
        ports = [
            ContainerPort(
                internal=self.port,
            ),
        ]
        checks = [
            ContainerCheck(
                delay=30,
                interval=10,
                timeout=5,
                retries=3,
                tcp=ContainerCheckTCP(port=self.port),
                teardown=True,
            ),
        ]
        plan = WorkloadPlan(
            name=self.name,
            namespace=self.namespace,
            host_network=True,
            containers=[
                Container(
                    image=f"gpustack/runner:HostX.Y-{self.service}{self.version}",
                    name="default",
                    envs=env,
                    resources=resources,
                    mounts=mounts,
                    execution=execution,
                    ports=ports,
                    checks=checks,
                ),
            ],
        )
        create_workload(plan)
        print(f"Created workload '{self.name}'.")

        try:
            while True:
                st = get_workload(name=self.name, namespace=self.namespace)
                if st and st.state == WorkloadStatusStateEnum.RUNNING:
                    break
                time.sleep(1)

            print("\033[2J\033[H", end="")
            logs_stream = logs_workload(
                name=self.name,
                namespace=self.namespace,
                tail=-1,
                follow=True,
            )
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
    device: str
    port: int
    namespace: str
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
            choices=supported_backends(),
        )

        deploy_parser.add_argument(
            "--device",
            type=str,
            help="device to use, multiple devices join by comma (default: all devices)",
            default="all",
        )

        deploy_parser.add_argument(
            "--port",
            type=int,
            help="port to expose (default: 80)",
            default=80,
        )

        deploy_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the workload",
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
        self.backend = args.backend
        self.device = args.device
        self.port = args.port
        self.namespace = args.namespace
        self.name = args.name
        self.image = args.image
        self.volume = args.volume
        self.extra_args = args.extra_args

        if not self.name or not self.image or not self.volume:
            msg = "The name, image, and volume arguments are required."
            raise ValueError(msg)

    def run(self):
        env = [
            ContainerEnv(
                name=name,
                value=value,
            )
            for name, value in os.environ.items()
            if not name.startswith(_IGNORE_ENVS)
        ]
        if self.backend:
            resources = ContainerResources(
                **{
                    v: self.device
                    for k, v in envs.GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY.items()
                    if k == self.backend
                },
            )
        else:
            resources = ContainerResources(
                **{
                    envs.GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY: self.device,
                },
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
        ports = [
            ContainerPort(
                internal=self.port,
            ),
        ]
        checks = [
            ContainerCheck(
                delay=30,
                interval=10,
                timeout=5,
                retries=3,
                tcp=ContainerCheckTCP(port=self.port),
                teardown=True,
            ),
        ]
        plan = WorkloadPlan(
            name=self.name,
            namespace=self.namespace,
            host_network=True,
            containers=[
                Container(
                    image=self.image,
                    name="default",
                    envs=env,
                    resources=resources,
                    mounts=mounts,
                    execution=execution,
                    ports=ports,
                    checks=checks,
                ),
            ],
        )
        create_workload(plan)
        print(f"Created workload '{self.name}'.")

        try:
            while True:
                st = get_workload(name=self.name, namespace=self.namespace)
                if st and st.state == WorkloadStatusStateEnum.RUNNING:
                    break
                time.sleep(1)

            print("\033[2J\033[H", end="")
            logs_stream = logs_workload(
                name=self.name,
                namespace=self.namespace,
                tail=-1,
                follow=True,
            )
            with contextlib.closing(logs_stream) as logs:
                for line in logs:
                    print(line.decode("utf-8").rstrip())
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


class DeleteWorkloadSubCommand(SubCommand):
    """
    Command to delete a workload deployment.
    """

    namespace: str
    name: str

    @staticmethod
    def register(parser: _SubParsersAction):
        delete_parser = parser.add_parser(
            "delete",
            help="delete a workload deployment",
        )

        delete_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the workload",
        )

        delete_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        delete_parser.set_defaults(func=DeleteWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.namespace = args.namespace
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

    format: str
    watch: int
    namespace: str
    name: str

    @staticmethod
    def register(parser: _SubParsersAction):
        get_parser = parser.add_parser(
            "get",
            help="get the status of a workload deployment",
        )

        get_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="output format",
        )

        get_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="continuously watch for the workload in intervals of N seconds",
            default=0,
        )

        get_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the workload",
        )

        get_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        get_parser.set_defaults(func=GetWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.format = args.format
        self.watch = args.watch
        self.namespace = args.namespace
        self.name = args.name

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        try:
            while True:
                workload = get_workload(self.name)
                if not workload:
                    print(f"Workload '{self.name}' not found.")
                    return

                sts: list[WorkloadStatus] = [workload]
                print("\033[2J\033[H", end="")
                match self.format.lower():
                    case "json":
                        print(format_workloads_json(sts))
                    case _:
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

    namespace: str
    labels: dict[str, str]
    format: str
    watch: int = 0

    @staticmethod
    def register(parser: _SubParsersAction):
        list_parser = parser.add_parser(
            "list",
            help="list all workload deployments",
        )

        list_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the workloads",
        )

        list_parser.add_argument(
            "--labels",
            type=lambda s: dict(item.split("=") for item in s.split(",")),
            required=False,
            help="filter workloads by labels (key=value pairs separated by commas)",
        )

        list_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="output format",
        )

        list_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="continuously watch for workloads in intervals of N seconds",
        )

        list_parser.set_defaults(func=ListWorkloadsSubCommand)

    def __init__(self, args: Namespace):
        self.namespace = args.namespace
        self.labels = args.labels
        self.format = args.format
        self.watch = args.watch

    def run(self):
        try:
            while True:
                sts: list[WorkloadStatus] = list_workloads(
                    namespace=self.namespace,
                    labels=self.labels,
                )
                print("\033[2J\033[H", end="")
                match self.format.lower():
                    case "json":
                        print(format_workloads_json(sts))
                    case _:
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

    tail: int
    follow: bool
    namespace: str
    name: str

    @staticmethod
    def register(parser: _SubParsersAction):
        logs_parser = parser.add_parser(
            "logs",
            help="get the logs of a workload deployment",
        )

        logs_parser.add_argument(
            "--tail",
            type=int,
            help="number of lines to show from the end of the logs (default: -1)",
            default=-1,
        )

        logs_parser.add_argument(
            "--follow",
            "-f",
            action="store_true",
            help="follow the logs in real-time",
        )

        logs_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the workload",
        )

        logs_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        logs_parser.set_defaults(func=LogsWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.tail = args.tail
        self.follow = args.follow
        self.namespace = args.namespace
        self.name = args.name

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        try:
            print("\033[2J\033[H", end="")
            logs_result = logs_workload(
                name=self.name,
                namespace=self.namespace,
                tail=self.tail,
                follow=self.follow,
            )
            if self.follow:
                with contextlib.closing(logs_result) as logs:
                    for line in logs:
                        if isinstance(line, str):
                            print(line.rstrip())
                        else:
                            print(line.decode("utf-8").rstrip())
            elif isinstance(logs_result, str):
                print(logs_result.rstrip())
            else:
                print(logs_result.decode("utf-8").rstrip())
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


class ExecWorkloadSubCommand(SubCommand):
    """
    Command to execute a command in a workload deployment.
    """

    interactive: bool
    namespace: str
    name: str
    command: list[str]

    @staticmethod
    def register(parser: _SubParsersAction):
        exec_parser = parser.add_parser(
            "exec",
            help="execute a command in a workload deployment",
        )

        exec_parser.add_argument(
            "--interactive",
            "-i",
            action="store_true",
            help="interactive mode",
        )

        exec_parser.add_argument(
            "--namespace",
            type=str,
            help="namespace of the workload",
        )

        exec_parser.add_argument(
            "name",
            type=str,
            help="name of the workload",
        )

        exec_parser.add_argument(
            "command",
            nargs=REMAINDER,
            help="command to execute in the workload",
        )

        exec_parser.set_defaults(func=ExecWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.interactive = args.interactive
        self.namespace = args.namespace
        self.name = args.name
        self.command = args.command

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        try:
            if self.interactive:
                from dockerpty import io, pty  # noqa: PLC0415
        except ImportError:
            print(
                "dockerpty is required for interactive mode. "
                "Please install it via 'pip install dockerpty'.",
            )
            sys.exit(1)

        try:
            print("\033[2J\033[H", end="")
            exec_result = exec_workload(
                name=self.name,
                namespace=self.namespace,
                detach=not self.interactive,
                command=self.command,
            )

            # Non-interactive mode: print output and exit with the command's exit code

            if not self.interactive:
                if isinstance(exec_result, bytes):
                    print(exec_result.decode("utf-8").rstrip())
                else:
                    print(exec_result)
                return

            # Interactive mode: use dockerpty to attach to the exec session

            class ExecOperation(pty.Operation):
                def __init__(self, sock):
                    self.stdin = sys.stdin
                    self.stdout = sys.stdout
                    self.sock = io.Stream(sock)

                def israw(self, **_):
                    return self.stdout.isatty()

                def start(self, **_):
                    sock = self.sockets()
                    return [
                        io.Pump(io.Stream(self.stdin), sock, wait_for_output=False),
                        io.Pump(sock, io.Stream(self.stdout), propagate_close=False),
                    ]

                def resize(self, height, width, **_):
                    pass

                def sockets(self):
                    return self.sock

            exec_op = ExecOperation(exec_result)
            pty.PseudoTerminal(None, exec_op).start()
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


def format_workloads_json(sts: list[WorkloadStatus]) -> str:
    return json.dumps([st.to_dict() for st in sts], indent=2)


def format_workloads_table(sts: list[WorkloadStatus]) -> str:
    if not sts:
        return "No workloads found."

    width = 100

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
