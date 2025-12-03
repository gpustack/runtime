from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from argparse import REMAINDER
from pathlib import Path
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
    ContainerRestartPolicyEnum,
    WorkloadPlan,
    WorkloadStatus,
    WorkloadStatusStateEnum,
    async_logs_workload,
    create_workload,
    delete_workload,
    exec_workload,
    get_workload,
    list_workloads,
)
from ..deployer.__utils__ import safe_json, safe_yaml
from ..detector import supported_backends
from .__types__ import SubCommand

if TYPE_CHECKING:
    from argparse import Namespace, _SubParsersAction

_IGNORE_ENVS_PREFIX = (
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

_IGNORE_ENVS_SUFFIX = (
    "_HOME",
    "_PATH",
    "_VISIBLE_DEVICES",
    "_DISABLE_REQUIRE",
    "_DRIVER_CAPABILITIES",
)

_IGNORE_SENSITIVE_ENVS_SUFFIX = (
    "_KEY",
    "_key",
    "_TOKEN",
    "_token",
    "_SECRET",
    "_secret",
    "_PASSWORD",
    "_password",
    "_PASS",
    "_pass",
)


class CreateRunnerWorkloadSubCommand(SubCommand):
    """
    Command to create a runner workload deployment.
    """

    backend: str
    device: str
    command_script: str | None
    port: int
    host_network: bool
    check: bool
    namespace: str
    service: str
    version: str
    name: str
    volume: str
    extra_args: list[str]

    @staticmethod
    def register(parser: _SubParsersAction):
        deploy_parser = parser.add_parser(
            "create-runner",
            help="Create a runner workload deployment",
        )

        deploy_parser.add_argument(
            "--backend",
            type=str,
            help="Backend to use (default: detect from current environment)",
            choices=supported_backends(),
        )

        deploy_parser.add_argument(
            "--device",
            type=str,
            help="Device to use, multiple devices join by comma (default: all devices)",
            default="all",
        )

        deploy_parser.add_argument(
            "--command-script-file",
            type=str,
            help="Path of command script for the workload",
        )

        deploy_parser.add_argument(
            "--port",
            type=int,
            help="Port to expose",
        )

        deploy_parser.add_argument(
            "--host-network",
            action="store_true",
            help="Use host network (default: False)",
            default=False,
        )

        deploy_parser.add_argument(
            "--check",
            action="store_true",
            help="Enable health check, needs --port (default: False)",
            default=False,
        )

        deploy_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the runner",
        )

        deploy_parser.add_argument(
            "service",
            type=str,
            help="Service of the runner",
        )

        deploy_parser.add_argument(
            "version",
            type=str,
            help="Version of the runner",
        )

        deploy_parser.add_argument(
            "volume",
            type=str,
            help="Volume to mount",
        )

        deploy_parser.add_argument(
            "extra_args",
            nargs=REMAINDER,
            help="Extra arguments for the runner",
        )

        deploy_parser.set_defaults(func=CreateRunnerWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.backend = args.backend
        self.device = args.device
        self.command_script = None
        self.port = args.port
        self.host_network = args.host_network
        self.check = args.check
        self.namespace = args.namespace
        self.service = args.service
        self.version = args.version
        self.name = f"{args.service}-{args.version}".lower().replace(".", "-")
        self.volume = args.volume
        self.extra_args = args.extra_args

        if not self.name or not self.volume:
            msg = "The name and volume arguments are required."
            raise ValueError(msg)

        if args.command_script_file:
            command_script_file = Path(args.command_script_file)
            if not command_script_file.is_file():
                msg = f"The command script file '{command_script_file}' does not exist."
                raise ValueError(msg)
            self.command_script = command_script_file.read_text(
                encoding="utf-8",
            ).strip()

    def run(self):
        env = [
            ContainerEnv(
                name=name,
                value=value,
            )
            for name, value in os.environ.items()
            if not name.startswith(_IGNORE_ENVS_PREFIX)
            and not name.endswith(_IGNORE_ENVS_SUFFIX)
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
        execution = ContainerExecution(
            command_script=self.command_script,
            args=self.extra_args,
        )
        ports = (
            [
                ContainerPort(
                    internal=self.port,
                ),
            ]
            if self.port
            else None
        )
        checks = (
            [
                ContainerCheck(
                    delay=60,
                    interval=10,
                    timeout=5,
                    retries=6,
                    tcp=ContainerCheckTCP(port=self.port),
                    teardown=True,
                ),
            ]
            if self.check and self.port
            else None
        )
        plan = WorkloadPlan(
            name=self.name,
            namespace=self.namespace,
            host_network=self.host_network,
            containers=[
                Container(
                    restart_policy=(
                        ContainerRestartPolicyEnum.NEVER
                        if not self.check
                        else ContainerRestartPolicyEnum.ALWAYS
                    ),
                    image=f"gpustack/runner:{self.backend if self.backend else 'Host'}X.Y-{self.service}{self.version}",
                    name=self.name,
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

        while True:
            st = get_workload(
                name=self.name,
                namespace=self.namespace,
            )
            if st and st.state not in (
                WorkloadStatusStateEnum.PENDING,
                WorkloadStatusStateEnum.INITIALIZING,
            ):
                break
            time.sleep(1)

        print("\033[2J\033[H", end="")

        async def stream_logs():
            logs_result = await async_logs_workload(
                name=self.name,
                namespace=self.namespace,
                tail=-1,
                follow=True,
            )
            async for line in logs_result:
                print(line.decode("utf-8").rstrip())

        asyncio.run(stream_logs())


class CreateWorkloadSubCommand(SubCommand):
    """
    Command to create a workload deployment.
    """

    backend: str
    device: str
    command_script: str | None
    port: int
    host_network: bool
    check: bool
    namespace: str
    name: str
    image: str
    volume: str
    extra_args: list[str]

    @staticmethod
    def register(parser: _SubParsersAction):
        deploy_parser = parser.add_parser(
            "create",
            help="Create a workload deployment",
        )

        deploy_parser.add_argument(
            "--backend",
            type=str,
            help="Backend to use (default: detect from current environment)",
            choices=supported_backends(),
        )

        deploy_parser.add_argument(
            "--device",
            type=str,
            help="Device to use, multiple devices join by comma (default: all devices)",
            default="all",
        )

        deploy_parser.add_argument(
            "--command-script-file",
            type=str,
            help="Path of command script for the workload",
        )

        deploy_parser.add_argument(
            "--port",
            type=int,
            help="Port to expose",
        )

        deploy_parser.add_argument(
            "--host-network",
            action="store_true",
            help="Use host network (default: False)",
            default=False,
        )

        deploy_parser.add_argument(
            "--check",
            action="store_true",
            help="Enable health check, needs --port (default: False)",
            default=False,
        )

        deploy_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workload",
        )

        deploy_parser.add_argument(
            "name",
            type=str,
            help="Name of the workload",
        )

        deploy_parser.add_argument(
            "image",
            type=str,
            help="Image to deploy (should be a valid Docker image)",
        )

        deploy_parser.add_argument(
            "volume",
            type=str,
            help="Volume to mount",
        )

        deploy_parser.add_argument(
            "extra_args",
            nargs=REMAINDER,
            help="Extra arguments for the workload",
        )

        deploy_parser.set_defaults(func=CreateWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.backend = args.backend
        self.device = args.device
        self.command_script = None
        self.port = args.port
        self.host_network = args.host_network
        self.check = args.check
        self.namespace = args.namespace
        self.name = args.name
        self.image = args.image
        self.volume = args.volume
        self.extra_args = args.extra_args

        if not self.name or not self.image or not self.volume:
            msg = "The name, image, and volume arguments are required."
            raise ValueError(msg)

        if args.command_script_file:
            command_script_file = Path(args.command_script_file)
            if not command_script_file.is_file():
                msg = f"The command script file '{command_script_file}' does not exist."
                raise ValueError(msg)
            self.command_script = command_script_file.read_text(
                encoding="utf-8",
            ).strip()

    def run(self):
        env = [
            ContainerEnv(
                name=name,
                value=value,
            )
            for name, value in os.environ.items()
            if not name.startswith(_IGNORE_ENVS_PREFIX)
            and not name.endswith(_IGNORE_ENVS_SUFFIX)
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
        execution = ContainerExecution(
            command_script=self.command_script,
            args=self.extra_args,
        )
        ports = (
            [
                ContainerPort(
                    internal=self.port,
                ),
            ]
            if self.port
            else None
        )
        checks = (
            [
                ContainerCheck(
                    delay=60,
                    interval=10,
                    timeout=5,
                    retries=6,
                    tcp=ContainerCheckTCP(port=self.port),
                    teardown=True,
                ),
            ]
            if self.check and self.port
            else None
        )
        plan = WorkloadPlan(
            name=self.name,
            namespace=self.namespace,
            host_network=self.host_network,
            containers=[
                Container(
                    restart_policy=(
                        ContainerRestartPolicyEnum.NEVER
                        if not self.check
                        else ContainerRestartPolicyEnum.ALWAYS
                    ),
                    image=self.image,
                    name=self.name,
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

        while True:
            st = get_workload(
                name=self.name,
                namespace=self.namespace,
            )
            if st and st.state not in (
                WorkloadStatusStateEnum.PENDING,
                WorkloadStatusStateEnum.INITIALIZING,
            ):
                break
            time.sleep(1)

        print("\033[2J\033[H", end="")

        async def stream_logs():
            logs_result = await async_logs_workload(
                name=self.name,
                namespace=self.namespace,
                tail=-1,
                follow=True,
            )
            async for line in logs_result:
                print(line.decode("utf-8").rstrip())

        asyncio.run(stream_logs())


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
            help="Delete a workload deployment",
        )

        delete_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workload",
        )

        delete_parser.add_argument(
            "name",
            type=str,
            help="Name of the workload",
        )

        delete_parser.set_defaults(func=DeleteWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.namespace = args.namespace
        self.name = args.name

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        st = delete_workload(
            name=self.name,
            namespace=self.namespace,
        )
        if st:
            print(f"Deleted workload '{self.name}'.")
        else:
            print(f"Workload '{self.name}' not found.")


class DeleteWorkloadsSubCommand(SubCommand):
    """
    Command to delete all workload deployments.
    """

    @staticmethod
    def register(parser: _SubParsersAction):
        delete_parser = parser.add_parser(
            "delete-all",
            help="Delete all workload deployments",
        )

        delete_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workload",
        )

        delete_parser.add_argument(
            "--labels",
            type=lambda s: dict(item.split("=") for item in s.split(",")),
            required=False,
            help="Filter workloads by labels (key=value pairs separated by commas)",
        )

        delete_parser.set_defaults(func=DeleteWorkloadsSubCommand)

    def __init__(self, args: Namespace):
        self.namespace = args.namespace
        self.labels = args.labels

    def run(self):
        sts: list[WorkloadStatus] = list_workloads(
            namespace=self.namespace,
            labels=self.labels,
        )
        for st in sts:
            delete_workload(
                name=st.name,
                namespace=st.namespace,
            )
            print(f"Deleted workload '{st.name}'.")
        if not sts:
            print("No workloads found.")


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
            help="Get the status of a workload deployment",
        )

        get_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="Putput format",
        )

        get_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="Continuously watch for the workload in intervals of N seconds",
            default=0,
        )

        get_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workload",
        )

        get_parser.add_argument(
            "name",
            type=str,
            help="Name of the workload",
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
        while True:
            workload = get_workload(self.name, self.namespace)
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
            help="List all workload deployments",
        )

        list_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workloads",
        )

        list_parser.add_argument(
            "--labels",
            type=lambda s: dict(item.split("=") for item in s.split(",")),
            required=False,
            help="Filter workloads by labels (key=value pairs separated by commas)",
        )

        list_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="Output format",
        )

        list_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="Continuously watch for workloads in intervals of N seconds",
        )

        list_parser.set_defaults(func=ListWorkloadsSubCommand)

    def __init__(self, args: Namespace):
        self.namespace = args.namespace
        self.labels = args.labels
        self.format = args.format
        self.watch = args.watch

    def run(self):
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
            help="Get the logs of a workload deployment",
        )

        logs_parser.add_argument(
            "--tail",
            type=int,
            help="Number of lines to show from the end of the logs (default: -1)",
            default=-1,
        )

        logs_parser.add_argument(
            "--follow",
            "-f",
            action="store_true",
            help="Follow the logs in real-time",
        )

        logs_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workload",
        )

        logs_parser.add_argument(
            "name",
            type=str,
            help="Name of the workload",
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
        print("\033[2J\033[H", end="")

        async def stream_logs():
            logs_result = await async_logs_workload(
                name=self.name,
                namespace=self.namespace,
                tail=self.tail,
                follow=self.follow,
            )
            if self.follow:
                async for line in logs_result:
                    print(line.decode("utf-8").rstrip())
            elif isinstance(logs_result, str):
                print(logs_result.rstrip())
            else:
                print(logs_result.decode("utf-8").rstrip())

        asyncio.run(stream_logs())


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
            help="Execute a command in a workload deployment",
        )

        exec_parser.add_argument(
            "--interactive",
            "-i",
            action="store_true",
            help="Interactive mode",
        )

        exec_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workload",
        )

        exec_parser.add_argument(
            "name",
            type=str,
            help="Name of the workload",
        )

        exec_parser.add_argument(
            "command",
            nargs=REMAINDER,
            help="Command to execute in the workload",
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


class InspectWorkloadSubCommand(SubCommand):
    """
    Command to diagnose a workload deployment.
    """

    @staticmethod
    def register(parser: _SubParsersAction):
        inspect_parser = parser.add_parser(
            "inspect",
            help="Inspect a workload deployment",
        )

        inspect_parser.add_argument(
            "--namespace",
            type=str,
            help="Namespace of the workloads",
        )

        inspect_parser.add_argument(
            "name",
            type=str,
            help="Name of the workload",
        )

        inspect_parser.set_defaults(func=InspectWorkloadSubCommand)

    def __init__(self, args: Namespace):
        self.namespace = args.namespace
        self.name = args.name

        if not self.name:
            msg = "The name argument is required."
            raise ValueError(msg)

    def run(self):
        workload = get_workload(self.name, self.namespace)
        if not workload:
            print(f"Workload '{self.name}' not found.")
            return

        if hasattr(workload, "_d_containers"):
            result = []
            for c in workload._d_containers:  # noqa: SLF001
                c_attrs = c.attrs
                # Mask sensitive environment variables
                if "Env" in c_attrs["Config"]:
                    for i, env in enumerate(c_attrs["Config"]["Env"] or []):
                        env_name, _ = env.split("=", maxsplit=1)
                        if env_name.endswith(_IGNORE_SENSITIVE_ENVS_SUFFIX):
                            c_attrs["Config"]["Env"][i] = f"{env_name}=******"
                result.append(c_attrs)
            print(safe_json(result, indent=2))
        elif hasattr(workload, "_k_pod"):
            k_pod = workload._k_pod  # noqa: SLF001
            # Remove managed fields to reduce output size
            k_pod.metadata.managed_fields = None
            # Mask sensitive environment variables
            for c in k_pod.spec.containers:
                for env in c.env or []:
                    if env.name.endswith(_IGNORE_SENSITIVE_ENVS_SUFFIX):
                        env.value = "******"
            print(safe_yaml(k_pod, indent=2, sort_keys=False))
        else:
            print("No detailed inspection information available for this workload.")


def format_workloads_json(sts: list[WorkloadStatus]) -> str:
    return json.dumps([st.to_dict() for st in sts], indent=2)


def format_workloads_table(sts: list[WorkloadStatus]) -> str:
    if not sts:
        return "No workloads found."

    headers = ["Name", "State", "Created At"]
    # Calculate max width for each column based on header and content
    col_widths = []
    for attr in headers:
        attr_key = attr.lower().replace(" ", "_")
        max_content_width = max(
            [len(str(getattr(st, attr_key))) for st in sts] + [len(attr)],
        )
        col_widths.append(max_content_width)

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
            str(st.name).ljust(col_widths[0]),
            str(st.state).ljust(col_widths[1]),
            str(st.created_at).ljust(col_widths[2]),
        ]
        line = "| " + " | ".join(row) + " |"
        lines.append(line)

    lines.append(separator_line)
    return "\n".join(lines)
