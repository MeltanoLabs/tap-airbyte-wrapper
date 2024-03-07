# Copyright (c) 2022 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
"""Airbyte tap class"""

from __future__ import annotations

import errno
import os
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from pathlib import Path, PurePath
from queue import Empty, Queue
from tempfile import TemporaryDirectory
from threading import Lock, Thread
from typing import Any, Callable, Dict, Iterable, List, Optional, cast
from uuid import UUID

import click
import orjson
import singer_sdk._singerlib as singer
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.cli import common_options
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.tap_base import CliTestOptionValue

# Sentinel value for broken pipe
PIPE_CLOSED = object()


def default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    elif isinstance(obj, Enum):
        return obj.value
    return str(obj)


def write_message(message) -> None:
    try:
        sys.stdout.buffer.write(
            orjson.dumps(message.to_dict(), option=TapAirbyte.ORJSON_OPTS, default=default)
        )
        sys.stdout.buffer.flush()
    except IOError as e:
        # Broken pipe
        if e.errno == errno.EPIPE and TapAirbyte.pipe_status is not PIPE_CLOSED:
            TapAirbyte.logger.info("Received SIGPIPE, stopping sync of stream.")
            TapAirbyte.pipe_status = PIPE_CLOSED
            # Prevent BrokenPipe writes to closed stdout
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        else:
            raise


STDOUT_LOCK = Lock()
singer.write_message = write_message


class AirbyteException(Exception):
    pass


class AirbyteMessage(str, Enum):
    RECORD = "RECORD"
    STATE = "STATE"
    LOG = "LOG"
    TRACE = "TRACE"
    CATALOG = "CATALOG"
    SPEC = "SPEC"
    CONNECTION_STATUS = "CONNECTION_STATUS"


# These translate between Singer's replication method and Airbyte's sync mode
REPLICATION_METHOD_MAP = {
    "FULL_TABLE": "FULL_REFRESH",
    "INCREMENTAL": "INCREMENTAL",
    "LOG_BASED": "INCREMENTAL",
}
# We are piping to Singer targets, so this field is irrelevant
NOOP_AIRBYTE_SYNC_MODE = "append"


class TapAirbyte(Tap):
    name = "tap-airbyte"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "airbyte_spec",
            th.ObjectType(
                th.Property(
                    "image",
                    th.StringType,
                    required=True,
                    description="Airbyte image to run",
                ),
                th.Property("tag", th.StringType, required=False, default="latest"),
            ),
            required=True,
            description=(
                "Specification for the Airbyte source connector. This is a JSON object minimally"
                " containing the `image` key. The `tag` key is optional and defaults to `latest`."
            ),
        ),
        th.Property(
            "airbyte_config",
            th.ObjectType(),
            required=False,
            description=(
                "Configuration to pass through to the Airbyte source connector, this can be gleaned"
                " by running the the tap with the `--about` flag and the `--config` flag pointing"
                " to a file containing the `airbyte_spec` configuration. This is a JSON object."
            ),
        ),
        th.Property(
            "docker_mounts",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "source",
                        th.StringType,
                        required=True,
                        description="Source path to mount",
                    ),
                    th.Property(
                        "target",
                        th.StringType,
                        required=True,
                        description="Target path to mount",
                    ),
                    th.Property(
                        "type",
                        th.StringType,
                        default="bind",
                        description="Type of mount",
                    ),
                )
            ),
            required=False,
            description=(
                "Docker mounts to make available to the Airbyte container. Expects a list of maps"
                " containing source, target, and type as is documented in the docker --mount"
                " documentation"
            ),
        ),
    ).to_dict()
    conf_dir: str = "/tmp"
    pipe_status = None

    # Airbyte image to run
    _image: Optional[str] = None
    _tag: Optional[str] = None
    _docker_mounts: Optional[List[Dict[str, str]]] = None
    container_runtime = os.getenv("OCI_RUNTIME", "docker")

    # Airbyte -> Demultiplexer -< Singer Streams
    singer_consumers: List[Thread] = []
    buffers: Dict[str, Queue] = {}

    # State container
    airbyte_state: Dict[str, Any] = {}

    ORJSON_OPTS = orjson.OPT_APPEND_NEWLINE

    @classproperty
    def cli(cls) -> Callable:
        @common_options.PLUGIN_VERSION
        @common_options.PLUGIN_ABOUT
        @common_options.PLUGIN_ABOUT_FORMAT
        @common_options.PLUGIN_CONFIG
        @click.option(
            "--discover",
            is_flag=True,
            help="Run the tap in discovery mode.",
        )
        @click.option(
            "--test",
            is_flag=True,
            help="Use --test to run the Airbyte connection test.",
        )
        @click.option(
            "--catalog",
            help="Use a Singer catalog file with the tap.",
            type=click.Path(),
        )
        @click.option(
            "--state",
            help="Use a bookmarks file for incremental replication.",
            type=click.Path(),
        )
        @click.command(
            help="Execute the Singer tap.",
            context_settings={"help_option_names": ["--help"]},
        )
        def cli(
            version: bool = False,
            about: bool = False,
            discover: bool = False,
            test: bool = False,
            config: tuple[str, ...] = (),
            state: Optional[str] = None,
            catalog: Optional[str] = None,
            format: Optional[str] = None,
        ) -> None:
            if version:
                cls.print_version()
                return
            if not about:
                cls.print_version(print_fn=cls.logger.info)
            validate_config: bool = True
            if discover or about:
                validate_config = False
            parse_env_config = False
            config_files: list[PurePath] = []
            for config_path in config:
                if config_path == "ENV":
                    parse_env_config = True
                    continue
                if not Path(config_path).is_file():
                    raise FileNotFoundError(
                        f"Could not locate config file at '{config_path}'."
                        "Please check that the file exists."
                    )
                config_files.append(Path(config_path))
            # Enrich about info with spec if possible
            if about:
                cls.discover_streams = lambda _: []
                try:
                    tap: TapAirbyte = cls(  # type: ignore
                        config=config_files or None,
                        state=state,
                        catalog=catalog,
                        parse_env_config=parse_env_config,
                        validate_config=validate_config,
                    )
                    spec = tap.run_spec()["connectionSpecification"]
                except Exception:
                    cls.logger.info("Tap-Airbyte instantiation failed. Printing basic about info.")
                    cls.print_about(format=format)
                else:
                    cls.logger.info(
                        "Tap-Airbyte instantiation succeeded. Printing spec-enriched about info."
                    )
                    cls.config_jsonschema["properties"]["airbyte_config"] = spec
                    cls.print_about(format=format)
                    cls.print_spec_as_config(spec)
                return
            # End modification
            tap: TapAirbyte = cls(  # type: ignore
                config=config_files or None,
                state=state,
                catalog=catalog,
                parse_env_config=parse_env_config,
                validate_config=validate_config,
            )
            if discover:
                tap.run_discovery()
                if test:
                    tap.run_connection_test()
            elif test:
                tap.run_connection_test()
            else:
                tap.sync_all()

        return cli

    def __init__(self, *args, **kwargs) -> None:
        # OCI check
        self.logger.info("Checking for %s on PATH.", self.container_runtime)
        if not shutil.which(self.container_runtime):
            self.logger.error(
                "Could not find %s on PATH. Please verify that %s is installed and on PATH.",
                self.container_runtime,
                self.container_runtime,
            )
            sys.exit(1)
        self.logger.info("Found %s on PATH.", self.container_runtime)
        self.logger.info("Checking %s version.", self.container_runtime)
        try:
            subprocess.check_call([self.container_runtime, "version"], stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            self.logger.error(
                (
                    "Failed to execute %s version with exit code %d. Please verify that %s is"
                    " configured correctly."
                ),
                self.container_runtime,
                e.returncode,
                self.container_runtime,
            )
            sys.exit(1)
        self.logger.info("Successfully executed %s version.", self.container_runtime)
        # End OCI check
        super().__init__(*args, **kwargs)

    def run_help(self):
        subprocess.run(
            ["docker", "run", f"{self.image}:{self.tag}", "--help"],
            check=True,
        )

    def run_spec(self):
        proc = subprocess.run(
            ["docker", "run", f"{self.image}:{self.tag}", "spec"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for line in proc.stdout.decode("utf-8").splitlines():
            try:
                message = orjson.loads(line)
            except orjson.JSONDecodeError:
                if line:
                    self.logger.warning("Could not parse message: %s", line)
                continue
            if message["type"] in (AirbyteMessage.LOG, AirbyteMessage.TRACE):
                self._process_log_message(message)
            elif message["type"] == AirbyteMessage.SPEC:
                return message["spec"]
            else:
                self.logger.warning("Unhandled message: %s", message)
        if proc.returncode != 0:
            raise AirbyteException(f"Could not run spec for {self.image}:{self.tag}: {proc.stderr}")
        raise AirbyteException(
            "Could not output spec, no spec message received.\n"
            f"Stdout: {proc.stdout.decode('utf-8')}\n"
            f"Stderr: {proc.stderr.decode('utf-8')}"
        )

    @staticmethod
    def print_spec_as_config(spec: Dict[str, Any]) -> None:
        print("\nSetup Instructions:\n")
        print("airbyte_config:")
        for prop, schema in spec["properties"].items():
            if "description" in schema:
                print(f"  # {schema['description']}")
            print(f"  {prop}: {'fixme' if schema['type'] != 'object' else ''}")
            if schema["type"] == "object":
                if "oneOf" in schema:
                    for i, one_of in enumerate(schema["oneOf"]):
                        print(f"    # Option {i + 1}")
                        for inner_prop, inner_schema in one_of["properties"].items():
                            if inner_prop == "option_title":
                                continue
                            if "description" in inner_schema:
                                print(f"    # {inner_schema['description']}")
                            print(f"    {inner_prop}: fixme")
                else:
                    for inner_prop, inner_schema in schema["properties"].items():
                        if "description" in inner_schema:
                            print(f"    # {inner_schema['description']}")
                        print(f"    {inner_prop}: fixme")

    def run_check(self) -> bool:
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "wb") as f:
                f.write(orjson.dumps(self.config.get("airbyte_config", {})))
            proc = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{tmpdir}:{self.conf_dir}",
                ]
                + self.docker_mounts
                + [
                    f"{self.image}:{self.tag}",
                    "check",
                    "--config",
                    f"{self.conf_dir}/config.json",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        for line in proc.stdout.decode("utf-8").splitlines():
            try:
                message = orjson.loads(line)
            except orjson.JSONDecodeError:
                if line:
                    self.logger.warning("Could not parse message: %s", line)
                continue
            if message["type"] in (AirbyteMessage.LOG, AirbyteMessage.TRACE):
                self._process_log_message(message)
            elif message["type"] == AirbyteMessage.CONNECTION_STATUS:
                if message["connectionStatus"]["status"] == "SUCCEEDED":
                    self.logger.info(
                        "Configuration has been verified via the Airbyte check command."
                    )
                    return True
                else:
                    self.logger.error(
                        "Connection check failed: %s",
                        message["connectionStatus"]["message"],
                    )
                    return False
            else:
                self.logger.warning("Unhandled message: %s", message)
        if proc.returncode != 0:
            raise AirbyteException(
                f"Connection check failed with return code {proc.returncode}:"
                f" {proc.stderr.decode()}"
            )
        raise AirbyteException(
            "Could not verify connection, no connection status message received.\n"
            f"Stdout: {proc.stdout.decode('utf-8')}\n"
            f"Stderr: {proc.stderr.decode('utf-8')}"
        )

    def run_connection_test(self) -> bool:  # type: ignore
        return self.run_check()

    @contextmanager
    def run_read(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "wb") as config, open(
                f"{tmpdir}/catalog.json", "wb"
            ) as catalog:
                config.write(orjson.dumps(self.config.get("airbyte_config", {})))
                catalog.write(orjson.dumps(self.configured_airbyte_catalog))
            if self.airbyte_state:
                with open(f"{tmpdir}/state.json", "wb") as state:
                    self.logger.debug("Using state: %s", self.airbyte_state)
                    state.write(orjson.dumps(self.airbyte_state))
            proc = subprocess.Popen(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{tmpdir}:{self.conf_dir}",
                ]
                + self.docker_mounts
                + [
                    f"{self.image}:{self.tag}",
                    "read",
                    "--config",
                    f"{self.conf_dir}/config.json",
                    "--catalog",
                    f"{self.conf_dir}/catalog.json",
                ]
                + (["--state", f"{self.conf_dir}/state.json"] if self.airbyte_state else []),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                # Context is held until EOF or exception
                yield proc
            finally:
                if not self.eof_received:
                    proc.kill()
                    self.logger.warning("Airbyte process terminated before EOF message received.")
                self.logger.debug("Waiting for Airbyte process to terminate.")
                returncode = proc.wait()
                if not self.eof_received and TapAirbyte.pipe_status is not PIPE_CLOSED:
                    # If EOF was not received, the process was killed and we should raise an exception
                    type, value, _ = sys.exc_info()
                    raise AirbyteException(
                        f"Airbyte process terminated early:\n{type.__name__}: {value}"
                    )
                if returncode != 0 and TapAirbyte.pipe_status is not PIPE_CLOSED:
                    # If EOF was received, the process should have exited with return code 0
                    raise AirbyteException(
                        f"Airbyte process failed with return code {returncode}:"
                        f" {proc.stderr.read()}"
                    )

    def _process_log_message(self, airbyte_message: Dict[str, Any]) -> None:
        if airbyte_message["type"] == AirbyteMessage.LOG:
            self.logger.info(airbyte_message["log"])
        elif airbyte_message["type"] == AirbyteMessage.TRACE:
            if airbyte_message["trace"].get("type") == "ERROR":
                exc = AirbyteException(
                    airbyte_message["trace"]["error"].get("stack_trace", "Airbyte process failed.")
                )
                self.logger.critical(
                    airbyte_message["trace"]["error"]["message"],
                    exc_info=exc,
                )
                raise exc
            self.logger.debug(airbyte_message["trace"])

    @property
    def image(self) -> str:
        if not self._image:
            try:
                self._image: str = self.config["airbyte_spec"]["image"]
            except KeyError:
                raise AirbyteException(
                    "Airbyte spec is missing required fields. Please ensure you are passing"
                    " --config and that the passed config contains airbyte_spec."
                ) from KeyError
        return self._image

    @property
    def tag(self) -> str:
        if not self._tag:
            try:
                self._tag: str = cast(dict, self.config["airbyte_spec"]).get("tag", "latest")
            except KeyError:
                raise AirbyteException(
                    "Airbyte spec is missing required fields. Please ensure you are passing"
                    " --config and that the passed config contains airbyte_spec."
                ) from KeyError
        return self._tag

    @property
    def docker_mounts(self) -> List[str]:
        if not self._docker_mounts:
            configured_mounts = []
            mounts = self.config.get("docker_mounts", [])
            mount: Dict[str, str]
            for mount in mounts:
                configured_mounts.extend(
                    [
                        "--mount",
                        (
                            f"source={mount['source']},target={mount['target']},type={mount.get('type', 'bind')}"
                        ),
                    ]
                )
            self._docker_mounts: List[str] = configured_mounts
        return self._docker_mounts

    @property
    @lru_cache
    def airbyte_catalog(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "wb") as f:
                f.write(orjson.dumps(self.config.get("airbyte_config", {})))
            proc = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{tmpdir}:{self.conf_dir}",
                ]
                + self.docker_mounts
                + [
                    f"{self.image}:{self.tag}",
                    "discover",
                    "--config",
                    f"{self.conf_dir}/config.json",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        for line in proc.stdout.decode("utf-8").splitlines():
            try:
                message = orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
            if message["type"] in (AirbyteMessage.LOG, AirbyteMessage.TRACE):
                self._process_log_message(message)
            elif message["type"] == AirbyteMessage.CATALOG:
                return message["catalog"]
        if proc.returncode != 0:
            raise AirbyteException(
                f"Discover failed with return code {proc.returncode}: {proc.stderr.decode()}"
            )
        raise AirbyteException(
            "Could not discover catalog, no catalog message received. \n"
            f"Stdout: {proc.stdout.decode('utf-8')}\n"
            f"Stderr: {proc.stderr.decode('utf-8')}"
        )

    @property
    def configured_airbyte_catalog(self) -> dict:
        output = {"streams": []}
        for stream in self.airbyte_catalog["streams"]:
            entry = self.catalog.get_stream(stream["name"])
            if entry is None:
                continue
            if entry.metadata.root.selected is False:
                continue
            try:
                sync_mode = REPLICATION_METHOD_MAP.get(
                    entry.replication_method.upper(), stream["supported_sync_modes"][0]
                )
                if sync_mode.lower() not in stream["supported_sync_modes"]:
                    sync_mode = stream["supported_sync_modes"][0]
            except (IndexError, KeyError):
                sync_mode = "FULL_REFRESH"
            output["streams"].append(
                {
                    "stream": stream,
                    "sync_mode": sync_mode.lower(),
                    "destination_sync_mode": NOOP_AIRBYTE_SYNC_MODE,
                }
            )
        return output

    def load_state(self, state: Dict[str, Any]) -> None:
        super().load_state(state)
        self.airbyte_state = state

    def sync_all(self) -> None:  # type: ignore
        stream: Stream
        self.eof_received = False
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue
            consumer = Thread(target=stream.sync, daemon=True)
            consumer.start()
            self.singer_consumers.append(consumer)
        t1 = time.perf_counter()
        with self.run_read() as airbyte_job:
            # Main processor loop
            while TapAirbyte.pipe_status is not PIPE_CLOSED:
                message = airbyte_job.stdout.readline()
                if not message and airbyte_job.poll() is not None:
                    self.eof_received = True
                    break
                try:
                    airbyte_message = orjson.loads(message)
                except orjson.JSONDecodeError:
                    if message:
                        self.logger.warning("Could not parse message: %s", message)
                    continue
                if airbyte_message["type"] == AirbyteMessage.RECORD:
                    stream_buffer: Queue = self.buffers.setdefault(
                        airbyte_message["record"]["stream"],
                        Queue(),
                    )
                    stream_buffer.put_nowait(airbyte_message["record"]["data"])
                elif airbyte_message["type"] in (
                    AirbyteMessage.LOG,
                    AirbyteMessage.TRACE,
                ):
                    self._process_log_message(airbyte_message)
                elif airbyte_message["type"] == AirbyteMessage.STATE:
                    state_message = airbyte_message["state"]
                    state_type = state_message["type"]
                    if "data" in state_message:
                        unpacked_state = state_message["data"]
                    elif state_type == "STREAM":
                        unpacked_state = state_message["stream"]
                    elif state_type == "GLOBAL":
                        unpacked_state = state_message["global"]
                    elif state_type == "LEGACY":
                        unpacked_state = state_message["legacy"]
                    self.airbyte_state = unpacked_state
                    with STDOUT_LOCK:
                        singer.write_message(singer.StateMessage(self.airbyte_state))
                else:
                    self.logger.warning("Unhandled message: %s", airbyte_message)
        # Daemon threads will be terminated when the main thread exits
        # so we do not need to wait on them to join after SIGPIPE
        if TapAirbyte.pipe_status is not PIPE_CLOSED:
            self.logger.info("Waiting for sync threads to finish...")
            for sync in self.singer_consumers:
                sync.join()
            # Write final state if EOF was received from Airbyte
            if self.eof_received:
                with STDOUT_LOCK:
                    singer.write_message(singer.StateMessage(self.airbyte_state))
        t2 = time.perf_counter()
        for stream in self.streams.values():
            stream.log_sync_costs()
        self.logger.info(f"Synced {len(self.streams)} streams in {t2 - t1:0.2f} seconds.")

    def discover_streams(self) -> List[Stream]:
        output_streams: List[AirbyteStream] = []
        stream: Dict[str, Any]
        for stream in self.airbyte_catalog["streams"]:
            airbyte_stream = AirbyteStream(
                tap=self,
                name=stream["name"],
                schema=stream["json_schema"],
            )
            try:
                # this is [str, ...?] in the Airbyte catalog
                if "cursor_field" in stream and isinstance(stream["cursor_field"][0], str):
                    airbyte_stream.replication_key = stream["cursor_field"][0]
                elif "source_defined_cursor" in stream and isinstance(stream["source_defined_cursor"], bool) and stream["source_defined_cursor"]:
                    # The stream has a source defined cursor. Try using that
                    if "default_cursor_field" in stream and isinstance(stream["default_cursor_field"][0], str):
                        airbyte_stream.replication_key = stream["default_cursor_field"][0]
                    else:
                        self.logger.warning(f"Stream {stream['name']} has a source defined cursor but no default_cursor_field.")
            except IndexError:
                pass
            try:
                # this is [[str, ...]] in the Airbyte catalog
                if "primary_key" in stream and isinstance(stream["primary_key"][0], List):
                    airbyte_stream.primary_keys = stream["primary_key"][0]
                elif "source_defined_primary_key" in stream and isinstance(
                    stream["source_defined_primary_key"][0], List
                ):
                    airbyte_stream.primary_keys = stream["source_defined_primary_key"][0]
            except IndexError:
                pass
            output_streams.append(airbyte_stream)
        return output_streams


class AirbyteStream(Stream):
    """Stream class for Airbyte streams."""

    def __init__(self, tap: TapAirbyte, schema: dict, name: str) -> None:
        super().__init__(tap, schema, name)
        self.parent = tap
        self._buffer: Optional[Queue] = None

    def _write_record_message(self, record: dict) -> None:
        for record_message in self._generate_record_messages(record):
            with STDOUT_LOCK:
                singer.write_message(record_message)

    def _write_state_message(self) -> None:
        pass

    def _increment_stream_state(self, *args, **kwargs) -> None:
        pass

    @property
    def buffer(self) -> Queue:
        """Get the buffer for the stream."""
        if not self._buffer:
            while self.name not in self.parent.buffers:
                if self.parent.eof_received:
                    # EOF received, no records for this stream
                    self._buffer = Queue()
                    break
                self.logger.debug(f"Waiting for records from Airbyte for stream {self.name}...")
                time.sleep(1)
            else:
                self._buffer = self.parent.buffers[self.name]
        return self._buffer

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get records from the stream."""
        while (
            self.parent.eof_received is False or not self.buffer.empty()
        ) and TapAirbyte.pipe_status is not PIPE_CLOSED:
            try:
                # The timeout permits the consumer to re-check the producer is alive
                yield self.buffer.get(timeout=1.0)
            except Empty:
                continue
            self.buffer.task_done()
        if self.name in self.parent.buffers:
            while not self.buffer.empty() and TapAirbyte.pipe_status is not PIPE_CLOSED:
                try:
                    yield self.buffer.get(timeout=1.0)
                except Empty:
                    break
                self.buffer.task_done()


if __name__ == "__main__":
    TapAirbyte.cli()
