"""Airbyte tap class."""
import atexit
import json
import subprocess
import time
from copy import deepcopy
from enum import Enum
from functools import lru_cache
from pathlib import Path, PurePath
from queue import Empty, Queue
from tempfile import TemporaryDirectory
from threading import Lock, Thread
from typing import Any, Callable, Dict, Iterable, List, Optional

import click
import singer_sdk._singerlib as singer
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.cli import common_options
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.tap_base import CliTestOptionValue

STDOUT_LOCK = Lock()


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
        ),
        th.Property(
            "connector_config",
            th.ObjectType(),
            required=False,
            default={},
            description="Configuration to pass through to the Airbyte source connector",
        ),
    ).to_dict()

    conf_dir: str = "/tmp"
    buffers: Dict[str, Queue] = {}
    airbyte_state: Dict[str, Any] = {}
    # Airbyte -> Demultiplexer -< Singer Streams
    airbyte_producer: Thread
    singer_consumers: List[Thread] = []

    @lru_cache
    def _pull_source_image(self):
        subprocess.run(
            ["docker", "pull", "--quiet", f"{self.image}:{self.tag}"],
            check=True,
            capture_output=True,
        )

    def run_help(self):
        subprocess.run(
            ["docker", "run", f"{self.image}:{self.tag}", "--help"],
            check=True,
        )

    def run_spec(self):
        output = subprocess.run(
            ["docker", "run", f"{self.image}:{self.tag}", "spec"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for line in output.stdout.splitlines():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                self.logger.warn("Could not parse message: %s", line)
                continue
            if message["type"] in (AirbyteMessage.LOG, AirbyteMessage.TRACE):
                self._process_log_message(message)
            elif message["type"] == AirbyteMessage.SPEC:
                return message["spec"]
            else:
                self.logger.warn("Unhandled message: %s", message)
        raise Exception("No spec found")

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
            is_flag=False,
            flag_value=CliTestOptionValue.All.value,
            default=CliTestOptionValue.Disabled,
            help=(
                "Use --test to sync a single record for each stream. "
                + "Use --test=schema to test schema output without syncing "
                + "records."
            ),
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
            test: CliTestOptionValue = CliTestOptionValue.Disabled,
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
            else:
                cls.discover_streams = lambda self: []

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

            try:
                tap: TapAirbyte = cls(  # type: ignore
                    config=config_files or None,
                    state=state,
                    catalog=catalog,
                    parse_env_config=parse_env_config,
                    validate_config=validate_config,
                )
            except Exception as exc:
                if about:
                    cls.logger.info("Tap-Airbyte instantiation failed. Printing basic about info.")
                    cls.print_about(format=format)
                    return
                raise exc

            if about:
                cls.logger.info(
                    "Tap-Airbyte instantiation succeeded. Printing spec-enriched about info."
                )
                spec = tap.run_spec()["connectionSpecification"]
                TapAirbyte.config_jsonschema["properties"]["connector_config"] = spec
                TapAirbyte.print_about(format=format)
                print("\nSetup Instructions:\n")

                # TODO: left for contemplation
                # print("settings:")
                # for prop, schema in spec["properties"].items():
                #     print(f"  - name: {prop}")
                #     print(f"    description: |")
                #     print(f"      {schema.get('description', '')}")
                #     if "examples" in schema:
                #         print(f"      Examples -> {schema['examples']}")
                #     if "pattern" in schema:
                #         print(f"      Pattern  -> {schema['pattern']}")

                # TODO: move this to a recursive function...
                print("connector_config:")
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
                return

            if discover:
                tap.run_discovery()
                if test == CliTestOptionValue.All.value:
                    tap.run_connection_test()
            elif test == CliTestOptionValue.All.value:
                tap.run_connection_test()
            elif test == CliTestOptionValue.Schema.value:
                tap.write_schemas()
            else:
                tap.sync_all()

        return cli

    def run_check(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "w") as f:
                json.dump(self.config["connector_config"], f)
            output = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{tmpdir}:{self.conf_dir}",
                    f"{self.image}:{self.tag}",
                    "check",
                    "--config",
                    f"{self.conf_dir}/config.json",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        for line in output.stdout.splitlines():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                self.logger.warn("Could not parse message: %s", line)
                continue
            if message["type"] in (AirbyteMessage.LOG, AirbyteMessage.TRACE):
                self._process_log_message(message)
            elif message["type"] == AirbyteMessage.CONNECTION_STATUS:
                if message["connectionStatus"]["status"] == "SUCCEEDED":
                    self.logger.info("Connection check succeeded")
                else:
                    self.logger.error(
                        "Connection check failed: %s",
                        message["connectionStatus"]["message"],
                    )
            else:
                self.logger.warn("Unhandled message: %s", message)

    def load_state(self, state: dict[str, Any]) -> None:
        super().load_state(state)
        self.airbyte_state = state

    def run_read(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "w") as config, open(
                f"{tmpdir}/catalog.json", "w"
            ) as catalog:
                json.dump(self.config.get("connector_config", {}), config)
                json.dump(self.configured_airbyte_catalog, catalog)
            if self.airbyte_state:
                with open(f"{tmpdir}/state.json", "w") as state:
                    self.logger.debug("Using state: %s", self.airbyte_state)
                    json.dump(self.airbyte_state, state)
            proc = subprocess.Popen(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{tmpdir}:{self.conf_dir}",
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
                text=True,
            )
            atexit.register(proc.kill)
            while True:
                message = proc.stdout.readline()
                if message == "" and proc.poll() is not None:
                    break
                try:
                    airbyte_message = json.loads(message)
                except json.JSONDecodeError:
                    self.logger.warn("Could not parse message: %s", message)
                    continue
                if airbyte_message["type"] in (
                    AirbyteMessage.LOG,
                    AirbyteMessage.TRACE,
                ):
                    self._process_log_message(airbyte_message)
                elif airbyte_message["type"] == AirbyteMessage.STATE:
                    state_message = airbyte_message["state"]
                    if "data" in state_message:
                        unpacked_state = state_message["data"]
                    elif "type" == "STREAM":
                        unpacked_state = state_message["stream"]
                    elif "type" == "GLOBAL":
                        unpacked_state = state_message["global"]
                    elif "type" == "LEGACY":
                        unpacked_state = state_message["legacy"]
                    self.airbyte_state = unpacked_state
                    with STDOUT_LOCK:
                        singer.write_message(singer.StateMessage(self.airbyte_state))
                elif airbyte_message["type"] == AirbyteMessage.RECORD:
                    stream_buffer: Queue = self.buffers.setdefault(
                        airbyte_message["record"]["stream"],
                        Queue(),
                    )
                    stream_buffer.put_nowait(airbyte_message["record"]["data"])
                else:
                    self.logger.warn("Unhandled message: %s", airbyte_message)
            atexit.unregister(proc.kill)

    def _process_log_message(self, airbyte_message: Dict[str, Any]) -> None:
        if airbyte_message["type"] == AirbyteMessage.LOG:
            self.logger.info(airbyte_message["log"])
        elif airbyte_message["type"] == AirbyteMessage.TRACE:
            if airbyte_message["trace"].get("type") == "ERROR":
                self.logger.critical(
                    airbyte_message["trace"]["error"]["message"],
                    exc_info=Exception(
                        airbyte_message["trace"]["error"].get(
                            "stack_trace", "No stack trace available"
                        )
                    ),
                )
                exit(1)
            self.logger.debug(airbyte_message["trace"])

    def sync_one(self, stream: Stream) -> None:
        stream.sync()
        stream.finalize_state_progress_markers()
        stream._write_state_message()

    def sync_all(self) -> None:  # type: ignore
        self.airbyte_producer = Thread(target=self.run_read, daemon=True)
        self.airbyte_producer.start()
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: Stream
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue
            consumer = Thread(target=self.sync_one, args=(stream,), daemon=True)
            consumer.start()
            self.singer_consumers.append(consumer)
        self.airbyte_producer.join()
        for sync in self.singer_consumers:
            sync.join()
        with STDOUT_LOCK:
            singer.write_message(singer.StateMessage(self.airbyte_state))
        for stream in self.streams.values():
            stream.log_sync_costs()

    @lru_cache
    def setup(self) -> None:
        self.image = self.config["airbyte_spec"]["image"]
        self.tag = self.config["airbyte_spec"].get("tag", "latest")
        self._pull_source_image()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.setup()

    @property
    @lru_cache
    def airbyte_catalog(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "w") as f:
                json.dump(self.config["connector_config"], f)
            discover = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{tmpdir}:{self.conf_dir}",
                    f"{self.image}:{self.tag}",
                    "discover",
                    "--config",
                    f"{self.conf_dir}/config.json",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ).stdout
        for line in discover.splitlines():
            try:
                airbyte_message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if airbyte_message["type"] in (AirbyteMessage.LOG, AirbyteMessage.TRACE):
                self._process_log_message(airbyte_message)
            elif airbyte_message["type"] == AirbyteMessage.CATALOG:
                return airbyte_message["catalog"]
        raise Exception("Could not discover catalog")

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
            except (IndexError, KeyError):
                sync_mode = "FULL_REFRESH"
            output["streams"].append(
                {
                    "stream": stream,
                    # This should be sourced from the user's config w/ default from catalog 0 index
                    "sync_mode": sync_mode.lower(),
                    # This is not used by the Singer targets we pipe to
                    "destination_sync_mode": NOOP_AIRBYTE_SYNC_MODE,
                }
            )
        return output

    def discover_streams(self) -> List[Stream]:
        self.setup()
        temp_airbyte_catalog: Dict[str, Any] = deepcopy(self.airbyte_catalog)
        output_streams: List[AirbyteStream] = []
        stream: Dict[str, Any]
        for stream in temp_airbyte_catalog["streams"]:
            stream_name = stream["name"]
            stream_schema = stream["json_schema"]
            airbyte_stream = AirbyteStream(
                tap=self,
                name=stream_name,
                schema=stream_schema,
            )
            try:
                # this is [str, ...?] in the Airbyte catalog
                if "cursor_field" in stream and isinstance(stream["cursor_field"][0], str):
                    airbyte_stream.replication_key = stream["cursor_field"][0]
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
            output_streams.append(
                AirbyteStream(
                    tap=self,
                    name=stream_name,
                    schema=stream_schema,
                )
            )
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

    @property
    def buffer(self) -> Queue:
        """Get the buffer for the stream."""
        if not self._buffer:
            while self.name not in self.parent.buffers:
                if not self.parent.airbyte_producer.is_alive():
                    self.logger.debug(
                        f"Airbyte producer died before records were received for stream {self.name}"
                    )
                    self._buffer = Queue()
                    break
                self.logger.debug(f"Waiting for records from Airbyte for stream {self.name}...")
                time.sleep(1)
            else:
                self._buffer = self.parent.buffers[self.name]
        return self._buffer

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get records from the stream."""
        while self.parent.airbyte_producer.is_alive():
            try:
                # The timeout permits the consumer to re-check the producer is alive
                yield self.buffer.get(timeout=1)
            except Empty:
                continue
            self.buffer.task_done()
        if self.name in self.parent.buffers:
            while not self.buffer.empty():
                yield self.buffer.get()
                self.buffer.task_done()


if __name__ == "__main__":
    TapAirbyte.cli()
