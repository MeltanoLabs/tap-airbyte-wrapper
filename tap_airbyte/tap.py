"""Airbyte tap class."""
import atexit
import json
import subprocess
import time
from copy import deepcopy
from enum import Enum
from functools import lru_cache
from queue import Empty, Queue
from tempfile import TemporaryDirectory
from threading import Lock, Thread
from typing import Any, Dict, Iterable, List, Optional

import singer_sdk._singerlib as singer
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

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
    state: Dict[str, Any] = {}
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
                self.logger.info(message["spec"])
                return message["spec"]
            else:
                self.logger.warn("Unhandled message: %s", message)
        raise Exception("No spec found")

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

    def run_read(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "w") as config, open(
                f"{tmpdir}/catalog.json", "w"
            ) as catalog:
                json.dump(self.config.get("connector_config", {}), config)
                json.dump(self.configured_airbyte_catalog, catalog)
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
                ],
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
                    self.state = airbyte_message["state"]
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
            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue
            consumer = Thread(target=self.sync_one, args=(stream,), daemon=True)
            consumer.start()
            self.singer_consumers.append(consumer)
        self.airbyte_producer.join()
        for sync in self.singer_consumers:
            sync.join()
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
            # TODO: figure out select, when is self.streams populated?
            # if stream["name"] not in self.streams:
            #   print(f"{stream['name']:.<39}.skipped")
            #   continue
            output["streams"].append(
                {
                    "stream": stream,
                    # This should be sourced from the user's config w/ default from catalog 0 index
                    "sync_mode": "incremental",
                    # This is not used by the Singer targets we pipe to
                    "destination_sync_mode": "append",
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
                if "cursor_field" in stream and isinstance(
                    stream["cursor_field"][0], str
                ):
                    # this is [str, ...?] in the Airbyte catalog
                    airbyte_stream.replication_key = stream["cursor_field"][0]
            except IndexError:
                pass
            try:
                if "primary_key" in stream and isinstance(
                    stream["primary_key"][0], List
                ):
                    # this is [[str, ...]] in the Airbyte catalog
                    airbyte_stream.primary_keys = stream["primary_key"][0]
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
        self.queue = Queue()
        self._buffer: Optional[Queue] = None

    def _write_record_message(self, record: dict) -> None:
        for record_message in self._generate_record_messages(record):
            with STDOUT_LOCK:
                singer.write_message(record_message)

    @property
    def buffer(self) -> Queue:
        """Get the buffer for the stream."""
        if not self._buffer:
            while self.name not in self.parent.buffers:
                self.logger.debug(
                    f"Waiting for records from Airbyte for stream {self.name}..."
                )
                time.sleep(1)
                continue
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
        if not self.name in self.parent.buffers:
            return
        while not self.buffer.empty():
            yield self.buffer.get()
            self.buffer.task_done()


if __name__ == "__main__":
    TapAirbyte.cli()
