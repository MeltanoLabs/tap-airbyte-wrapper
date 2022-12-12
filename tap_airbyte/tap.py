"""Airbyte tap class."""
import atexit
import json
import subprocess
import time
from copy import deepcopy
from enum import Enum
from functools import lru_cache
from tempfile import TemporaryDirectory
from threading import Lock, Thread
from typing import Iterable, List, Optional

from singer_sdk import Stream, Tap
from singer_sdk import typing as th


class AirbyteMessage(str, Enum):
    RECORD = "RECORD"
    STATE = "STATE"
    LOG = "LOG"
    TRACE = "TRACE"
    ACTIVATION = "ACTIVATION"


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
    buffers = {}
    state = {}
    airbyte_job: Thread
    # {spec,check,discover,read} are all implemented as methods

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
        subprocess.run(
            ["docker", "run", f"{self.image}:{self.tag}", "spec"], check=True
        )

    def run_check(self):
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/config.json", "w") as f:
                json.dump(self.config["connector_config"], f)
            subprocess.run(
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
                check=True,
            )

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
                # print(airbyte_message)
                if airbyte_message["type"] == AirbyteMessage.LOG:
                    self.logger.info(airbyte_message["log"])
                elif airbyte_message["type"] == AirbyteMessage.TRACE:
                    self.logger.critical(airbyte_message["trace"])
                    exit(1)
                elif airbyte_message["type"] == AirbyteMessage.STATE:
                    self.state = airbyte_message["state"]
                elif airbyte_message["type"] == AirbyteMessage.RECORD:
                    stream_buffer = self.buffers.setdefault(
                        airbyte_message["record"]["stream"],
                        {"stream_buffer_lock": Lock(), "records": []},
                    )
                    with stream_buffer["stream_buffer_lock"]:
                        stream_buffer["records"].append(
                            airbyte_message["record"]["data"]
                        )
                else:
                    self.logger.warn("Unhandled message: %s", airbyte_message)
            atexit.unregister(proc.kill)

    def sync_all(self) -> None:  # type: ignore
        self.airbyte_job = Thread(target=self.run_read, daemon=True)
        self.airbyte_job.start()
        super().sync_all()

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
            airbyte_catalog = json.loads(
                subprocess.run(
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
                    check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                ).stdout
            )
        return airbyte_catalog["catalog"]

    @property
    def configured_airbyte_catalog(self) -> dict:
        output = {"streams": []}
        for stream in self.airbyte_catalog["streams"]:
            # TODO: figure out select
            # if stream["name"] not in self.streams:
                # as we try to introduce `select` interoperability
                # print(f"{stream['name']:.<39}.skipped")
                # continue
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
        temp_airbyte_catalog = deepcopy(self.airbyte_catalog)
        output_streams = []
        for stream in temp_airbyte_catalog["streams"]:
            stream_name = stream["name"]
            stream_schema = stream["json_schema"]
            airbyte_stream = AirbyteStream(
                tap=self,
                name=stream_name,
                schema=stream_schema,
            )
            if stream.get("source_defined_cursor"):
                airbyte_stream.primary_keys = stream["default_cursor_field"][0]
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

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get records from the stream."""
        while self.parent.airbyte_job.is_alive():
            if not (
                self.name in self.parent.buffers
                and len(self.parent.buffers[self.name]["records"]) > 0
            ):
                self.logger.debug(
                    f"Waiting for records from Airbyte for stream {self.name}..."
                )
                time.sleep(1)
                continue
            with self.parent.buffers[self.name]["stream_buffer_lock"]:
                while len(self.parent.buffers[self.name]["records"]) > 10:
                    yield self.parent.buffers[self.name]["records"].pop(0)
        else:
            if not self.name in self.parent.buffers:
                return
            with self.parent.buffers[self.name]["stream_buffer_lock"]:
                while len(self.parent.buffers[self.name]["records"]) > 0:
                    yield self.parent.buffers[self.name]["records"].pop(0)


if __name__ == "__main__":
    TapAirbyte.cli()
