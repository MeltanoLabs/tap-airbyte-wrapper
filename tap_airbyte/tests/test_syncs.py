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

import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import orjson

from tap_airbyte.tap import TapAirbyte


def test_weather_sync():
    """Run a sync and compare the output to a fixture derived from a public dataset.
    This test provides a very strong guarantee that the tap is working as expected."""

    tap = TapAirbyte(
        config={
            "airbyte_spec": {"image": "airbyte/source-file", "tag": "0.2.32"},
            "airbyte_config": {
                "dataset_name": "test",
                "format": "csv",
                "url": "https://raw.githubusercontent.com/fivethirtyeight/data/master/us-weather-history/KPHX.csv",
                "provider": {
                    "storage": "HTTPS",
                    "user_agent": True,
                },
            },
        },
        parse_env_config=True,
    )

    tap.ORJSON_OPTS |= orjson.OPT_SORT_KEYS

    FIXTURE = Path(__file__).parent.joinpath("fixtures", "KPHX.singer")
    SINGER_DUMP = FIXTURE.read_text()

    stdout = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    stderr = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(stdout), redirect_stderr(stderr):
        tap.sync_all()
    stdout.seek(0), stderr.seek(0)

    inp = stdout.readlines()
    dmp = SINGER_DUMP.splitlines()

    assert len(inp) == len(dmp), f"Expected {len(dmp)} stdout lines, got {len(inp)}"

    for no, test_case, baseline in enumerate(zip(stdout.readlines(), SINGER_DUMP.splitlines())):
        try:
            parsed_test_case, parsed_baseline = json.loads(test_case), json.loads(baseline)
            if parsed_test_case["type"] == "RECORD":
                assert (
                    parsed_baseline["type"] == "RECORD"
                ), f"Parsed message at line {no} is not a record but the test input is"
                parsed_baseline.pop("time_extracted", None)
                parsed_test_case.pop("time_extracted", None)
                assert (
                    parsed_baseline == parsed_test_case
                ), f"{no}: {parsed_baseline} != {parsed_test_case}"
        except json.JSONDecodeError:
            pass


def test_poke_sync():
    """Run a sync and compare the output to a fixture derived from a public dataset.
    This test provides a very strong guarantee that the tap is working as expected."""

    tap = TapAirbyte(
        config={
            "airbyte_spec": {"image": "airbyte/source-pokeapi", "tag": "0.1.5"},
            "airbyte_config": {
                # sketch -> spore, endeavor, extreme speed, destiny bond w/ focus sash
                # if you know, you know.
                "pokemon_name": "smeargle",
            },
        },
    )

    tap.ORJSON_OPTS |= orjson.OPT_SORT_KEYS

    FIXTURE = Path(__file__).parent.joinpath("fixtures", "SMEARGLE.singer")
    SINGER_DUMP = FIXTURE.read_text()

    stdout = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    stderr = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(stdout), redirect_stderr(stderr):
        tap.sync_all()
    stdout.seek(0), stderr.seek(0)

    inp = stdout.readlines()
    dmp = SINGER_DUMP.splitlines()

    assert len(inp) == len(dmp), f"Expected {len(dmp)} stdout lines, got {len(inp)}"

    for no, test_case, baseline in enumerate(zip(stdout.readlines(), SINGER_DUMP.splitlines())):
        try:
            parsed_test_case, parsed_baseline = json.loads(test_case), json.loads(baseline)
            if parsed_test_case["type"] == "RECORD":
                assert (
                    parsed_baseline["type"] == "RECORD"
                ), f"Parsed message at line {no} is not a record but the test input is"
                parsed_baseline.pop("time_extracted", None)
                parsed_test_case.pop("time_extracted", None)
                assert (
                    parsed_baseline == parsed_test_case
                ), f"{no}: {parsed_baseline} != {parsed_test_case}"
        except json.JSONDecodeError:
            pass


def test_pub_apis_sync():
    """Run a sync and compare the output to a fixture derived from a public dataset.
    This test provides a very strong guarantee that the tap is working as expected."""

    tap = TapAirbyte(
        config={
            "airbyte_spec": {"image": "airbyte/source-public-apis", "tag": "0.1.0"},
            "airbyte_config": {},
        },
    )

    tap.ORJSON_OPTS |= orjson.OPT_SORT_KEYS

    FIXTURE = Path(__file__).parent.joinpath("fixtures", "PUBLIC_APIS.singer")
    SINGER_DUMP = FIXTURE.read_text()

    stdout = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    stderr = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(stdout), redirect_stderr(stderr):
        tap.sync_all()
    stdout.seek(0), stderr.seek(0)

    inp = stdout.readlines()
    dmp = SINGER_DUMP.splitlines()

    assert len(inp) == len(dmp), f"Expected {len(dmp)} stdout lines, got {len(inp)}"

    for no, test_case, baseline in enumerate(zip(stdout.readlines(), SINGER_DUMP.splitlines())):
        try:
            parsed_test_case, parsed_baseline = json.loads(test_case), json.loads(baseline)
            if parsed_test_case["type"] == "RECORD":
                assert (
                    parsed_baseline["type"] == "RECORD"
                ), f"Parsed message at line {no} is not a record but the test input is"
                parsed_baseline.pop("time_extracted", None)
                parsed_test_case.pop("time_extracted", None)
                assert (
                    parsed_baseline == parsed_test_case
                ), f"{no}: {parsed_baseline} != {parsed_test_case}"
        except json.JSONDecodeError:
            pass


def test_docker_mount_sync():
    """This test ensures that the tap can mount a docker volume and read from it."""

    data = Path(__file__).parent.joinpath("fixtures", "KPHX.csv")
    tap = TapAirbyte(
        config={
            "airbyte_spec": {"image": "airbyte/source-file", "tag": "0.2.32"},
            "airbyte_config": {
                "dataset_name": "test",
                "format": "csv",
                "url": "/local/KPHX.csv",
                "provider": {
                    "storage": "local",
                },
            },
            "docker_mounts": [
                {
                    "source": str(data.parent),
                    "target": "/local",
                    "type": "bind",
                }
            ],
        },
    )

    tap.ORJSON_OPTS |= orjson.OPT_SORT_KEYS

    FIXTURE = Path(__file__).parent.joinpath("fixtures", "KPHX.singer")
    SINGER_DUMP = FIXTURE.read_text()

    stdout = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    stderr = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(stdout), redirect_stderr(stderr):
        tap.sync_all()
    stdout.seek(0), stderr.seek(0)

    inp = stdout.readlines()
    dmp = SINGER_DUMP.splitlines(keepends=True)

    assert len(inp) == len(dmp), f"Expected {len(dmp)} stdout lines, got {len(inp)}"

    for no, test_case, baseline in enumerate(zip(stdout.readlines(), SINGER_DUMP.splitlines())):
        try:
            parsed_test_case, parsed_baseline = json.loads(test_case), json.loads(baseline)
            if parsed_test_case["type"] == "RECORD":
                assert (
                    parsed_baseline["type"] == "RECORD"
                ), f"Parsed message at line {no} is not a record but the test input is"
                parsed_baseline.pop("time_extracted", None)
                parsed_test_case.pop("time_extracted", None)
                assert (
                    parsed_baseline == parsed_test_case
                ), f"{no}: {parsed_baseline} != {parsed_test_case}"
        except json.JSONDecodeError:
            pass


if __name__ == "__main__":
    test_weather_sync()
    test_poke_sync()
    test_pub_apis_sync()
    test_docker_mount_sync()
