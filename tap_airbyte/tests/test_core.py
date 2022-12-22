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

"""Tests standard tap features using the built-in SDK tests library"""
import os

from singer_sdk.testing import get_standard_tap_tests

from tap_airbyte.tap import TapAirbyte


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapAirbyte,
        config={
            "airbyte_spec": {"image": "airbyte/source-github", "tag": "0.3.8"},
            "airbyte_config": {
                "credentials": {
                    "access_token": os.getenv("ACCESS_TOKEN", "dummy"),
                },
                "start_date": "2021-01-01T00:00:00Z",
                "repository": "z3z1ma/tap-airbyte",
                "page_size_for_large_streams": 20,
            },
        },
    )
    for test in tests:
        test()


if __name__ == "__main__":
    test_standard_tap_tests()
