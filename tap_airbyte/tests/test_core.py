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
            "airbyte_spec": {"image": "airbyte/source-pokeapi", "tag": "0.1.5"},
            "airbyte_config": {
                "pokemon_name": "infernape",
            },
        },
    )
    for test in tests:
        test()


if __name__ == "__main__":
    test_standard_tap_tests()
