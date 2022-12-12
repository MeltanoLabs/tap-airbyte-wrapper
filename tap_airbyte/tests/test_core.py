"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_standard_tap_tests

from tap_airbyte.tap import TapAirbyte

SAMPLE_CONFIG = {
    "_image": "airbyte/source-asana",
    "_tag": "0.1.5",
    "connector_config": {
        "credentials": {
            "personal_access_token": "1/1201980351759625:77e6c03195c3788a36f0f8c139527ee7",
        },
    },
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapAirbyte, config=SAMPLE_CONFIG)
    for test in tests:
        test()


test_standard_tap_tests()
