<h1 align="center">Tap-Airbyte-Wrapper</h1>

<p align="center">
<a href="https://github.com/z3z1ma/tap-airbyte/actions/"><img alt="Actions Status" src="https://github.com/z3z1ma/tap-airbyte/actions/workflows/ci.yml/badge.svg"></a>
<a href="https://github.com/z3z1ma/tap-airbyte/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

`tap-airbyte` is a Singer tap that wraps *all* Airbyte sources implicitly. This adds over 250 immediately usable extractors to the broader Singer ecosystem. This opens up high quality connectors for an expanded audience further democratizing ELT and encourages contributions upstream where system experts using an airbyte source via this wrapper may be inclined to contribute to the connector source, open issues, etc if it is the better option than what's available in the Singer catalog alone.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Configuration 📝

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| airbyte_spec        | True     | None    | Specification for the Airbyte source connector. This is a JSON object minimally containing the `image` key. The `tag` key is optional and defaults to `latest`. |
| airbyte_config      | False    | None    | Configuration to pass through to the Airbyte source connector, this can be gleaned by running the the tap with the `--about` flag and the `--config` flag pointing to a file containing the `airbyte_spec` configuration. This is a JSON object. |
| docker_mounts       | False    | None    | Docker mounts to mount to the container. Expects a list of maps containing source, target, and type as is documented in the docker --mount [documentation](https://docs.docker.com/storage/bind-mounts/#choose-the--v-or---mount-flag) |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |


### Configure using environment variables ✏️

`OCI_RUNTIME` can be set to override the default of `docker`. This lets the tap work with podman, nerdctl, colima, and so on.

```sh
OCI_RUNTIME=nerdctl meltano run tap-pokeapi target-jsonl
```

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization 👮🏽‍♂️

First, configure your tap by creating a configuration json file. In this example we will call it `github.json` since this tap may use many configurations for different sources.


> ❗️ Remember the required keys for `airbyte_config` can be dumped to stdout by running `tap-airbyte --about --config /path/to/FILE` where FILE minimally contains just the airbyte_spec.image value

```json
{
  "airbyte_spec": {
    "image": "source-github"
  },
  "airbyte_config": {
    "credentials": {
      "access_token": "..."
    },
    "repositories": "z3z1ma/*",
  }
}
```

Run the built in Airbyte connection test to validate your configuration like this where `github.json` represents the above config (note the choice of file name is purely for illustration):

```bash
tap-airbyte --config ./github.json --test
```

The `--test` flag will **validate your configuration** as being able to access the configured data source! Be sure to use it. With meltano, configuration is implicitly passed based on what's in your meltano.yml configuration which simplifies it to just `meltano invoke tap-airbyte --test`

See more configuration examples in the [sync tests](tap_airbyte/tests/test_syncs.py)

## Usage 👷‍♀️

You can easily run `tap-airbyte` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly 🔨

```bash
tap-airbyte --version
tap-airbyte --help
tap-airbyte --config CONFIG --discover > ./catalog.json
```

## Developer Resources 👩🏼‍💻

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests 🧪

Create tests within the `tap_airbyte/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-airbyte` CLI interface directly using `poetry run`:

```bash
poetry run tap-airbyte --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._


Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-airbyte
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-airbyte --version
# OR run a test `elt` pipeline:
meltano elt tap-airbyte target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
