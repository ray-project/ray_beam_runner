Ray-based Apache Beam Runner
==========================
This is a WIP proof of concept implementation undergoing frequent breaking changes and should not be used in production.


## The Portable Ray Beam Runner

The directory `ray_beam_runner/portability` contains a prototype for an implementation of a [Beam](https://beam.apache.org)
runner for [Ray](https://ray.io) that relies on Beam's portability framework.

### Install and build from source

To install the existing Ray Beam runner from a clone of this repository, you can follow the next steps:

```shell
# First create a virtual environment to install and run Python dependencies
virtualenv venv
. venv/bin/activate

# Install development dependencies for the project
pip install -r requirements_dev.txt

# Create a local installation to include test dependencies
pip install -e '.[test]'
```

### Testing

The project has extensive unit tests that can run on a local environment. Tests that verify the basic runner
functionality exist in the file `ray_beam_runner/portability/ray_runner_test.py`.

**To run the runner functionality test suite** for the Ray Beam Runner, you can run the following command:

```shell
pytest ray_beam_runner/portability/ray_runner_test.py
```

If you got `ImportError: urllib3 v2.0 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'OpenSSL XXXXX'. See: https://github.com/urllib3/urllib3/issues/2168`, following instructions on the given URL might solve. You can either have `OpenSSL` updated or explicitly restrict version of `urllib3` library.


To run all local unit tests, you can simply run `pytest` from the root directory.

### Found issues? Want to help?

Please file any problems with the runner in [this repository's issue section](https://github.com/ray-project/ray_beam_runner/issues).
If you would like to help, please **look at the issues as well**. You can pick up one of them and try to implement
it.

### Performance testing

```shell
# TODO: Write these tests and document how to run them.
```