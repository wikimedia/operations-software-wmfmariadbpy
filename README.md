[![pipeline status](https://gitlab.wikimedia.org/repos/sre/wmfmariadbpy/badges/main/pipeline.svg)](https://gitlab.wikimedia.org/repos/sre/wmfmariadbpy/-/commits/main) 
[![Latest Release](https://gitlab.wikimedia.org/repos/sre/wmfmariadbpy/-/badges/release.svg)](https://gitlab.wikimedia.org/repos/sre/wmfmariadbpy/-/releases)
Collection of Python classes and scripts to operate with MariaDB servers.

## Dependencies

Some dependencies are required in order to run the scripts and the tests. The easiest way to work is by using a virtualenv:

```
tox --notest
tox -e py3-venv -- <some command>
```

## Run tests

Tests are located under *wmfmariadbpy/test*. They are split between unit and integration tests. To run unit tests:

```
tox -e py3-unit
```

### Integration tests

Requirements:
* docker (and be in the `docker` group)
* `mysql` (client binary)
* `pt-online-schema-change`

On a debian/ubuntu system, this should install the required packages:
```bash
sudo apt install docker.io mariadb-client percona-toolkit
# You'll need to relog after this:
sudo usermod -a -G docker $LOGNAME
```

Then:
```
tox -e py3-integration
```
If it ever ends up in a broken state:
```
tox -e py3-integration_env stop
```

### Tests coverage report

To run the unit and integration tests and generate a HTML coverage report under `cover/`

```
tox -e py3-cover
```

## Code style compliance

To check the code style compliance:

```
tox -e py3-flake8
```

To check if the formatters would make changes:

```
tox -e py3-format
```

## Reformat the code with 'isort' and 'black'

```
tox -e py3-reformat
```

