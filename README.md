[![PyPI version](https://badge.fury.io/py/mergin-client.svg)](https://badge.fury.io/py/mergin-client)
[![Build and upload to PyPI](https://github.com/MerginMaps/python-api-client/actions/workflows/python_packages.yml/badge.svg)](https://github.com/MerginMaps/python-api-client/actions/workflows/python_packages.yml)

[![Code Style](https://github.com/MerginMaps/python-api-client/actions/workflows/code_style.yml/badge.svg)](https://github.com/MerginMaps/python-api-client/actions/workflows/code_style.yml)
[![Auto Tests/Package](https://github.com/MerginMaps/python-api-client/workflows/Auto%20Tests/badge.svg)](https://github.com/MerginMaps/python-api-client/actions?query=workflow%3A%22Auto+Tests%22)
[![Coverage Status](https://img.shields.io/coveralls/MerginMaps/python-api-client.svg)](https://coveralls.io/github/MerginMaps/python-api-client)

# Mergin Maps Python Client

This repository contains a Python client module for access to [Mergin Maps](https://merginmaps.com/) service and a command-line tool for easy access to data stored in Mergin Maps.

<div><img align="left" width="45" height="45" src="https://raw.githubusercontent.com/MerginMaps/docs/main/src/.vuepress/public/slack.svg"><a href="https://merginmaps.com/community/join">Join our community chat</a><br/>and ask questions!</div><br />


To install the module:
```bash
    pip3 install mergin-client
```

Note: Check also [Mergin Maps Cpp Client](https://github.com/MerginMaps/cpp-api-client)

## Using Python API

To use Mergin Maps from Python, it is only needed to create `MerginClient` object and then use it:

```python
import mergin

client = mergin.MerginClient(login='john', password='topsecret')
client.download_project('lutraconsulting/Basic survey', '/tmp/basic-survey')
```

If you have Mergin Maps plugin for QGIS installed and you want to use it from QGIS' Python console

```python
import Mergin.mergin as mergin

client = mergin.MerginClient(login='john', password='topsecret')
```

## Command-line Tool

When the module is installed, it comes with `mergin` command line tool.

```
$ mergin --help
Usage: mergin [OPTIONS] COMMAND [ARGS]...

  Command line interface for the Mergin Maps client module. For user
  authentication on server there are two options:

   1. authorization token environment variable (MERGIN_AUTH) is defined, or
   2. username and password need to be given either as environment variables
   (MERGIN_USERNAME, MERGIN_PASSWORD),  or as command options (--username,
   --password).

  Run `mergin --username <your_user> login` to see how to set the token
  variable manually.

Options:
  --url TEXT         Mergin Maps server URL. Default is:
                     https://app.merginmaps.com/
  --auth-token TEXT  Mergin authentication token string
  --username TEXT
  --password TEXT
  --help             Show this message and exit.

Commands:
  clone                Clone project from server.
  create               Create a new project on Mergin Maps server.
  download             Download last version of Mergin Maps project
  download-file        Download project file at specified version.
  list-projects        List projects on the server
  login                Login to the service and see how to set the token...
  pull                 Fetch changes from Mergin Maps repository
  push                 Upload local changes into Mergin Maps repository
  remove               Remove project from server.
  rename               Rename project in Mergin Maps repository.
  reset                Reset local changes in project.
  share                Fetch permissions to project
  share-add            Add permissions to [users] to project
  share-remove         Remove [users] permissions from project
  show-file-changeset  Displays information about project changes.
  show-file-history    Displays information about a single version of a...
  show-version         Displays information about a single version of a...
  status               Show all changes in project files - upstream and...
```

### Examples

For example, to download a project:

```
$ mergin --username john download john/project1 ~/mergin/project1
```
To download a specific version of a project:
```
$ mergin --username john download --version v42 john/project1 ~/mergin/project1
```

To download a sepecific version of a single file:

1. First you need to download the project:
```
mergin --username john download john/myproject
```

2. Go to the project directory
```
cd myproject
```

3. Download the version of a file you want:
```
mergin --username john download-file --version v273 myfile.gpkg /tmp/myfile-v273.gpkg
```

If you do not want to specify username on the command line and be asked for you password every time,
it is possible to set env variables MERGIN_USERNAME and MERGIN_PASSWORD.

When a project is downloaded, `mergin` commands can be run in the project's
working directory:

1. get status of the project (check if there are any local/remote changes)
   ```
   $ mergin status
   ```
2. pull changes from Mergin Maps service
   ```
   $ mergin pull
   ```
3. push local changes to Mergin Maps service
   ```
   $ mergin push
   ```

### Using CLI with auth token

If you plan to run `mergin` command multiple times and you wish to avoid logging in every time,
you can use "login" command to get authorization token.
It will ask for password and then output environment variable with auth token. The returned token
is not permanent - it will expire after several hours.
```bash
$ mergin --username john login
Password: topsecret
Login successful!
To set the MERGIN_AUTH variable run in Linux:
export MERGIN_AUTH="Bearer ......."
In Windows:
SET MERGIN_AUTH=Bearer .......
```
When setting the variable in Windows you do not quote the value.

When the MERGIN_AUTH env variable is set (or passed with `--auth-token` command line argument),
it is possible to run other commands without specifying username/password.


## Development

### Installing deps

Python 3.7+ required. Create `mergin/deps` folder where [geodiff](https://github.com/MerginMaps/geodiff) lib is supposed to be and install dependencies:
```bash
    rm -r mergin/deps
    mkdir mergin/deps
    pip install python-dateutil pytz
    pip install pygeodiff --target=mergin/deps
```

For using mergin client with its dependencies packaged locally run:
```bash
    pip install wheel
    python3 setup.py sdist bdist_wheel
    mkdir -p mergin/deps
    pip wheel -r mergin_client.egg-info/requires.txt -w mergin/deps
    unzip mergin/deps/pygeodiff-*.whl -d mergin/deps
    pip install --editable .
```

### Tests
For running test do:

```bash
    cd mergin
    export TEST_MERGIN_URL=<url> # testing server
    export TEST_API_USERNAME=<username>
    export TEST_API_PASSWORD=<pwd>
    export TEST_API_USERNAME2=<username2>
    export TEST_API_PASSWORD2=<pwd2>
    # workspace name with controlled available storage space (e.g. 20MB), default value: testpluginstorage
    export TEST_STORAGE_WORKSPACE=<workspacename>
    pip install pytest pytest-cov coveralls
    pytest --cov-report html --cov=mergin mergin/test/
```
