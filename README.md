# Mergin Python Client

This repository contains a Python client module for access to [Mergin](https://public.cloudmergin.com/)
service and a command-line tool for easy access to data stored in Mergin.

To install the module:

    pip3 install mergin-client

[![PyPI version](https://badge.fury.io/py/mergin-client.svg)](https://badge.fury.io/py/mergin-client)
[![Auto Tests](https://github.com/lutraconsulting/mergin-py-client/workflows/Auto%20Tests/badge.svg)](https://github.com/lutraconsulting/mergin-py-client/actions?query=workflow%3A%22Auto+Tests%22)
[![Coverage Status](https://img.shields.io/coveralls/lutraconsulting/mergin-py-client.svg)](https://coveralls.io/github/lutraconsulting/mergin-py-client)

Note: Check also [Mergin Cpp Client](https://github.com/lutraconsulting/mergin-cpp-client)

## Using Python API

To use Mergin from Python, it is only needed to create `MerginClient` object and then use it:

```python
import mergin

client = mergin.MerginClient(login='john', password='topsecret')
client.download_project('lutraconsulting/Basic survey', '/tmp/basic-survey')
```


## Command-line Tool

When the module is installed, it comes with `mergin` command line tool.

```
$ mergin --help
Usage: mergin [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create         Create a new project on Mergin server
  download       Download last version of mergin project
  list-projects  List projects on the server
  login          Fetch new authentication token.
  modtime        Show files modification time info.
  pull           Fetch changes from Mergin repository
  push           Upload local changes into Mergin repository
  remove         Remove project from server and locally (if exists).
  status         Show all changes in project files - upstream and local
```

To start using `mergin`, first run its "login" command to get authorization token:

```
$ mergin login
```

It will ask for username and password and then output environment variables
with authorization. The returned token is not permanent - it will expire after
several hours. When the variables are set, it is possible to run other commands,
for example, to download a project:

```
$ mergin download username/project1 ~/mergin/project1
```

When a project is downloaded, `mergin` commands can be run in the project's
working directory:

1. get status of the project (check if there are any local/remote changes)
   ```
   $ mergin status
   ```
2. pull changes from Mergin service
   ```
   $ mergin pull
   ```
3. push local changes to Mergin service
   ```
   $ mergin push
   ```


## Development

### How to release 

1. Update version in `setup.py` and `mergin/version.py`
2. Tag git repository with the new version
3. Create package and upload it

```
python3 setup.py sdist
python3 -m twine upload dist/mergin-client-x.y.z.tar.gz
```

### Installing deps

[this section needs revising - now using pip for dependencies]

Python 3.6+ required. Create `mergin/deps` folder where [geodiff](https://github.com/lutraconsulting/geodiff) lib is supposed to be and install dependencies:
    
    rm -r mergin/deps
    mkdir mergin/deps
    pipenv install --dev --three
    pipenv run pip install -r <(pipenv lock -r | grep pygeodiff) --target mergin/deps

For using mergin client with its dependencies packaged locally run:

    pip install wheel 
    python3 setup.py sdist bdist_wheel
    mkdir -p mergin/deps
    pip wheel -r mergin_client.egg-info/requires.txt -w mergin/deps
    unzip mergin/deps/pygeodiff-*.whl -d mergin/deps 

### Tests
For running test do:

    cd mergin
    export TEST_MERGIN_URL=<url> # testing server
    export TEST_API_USERNAME=<username>
    export TEST_API_PASSWORD=<pwd>
    export TEST_API_USERNAME2=<username2>
    export TEST_API_PASSWORD2=<pwd2>
    pipenv run pytest --cov-report html --cov=mergin test/
