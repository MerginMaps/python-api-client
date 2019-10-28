# Mergin python client

Repo for [mergin](https://public.cloudmergin.com/) client and basic utils.

## Development
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

## Tests
For running test do:

    cd mergin
    export TEST_MERGIN_URL=<url> # testing server
    export TEST_API_USERNAME=<username>
    export TEST_API_PASSWORD=<pwd>
    pipenv run pytest --cov-report html --cov=mergin test/


## CLI
There is command line tool based on [click](https://click.palletsprojects.com/) included, to run it make sure you have click installed:

    pip install click

You can use CLI like this:

    chmod +x cli.py
    sudo ln -s `pwd`/cli.py /usr/bin/mergin
    mergin login <url>