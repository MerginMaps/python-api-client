# Mergin python client

Repo for [mergin](https://public.cloudmergin.com/) client and basic utils.

Python 3.0+ required.

For using mergin client with its dependencies locally run:

    pip install wheel
    python3 setup.py sdist bdist_wheel
    mkdir -p mergin/deps
    pip wheel -r mergin_client.egg-info/requires.txt -w mergin/deps

To run cli you need to install click:

    pip install click
