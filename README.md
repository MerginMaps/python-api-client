# Mergin python client

Repo for mergin client and basic utils.

For using mergin client with its dependencies locally run:

    python3 setup.py sdist bdist_wheel
    mkdir -p mergin/deps
    pip wheel -r mergin_client.egg-info/requires.txt -w mergin/deps