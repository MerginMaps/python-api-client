"""
Simple tests for cli, needs more work in future.
"""

import pytest
from click.testing import CliRunner

from ..cli import cli
from ..version import __version__

# nothing listens on port 9 (discard), so any login attempt fails immediately without network access
OFFLINE_URL = ["--url", "http://127.0.0.1:9"]


@pytest.fixture(autouse=True)
def no_mergin_env(monkeypatch):
    """Make sure credentials from the environment do not affect the tests."""
    for var in ("MERGIN_URL", "MERGIN_AUTH", "MERGIN_USERNAME", "MERGIN_PASSWORD"):
        monkeypatch.delenv(var, raising=False)


def test_help():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Commands:" in result.output
    assert f"python-api-client v{__version__}" in result.output


@pytest.mark.parametrize("command", sorted(cli.commands))
def test_command_help(command):
    result = CliRunner().invoke(cli, [command, "--help"])
    assert result.exit_code == 0, result.output
    assert f"Usage: cli {command}" in result.output


def test_password_prompt_with_username():
    result = CliRunner().invoke(cli, OFFLINE_URL + ["--username", "john", "list-projects", "ws"], input="secret\n")
    assert "Password:" in result.output
    assert "Unable to log in" in result.output


def test_no_password_prompt_with_username_and_password():
    result = CliRunner().invoke(
        cli, OFFLINE_URL + ["--username", "john", "--password", "secret", "list-projects", "ws"]
    )
    assert "Password:" not in result.output
    assert "Unable to log in" in result.output


def test_missing_credentials():
    result = CliRunner().invoke(cli, OFFLINE_URL + ["list-projects", "ws"])
    assert "Password:" not in result.output
    assert "Missing authorization data" in result.output
