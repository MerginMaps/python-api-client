#!/usr/bin/env python3

"""
Command line interface for the Mergin client module. When installed with pip, this script
is installed as 'mergin' command line tool (defined in setup.py). If you have installed the module
but the tool is not available, you may need to fix your PATH (e.g. add ~/.local/bin where
pip puts these tools).
"""

from datetime import datetime, timezone
import click
import json
import os
import sys
import time
import traceback

from mergin import (
    ClientError,
    InvalidProject,
    LoginError,
    MerginClient,
    MerginProject,
    __version__,
)
from mergin.client_pull import (
    download_project_async,
    download_project_cancel,
    download_file_async,
    download_file_finalize,
    download_project_finalize,
    download_project_is_running,
)
from mergin.client_pull import pull_project_async, pull_project_is_running, pull_project_finalize, pull_project_cancel
from mergin.client_push import push_project_async, push_project_is_running, push_project_finalize, push_project_cancel

from pygeodiff import GeoDiff


class OptionPasswordIfUser(click.Option):
    """Custom option class for getting a password only if the --username option was specified."""

    def handle_parse_result(self, ctx, opts, args):
        self.has_username = "username" in opts
        return super(OptionPasswordIfUser, self).handle_parse_result(ctx, opts, args)

    def prompt_for_value(self, ctx):
        if self.has_username:
            return super(OptionPasswordIfUser, self).prompt_for_value(ctx)
        return None


def get_changes_count(diff):
    attrs = ["added", "removed", "updated"]
    return sum([len(diff[attr]) for attr in attrs])


def pretty_diff(diff):
    added = diff["added"]
    removed = diff["removed"]
    updated = diff["updated"]
    if removed:
        click.secho("\n>>> Removed:", fg="cyan")
        click.secho("\n".join("- " + f["path"] for f in removed), fg="red")
    if added:
        click.secho("\n>>> Added:", fg="cyan")
        click.secho("\n".join("+ " + f["path"] for f in added), fg="green")
    if updated:
        click.secho("\n>>> Modified:", fg="cyan")
        click.secho("\n".join("M " + f["path"] for f in updated), fg="yellow")


def pretty_summary(summary):
    for k, v in summary.items():
        click.secho("Details " + k)
        click.secho(
            "".join(
                "layer name - "
                + d["table"]
                + ": inserted: "
                + str(d["insert"])
                + ", modified: "
                + str(d["update"])
                + ", deleted: "
                + str(d["delete"])
                + "\n"
                for d in v["geodiff_summary"]
                if d["table"] != "gpkg_contents"
            )
        )


def get_token(url, username, password):
    """Get authorization token for given user and password."""
    mc = MerginClient(url)
    if not mc.is_server_compatible():
        click.secho(str("This client version is incompatible with server, try to upgrade"), fg="red")
        return None
    try:
        session = mc.login(username, password)
    except LoginError as e:
        click.secho("Unable to log in: " + str(e), fg="red")
        return None
    return session["token"]


def get_client(url=None, auth_token=None, username=None, password=None):
    """Return Mergin client."""
    if auth_token is not None:
        try:
            mc = MerginClient(url, auth_token=auth_token)
        except ClientError as e:
            click.secho(str(e), fg="red")
            return None
        # Check if the token has expired or is just about to expire
        delta = mc._auth_session["expire"] - datetime.now(timezone.utc)
        if delta.total_seconds() > 5:
            return mc
    if username and password:
        auth_token = get_token(url, username, password)
        if auth_token is None:
            return None
        mc = MerginClient(url, auth_token=f"Bearer {auth_token}")
    else:
        click.secho(
            "Missing authorization data.\n"
            "Either set environment variables (MERGIN_USERNAME and MERGIN_PASSWORD) "
            "or specify --username / --password options.\n"
            "Note: if --username is specified but password is missing you will be prompted for password.",
            fg="red",
        )
        return None
    return mc


def _print_unhandled_exception():
    """ Outputs details of an unhandled exception that is being handled right now """
    click.secho("Unhandled exception!", fg="red")
    for line in traceback.format_exception(*sys.exc_info()):
        click.echo(line)


@click.group(epilog=f"Copyright (C) 2019-2021 Lutra Consulting\n\n(mergin-py-client v{__version__} / pygeodiff v{GeoDiff().version()})")
@click.option(
    "--url",
    envvar="MERGIN_URL",
    default=MerginClient.default_url(),
    help=f"Mergin server URL. Default is: {MerginClient.default_url()}",
)
@click.option("--auth-token", envvar="MERGIN_AUTH", help="Mergin authentication token string")
@click.option("--username", envvar="MERGIN_USERNAME")
@click.option("--password", cls=OptionPasswordIfUser, prompt=True, hide_input=True, envvar="MERGIN_PASSWORD")
@click.pass_context
def cli(ctx, url, auth_token, username, password):
    """
    Command line interface for the Mergin client module.
    For user authentication on server there are two options:

     1. authorization token environment variable (MERGIN_AUTH) is defined, or
     2. username and password need to be given either as environment variables (MERGIN_USERNAME, MERGIN_PASSWORD),
     or as command options (--username, --password).

    Run `mergin --username <your_user> login` to see how to set the token variable manually.
    """
    mc = get_client(url=url, auth_token=auth_token, username=username, password=password)
    ctx.obj = {"client": mc}


@cli.command()
@click.pass_context
def login(ctx):
    """Login to the service and see how to set the token environment variable."""
    mc = ctx.obj["client"]
    if mc is not None:
        click.secho("Login successful!", fg="green")
        token = mc._auth_session["token"]
        click.secho(f'To set the MERGIN_AUTH variable run:\nexport MERGIN_AUTH="{token}"')


@cli.command()
@click.argument("project")
@click.option("--public", is_flag=True, default=False, help="Public project, visible to everyone")
@click.pass_context
def create(ctx, project, public):
    """Create a new project on Mergin server. `project` needs to be a combination of namespace/project."""
    mc = ctx.obj["client"]
    if mc is None:
        return
    if "/" in project:
        try:
            namespace, project = project.split("/")
            assert namespace, "No namespace given"
            assert project, "No project name given"
        except (ValueError, AssertionError) as e:
            click.secho(f"Incorrect namespace/project format: {e}", fg="red")
            return
    else:
        # namespace not specified, use current user namespace
        namespace = mc.username()
    try:
        mc.create_project(project, is_public=public, namespace=namespace)
        click.echo("Remote project created")
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
        return
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.option(
    "--flag",
    help="What kind of projects (e.g. 'created' for just my projects,"
    "'shared' for projects shared with me. No flag means returns all public projects.",
)
@click.pass_context
def list_projects(ctx, flag):
    """List projects on the server"""
    filter_str = "(filter flag={})".format(flag) if flag is not None else "(all public)"
    click.echo("List of projects {}:".format(filter_str))
    mc = ctx.obj["client"]
    if mc is None:
        return
    resp = mc.paginated_projects_list(flag=flag)
    projects_list = resp["projects"]
    for project in projects_list:
        full_name = "{} / {}".format(project["namespace"], project["name"])
        click.echo(
            "  {:40}\t{:6.1f} MB\t{}".format(full_name, project["disk_usage"] / (1024 * 1024), project["version"])
        )


@cli.command()
@click.argument("project")
@click.argument("directory", type=click.Path(), required=False)
@click.option("--version", default=None, help="Version of project to download")
@click.pass_context
def download(ctx, project, directory, version):
    """Download last version of mergin project"""
    mc = ctx.obj["client"]
    if mc is None:
        return
    directory = directory or os.path.basename(project)
    click.echo("Downloading into {}".format(directory))
    try:
        job = download_project_async(mc, project, directory, version)
        with click.progressbar(length=job.total_size) as bar:
            last_transferred_size = 0
            while download_project_is_running(job):
                time.sleep(1 / 10)  # 100ms
                new_transferred_size = job.transferred_size
                bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                last_transferred_size = new_transferred_size
        download_project_finalize(job)
        click.echo("Done")
    except KeyboardInterrupt:
        click.secho("Cancelling...")
        download_project_cancel(job)
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.argument("filepath")
@click.argument("output")
@click.option("--version", help="Project version tag, for example 'v3'")
@click.pass_context
def download_file(ctx, filepath, output, version):
    """
    Download project file at specified version. `project` needs to be a combination of namespace/project.
    If no version is given, the latest will be fetched.
    """
    mc = ctx.obj["client"]
    if mc is None:
        return
    mp = MerginProject(os.getcwd())
    project_path = mp.metadata["name"]
    try:
        job = download_file_async(mc, project_path, filepath, output, version)
        with click.progressbar(length=job.total_size) as bar:
            last_transferred_size = 0
            while download_project_is_running(job):
                time.sleep(1 / 10)  # 100ms
                new_transferred_size = job.transferred_size
                bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                last_transferred_size = new_transferred_size
        download_file_finalize(job)
        click.echo("Done")
    except KeyboardInterrupt:
        click.secho("Cancelling...")
        download_project_cancel(job)
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
    except Exception as e:
        _print_unhandled_exception()


def num_version(name):
    return int(name.lstrip("v"))


@cli.command()
@click.pass_context
def status(ctx):
    """Show all changes in project files - upstream and local"""
    mc = ctx.obj["client"]
    if mc is None:
        return
    try:
        pull_changes, push_changes, push_changes_summary = mc.project_status(os.getcwd())
    except InvalidProject as e:
        click.secho("Invalid project directory ({})".format(str(e)), fg="red")
        return
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
        return
    except Exception as e:
        _print_unhandled_exception()
        return
    click.secho("### Server changes:", fg="magenta")
    pretty_diff(pull_changes)
    click.secho("### Local changes:", fg="magenta")
    pretty_diff(push_changes)
    click.secho("### Local changes summary ###")
    pretty_summary(push_changes_summary)


@cli.command()
@click.pass_context
def push(ctx):
    """Upload local changes into Mergin repository"""
    mc = ctx.obj["client"]
    if mc is None:
        return
    directory = os.getcwd()
    try:
        job = push_project_async(mc, directory)
        if job is not None:  # if job is none, we don't upload any files, and the transaction is finished already
            with click.progressbar(length=job.total_size) as bar:
                last_transferred_size = 0
                while push_project_is_running(job):
                    time.sleep(1 / 10)  # 100ms
                    new_transferred_size = job.transferred_size
                    bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                    last_transferred_size = new_transferred_size
            push_project_finalize(job)
        click.echo("Done")
    except InvalidProject as e:
        click.secho("Invalid project directory ({})".format(str(e)), fg="red")
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
        return
    except KeyboardInterrupt:
        click.secho("Cancelling...")
        push_project_cancel(job)
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.pass_context
def pull(ctx):
    """Fetch changes from Mergin repository"""
    mc = ctx.obj["client"]
    if mc is None:
        return
    directory = os.getcwd()
    try:
        job = pull_project_async(mc, directory)
        if job is None:
            click.echo("Project is up to date")
            return
        with click.progressbar(length=job.total_size) as bar:
            last_transferred_size = 0
            while pull_project_is_running(job):
                time.sleep(1 / 10)  # 100ms
                new_transferred_size = job.transferred_size
                bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                last_transferred_size = new_transferred_size
        pull_project_finalize(job)
        click.echo("Done")
    except InvalidProject as e:
        click.secho("Invalid project directory ({})".format(str(e)), fg="red")
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
        return
    except KeyboardInterrupt:
        click.secho("Cancelling...")
        pull_project_cancel(job)
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.argument("version")
@click.pass_context
def show_version(ctx, version):
    """ Displays information about a single version of a project. `version` is 'v1', 'v2', etc. """
    mc = ctx.obj["client"]
    if mc is None:
        return
    directory = os.getcwd()
    mp = MerginProject(directory)
    project_path = mp.metadata["name"]
    # TODO: handle exception when version not found
    version_info_dict = mc.project_version_info(project_path, version)[0]
    click.secho("Project: " + version_info_dict["project"]["namespace"] + "/" + version_info_dict["project"]["name"])
    click.secho("Version: " + version_info_dict["name"] + " by " + version_info_dict["author"])
    click.secho("Time:    " + version_info_dict["created"])
    pretty_diff(version_info_dict["changes"])


@cli.command()
@click.argument("path")
@click.pass_context
def show_file_history(ctx, path):
    """ Displays information about a single version of a project """
    mc = ctx.obj["client"]
    if mc is None:
        return
    directory = os.getcwd()
    mp = MerginProject(directory)
    project_path = mp.metadata["name"]
    info_dict = mc.project_file_history_info(project_path, path)
    # TODO: handle exception if history not found
    history_dict = info_dict["history"]
    click.secho("File history: " + info_dict["path"])
    click.secho("-----")
    for version, version_data in history_dict.items():
        diff_info = ""
        if "diff" in version_data:
            diff_info = "diff ({} bytes)".format(version_data["diff"]["size"])
        click.secho(" {:5} {:10} {}".format(version, version_data["change"], diff_info))


@cli.command()
@click.argument("path")
@click.argument("version")
@click.pass_context
def show_file_changeset(ctx, path, version):
    """ Displays information about project changes."""
    mc = ctx.obj["client"]
    if mc is None:
        return
    directory = os.getcwd()
    mp = MerginProject(directory)
    project_path = mp.metadata["name"]
    info_dict = mc.project_file_changeset_info(project_path, path, version)
    # TODO: handle exception if Diff not found
    click.secho(json.dumps(info_dict, indent=2))


@cli.command()
@click.argument("source_project_path", required=True)
@click.argument("cloned_project_name", required=True)
@click.argument("cloned_project_namespace", required=False)
@click.pass_context
def clone(ctx, source_project_path, cloned_project_name, cloned_project_namespace=None):
    """Clone project from server."""
    mc = ctx.obj["client"]
    if mc is None:
        return
    try:
        mc.clone_project(source_project_path, cloned_project_name, cloned_project_namespace)
        click.echo("Done")
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.argument("project", required=True)
@click.pass_context
def remove(ctx, project):
    """Remove project from server."""
    mc = ctx.obj["client"]
    if mc is None:
        return
    if "/" in project:
        try:
            namespace, project = project.split("/")
            assert namespace, "No namespace given"
            assert project, "No project name given"
        except (ValueError, AssertionError) as e:
            click.secho(f"Incorrect namespace/project format: {e}", fg="red")
            return
    else:
        # namespace not specified, use current user namespace
        namespace = mc.username()
    try:
        mc.delete_project(f"{namespace}/{project}")
        click.echo("Remote project removed")
    except ClientError as e:
        click.secho("Error: " + str(e), fg="red")
    except Exception as e:
        _print_unhandled_exception()


if __name__ == "__main__":
    cli()
