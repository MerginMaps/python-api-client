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
)
from mergin.client_pull import (
    download_project_async,
    download_project_cancel,
    download_project_finalize,
    download_project_is_running,
)
from mergin.client_pull import pull_project_async, pull_project_is_running, pull_project_finalize, pull_project_cancel
from mergin.client_push import push_project_async, push_project_is_running, push_project_finalize, push_project_cancel


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


def get_token(url, username, password, show_token=False):
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
    if show_token:
        click.secho(f'export MERGIN_AUTH="{session["token"]}"')
    else:
        click.secho(f"auth_token created (use --show_token option to see the token).")
    return session["token"]


def get_client(url=None, auth_token=None, username=None, password=None, show_token=False):
    """Return Mergin client."""
    if auth_token is not None:
        mc = MerginClient(url, auth_token=f"Bearer {auth_token}")
        # Check if the token has expired or is just about to expire
        delta = mc._auth_session["expire"] - datetime.now(timezone.utc)
        if delta.total_seconds() > 5:
            if show_token:
                click.secho(f'export MERGIN_AUTH="{auth_token}"')
            return mc
    if username and password:
        auth_token = get_token(url, username, password, show_token=show_token)
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


@click.group()
@click.option(
    "--url",
    envvar="MERGIN_URL",
    default="https://public.cloudmergin.com",
    help="Mergin server URL. Default is: https://public.cloudmergin.com",
)
@click.option("--auth_token", envvar="MERGIN_AUTH", help="Mergin authentication token string")
@click.option("--username", envvar="MERGIN_USERNAME")
@click.option("--password", cls=OptionPasswordIfUser, prompt=True, hide_input=True, envvar="MERGIN_PASSWORD")
@click.option(
    "--show_token",
    is_flag=True,
    help="Flag for echoing the authentication token. Useful for setting the MERGIN_AUTH environment variable.",
)
@click.pass_context
def cli(ctx, url, auth_token, username, password, show_token):
    """
    Command line interface for the Mergin client module.
    For user authentication on server, username and password need to be given either as environment variables
    (MERGIN_USERNAME, MERGIN_PASSWORD), or as options (--username, --password).
    To set MERGIN_AUTH variable, run the command with --show_token option to see the token and how to set it manually.
    """
    mc = get_client(url=url, auth_token=auth_token, username=username, password=password, show_token=show_token)
    ctx.obj = {"client": mc}


@cli.command()
@click.argument("project")
@click.option("--public", is_flag=True, default=False, help="Public project, visible to everyone")
@click.option("--namespace", help="Namespace for the new project. Default is current Mergin user namespace.")
@click.pass_context
def create(ctx, project, public, namespace):
    """
    Create a new project on Mergin server.
    `project` can be a combination of namespace/project. Namespace can also be specified as an option, however if
    namespace is specified as a part of `project` argument, the option is ignored.
    """
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
@click.argument("directory", required=False)
def modtime(directory):
    """
    Show files modification time info. For debug purposes only.
    """
    directory = os.path.abspath(directory or os.getcwd())
    strip_len = len(directory) + 1
    for root, dirs, files in os.walk(directory, topdown=True):
        for entry in dirs + files:
            abs_path = os.path.abspath(os.path.join(root, entry))
            click.echo(abs_path[strip_len:])
            # click.secho('atime %s' % datetime.fromtimestamp(os.path.getatime(abs_path)), fg="cyan")
            click.secho("mtime %s" % datetime.fromtimestamp(os.path.getmtime(abs_path)), fg="cyan")
            # click.secho('ctime %s' % datetime.fromtimestamp(os.path.getctime(abs_path)), fg="cyan")
            click.echo()


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
    try:
        mc.delete_project(project)
        click.echo("Remote project removed")
    except Exception as e:
        _print_unhandled_exception()


if __name__ == "__main__":
    cli()
