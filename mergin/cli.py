#!/usr/bin/env python3

"""
Command line interface for the Mergin client module. When installed with pip, this script
is installed as 'mergin' command line tool (defined in setup.py). If you have installed the module
but the tool is not available, you may need to fix your PATH (e.g. add ~/.local/bin where
pip puts these tools).
"""

import json
import os
import sys
import traceback
import click

from mergin import (
    ClientError,
    MerginClient,
    MerginProject,
    InvalidProject,
    LoginError,
)
from mergin.client_pull import download_project_async, download_project_is_running, download_project_finalize, download_project_cancel
from mergin.client_pull import pull_project_async, pull_project_is_running, pull_project_finalize, pull_project_cancel
from mergin.client_push import push_project_async, push_project_is_running, push_project_finalize, push_project_cancel


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
        click.secho("".join("layer name - " + d["table"] + ": inserted: " + str(d["insert"]) + ", modified: " +
                            str(d["update"]) + ", deleted: " + str(d["delete"]) + "\n" for d in v['geodiff_summary'] if d["table"] != "gpkg_contents"))


def _init_client():
    url = os.environ.get('MERGIN_URL')
    auth_token = os.environ.get('MERGIN_AUTH')
    if auth_token is None:
        click.secho("Missing authorization token: please run 'login' first and set MERGIN_AUTH env variable", fg='red')
        return None

    mc = MerginClient(url, auth_token='Bearer {}'.format(auth_token))

    # check whether the token has not expired already (normally it expires in 12 hours)
    from datetime import datetime, timezone
    delta = mc._auth_session['expire'] - datetime.now(timezone.utc)
    if delta.total_seconds() < 0:
        click.secho("Access token has expired: please run 'login' again and set MERGIN_AUTH env variable", fg='red')
        return None

    return mc


def _print_unhandled_exception():
    """ Outputs details of an unhandled exception that is being handled right now """
    click.secho("Unhandled exception!", fg='red')
    for line in traceback.format_exception(*sys.exc_info()):
        click.echo(line)


@click.group()
def cli():
    pass


@cli.command()
@click.argument('url', required=False)
@click.option('--login', prompt=True)
@click.option('--password', prompt=True, hide_input=True)
def login(url, login, password):
    """Fetch new authentication token. If no URL is specified, the public Mergin instance will be used."""
    c = MerginClient(url)
    click.echo("Mergin URL: " + c.url)
    if not c.is_server_compatible():
        click.secho(str('This client version is incompatible with server, try to upgrade'), fg='red')
        return
    try:
        session = c.login(login, password)
    except LoginError as e:
        click.secho('Unable to log in: ' + str(e), fg='red')
        return

    print('export MERGIN_URL="%s"' % c.url)
    print('export MERGIN_AUTH="%s"' % session['token'])


@cli.command()
@click.argument('project')
@click.option('--public', is_flag=True, default=False, help='Public project, visible to everyone')
def create(project, public):
    """Create a new project on Mergin server"""

    c = _init_client()
    if c is None:
        return

    try:
        c.create_project(project, is_public=public)
        click.echo('Done')
    except ClientError as e:
        click.secho('Error: ' + str(e), fg='red')
        return
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.option('--flag', help="What kind of projects (e.g. 'created' for just my projects,"
              "'shared' for projects shared with me. No flag means returns all public projects.")
def list_projects(flag):
    """List projects on the server"""
    filter_str = "(filter flag={})".format(flag) if flag is not None else "(all public)"
    click.echo('List of projects {}:'.format(filter_str))
    c = _init_client()
    if c is None:
        return
    projects_list = c.projects_list(flag=flag)
    for project in projects_list:
        full_name = "{} / {}".format(project["namespace"], project["name"])
        click.echo("  {:40}\t{:6.1f} MB\t{}".format(full_name, project["disk_usage"]/(1024*1024), project['version']))


@cli.command()
@click.argument('project')
@click.argument('directory', type=click.Path(), required=False)
def download(project, directory):
    """Download last version of mergin project"""
    
    c = _init_client()
    if c is None:
        return
    directory = directory or os.path.basename(project)

    click.echo('Downloading into {}'.format(directory))
    try:
        job = download_project_async(c, project, directory)

        import time
        with click.progressbar(length=job.total_size) as bar:
            last_transferred_size = 0
            while download_project_is_running(job):
                time.sleep(1/10)  # 100ms
                new_transferred_size = job.transferred_size
                bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                last_transferred_size = new_transferred_size

        download_project_finalize(job)

        click.echo('Done')
    except KeyboardInterrupt:
        print("Cancelling...")
        download_project_cancel(job)
    except ClientError as e:
        click.secho("Error: " + str(e), fg='red')
    except Exception as e:
        _print_unhandled_exception()


def num_version(name):
    return int(name.lstrip("v"))


@cli.command()
def status():
    """Show all changes in project files - upstream and local"""

    c = _init_client()
    if c is None:
        return

    try:
        pull_changes, push_changes, push_changes_summary = c.project_status(os.getcwd())
    except InvalidProject as e:
        click.secho('Invalid project directory ({})'.format(str(e)), fg='red')
        return
    except ClientError as e:
        click.secho('Error: ' + str(e), fg='red')
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
def push():
    """Upload local changes into Mergin repository"""

    c = _init_client()
    if c is None:
        return
    directory = os.getcwd()

    try:
        job = push_project_async(c, directory)

        if job is not None:   # if job is none, we don't upload any files, and the transaction is finished already
            import time
            with click.progressbar(length=job.total_size) as bar:
                last_transferred_size = 0
                while push_project_is_running(job):
                    time.sleep(1/10)  # 100ms
                    new_transferred_size = job.transferred_size
                    bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                    last_transferred_size = new_transferred_size

            push_project_finalize(job)

        click.echo('Done')
    except InvalidProject as e:
        click.secho('Invalid project directory ({})'.format(str(e)), fg='red')
    except ClientError as e:
        click.secho('Error: ' + str(e), fg='red')
        return
    except KeyboardInterrupt:
        print("Cancelling...")
        push_project_cancel(job)
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
def pull():
    """Fetch changes from Mergin repository"""

    c = _init_client()
    if c is None:
        return
    directory = os.getcwd()

    try:
        job = pull_project_async(c, directory)

        if job is None:
            click.echo('Project is up to date')
            return

        import time
        with click.progressbar(length=job.total_size) as bar:
            last_transferred_size = 0
            while pull_project_is_running(job):
                time.sleep(1/10)  # 100ms
                new_transferred_size = job.transferred_size
                bar.update(new_transferred_size - last_transferred_size)  # the update() needs increment only
                last_transferred_size = new_transferred_size

        pull_project_finalize(job)

        click.echo('Done')
    except InvalidProject as e:
        click.secho('Invalid project directory ({})'.format(str(e)), fg='red')
    except ClientError as e:
        click.secho('Error: ' + str(e), fg='red')
        return
    except KeyboardInterrupt:
        print("Cancelling...")
        pull_project_cancel(job)
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.argument('version')
def show_version(version):
    """ Displays information about a single version of a project """

    c = _init_client()
    if c is None:
        return
    directory = os.getcwd()

    mp = MerginProject(directory)
    project_path = mp.metadata["name"]

    version_info_dict = c.project_version_info(project_path, version)[0]
    print("Project: " + version_info_dict['project']['namespace'] + "/" + version_info_dict['project']['name'])
    print("Version: " + version_info_dict['name'] + " by " + version_info_dict['author'])
    print("Time:    " + version_info_dict['created'])
    pretty_diff(version_info_dict['changes'])


@cli.command()
@click.argument('path')
def show_file_history(path):
    """ Displays information about a single version of a project """

    c = _init_client()
    if c is None:
        return
    directory = os.getcwd()

    mp = MerginProject(directory)
    project_path = mp.metadata["name"]

    info_dict = c.project_file_history_info(project_path, path)
    history_dict = info_dict['history']

    print("File history: " + info_dict['path'])
    print("-----")
    for version, version_data in history_dict.items():
        diff_info = ''
        if 'diff' in version_data:
            diff_info = "diff ({} bytes)".format(version_data['diff']['size'])
        print(" {:5} {:10} {}".format(version, version_data['change'], diff_info))


@cli.command()
@click.argument('path')
@click.argument('version')
def show_file_changeset(path, version):
    """ Displays information about a single version of a project """

    c = _init_client()
    if c is None:
        return
    directory = os.getcwd()

    mp = MerginProject(directory)
    project_path = mp.metadata["name"]

    info_dict = c.project_file_changeset_info(project_path, path, version)
    print(json.dumps(info_dict, indent=2))


@cli.command()
@click.argument('directory', required=False)
def modtime(directory):
    """
    Show files modification time info. For debug purposes only.
    """
    from datetime import datetime
    directory = os.path.abspath(directory or os.getcwd())
    strip_len = len(directory) + 1
    for root, dirs, files in os.walk(directory, topdown=True):
        for entry in dirs + files:
            abs_path = os.path.abspath(os.path.join(root, entry))

            click.echo(abs_path[strip_len:])
            # click.secho('atime %s' % datetime.fromtimestamp(os.path.getatime(abs_path)), fg="cyan")
            click.secho('mtime %s' % datetime.fromtimestamp(os.path.getmtime(abs_path)), fg="cyan")
            # click.secho('ctime %s' % datetime.fromtimestamp(os.path.getctime(abs_path)), fg="cyan")
            click.echo()


@cli.command()
@click.argument('source_project_path', required=True)
@click.argument('cloned_project_name', required=True)
@click.argument('cloned_project_namespace', required=False)
def clone(source_project_path, cloned_project_name, cloned_project_namespace=None):
    """Clone project from server."""
    c = _init_client()
    if c is None:
        return
    try:
        c.clone_project(source_project_path, cloned_project_name, cloned_project_namespace)
        click.echo('Done')
    except Exception as e:
        _print_unhandled_exception()


@cli.command()
@click.argument('project', required=False)
def remove(project):
    """Remove project from server and locally (if exists)."""
    local_info = None
    if not project:
        from mergin.client import inspect_project
        try:
            local_info = inspect_project(os.path.join(os.getcwd()))
            project = local_info['name']
        except InvalidProject as e:
            click.secho('Invalid project directory ({})'.format(str(e)), fg='red')
            return

    c = _init_client()
    if c is None:
        return
    try:
        c.delete_project(project)
        if local_info:
            import shutil
            shutil.rmtree(os.path.join(os.getcwd()))
        click.echo('Done')
    except Exception as e:
        _print_unhandled_exception()


if __name__ == '__main__':
    cli()
