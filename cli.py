#!/usr/bin/env python3

"""
Install:

chmod +x cli.py
sudo ln -s `pwd`/cli.py /usr/bin/mergin

"""

import os
import click

from mergin import (
    MerginClient,
    InvalidProject,
    list_project_directory,
    project_changes,
    inspect_project
)

# TODO: use config file (configparser or json)
url = "http://localhost:5000"


def get_changes_count(diff):
    attrs = ["added", "removed", "updated", "renamed"]
    return sum([len(diff[attr]) for attr in attrs])

def pretty_diff(diff):
    added = diff["added"]
    removed = diff["removed"]
    updated = diff["updated"]
    renamed = diff["renamed"]

    if renamed:
        click.secho("\n>>> Renamed:", fg="cyan")
        for f in renamed:
            click.echo(" ".join([f["path"], "->", f["new_path"]]))

    if removed:
        click.secho("\n>>> Removed:", fg="cyan")
        click.secho("\n".join("- " + f["path"] for f in removed), fg="red")

    if added:
        click.secho("\n>>> Added:", fg="cyan")
        click.secho("\n".join("+ " + f["path"] for f in added), fg="green")

    if updated:
        click.secho("\n>>> Modified:", fg="cyan")
        click.secho("\n".join("M " + f["path"] for f in updated), fg="yellow")


def _init_client(url, auth):
    if auth:
        username, password = auth.split(":", 1)
        return MerginClient(url, username, password)

    return MerginClient(url)


@click.group()
def cli():
    pass

@cli.command()
# @click.argument('url')
@click.argument('directory', type=click.Path(exists=True))
@click.option('--public', is_flag=True, default=False, help='Public project, visible to everyone')
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def init(directory, public, auth):
    """Initialize new project from existing DIRECTORY name"""

    directory = os.path.abspath(directory)
    project_name = os.path.basename(directory)
    c = _init_client(url, auth)

    try:
        c.create_project(project_name, directory, is_public=public)
        click.echo('Done')
    except Exception as e:
        click.secho(str(e), fg='red')

@cli.command()
# @click.argument('url')
@click.argument('project')
@click.argument('directory', type=click.Path(), required=False)
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def download(project, directory, auth):
    """Download last version of mergin project"""

    c = _init_client(url, auth)
    directory = directory or project
    click.echo('Downloading into {}'.format(directory))
    try:
        c.download_project(project, directory)
        click.echo('Done')
    except Exception as e:
        click.secho(str(e), fg='red')


def num_version(name):
    return int(name.lstrip("v"))

@cli.command()
# @click.argument('url')
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def status(auth):
    """Show all changes in project files - upstream and local"""

    try:
        project_info = inspect_project(os.getcwd())
    except InvalidProject:
        click.secho('Invalid project directory', fg='red')
        return

    project_name = project_info["name"]
    c = _init_client(url, auth)

    local_version = num_version(project_info["version"])
    local_files = list_project_directory(os.getcwd())

    try:
        versions = c.project_versions(project_name)
    except Exception as e:
        click.secho(str(e), fg='red')
        return

    click.echo("Current version: {}".format(project_info["version"]))
    last_version = versions[-1]
    new_versions = [v for v in versions if num_version(v["name"]) > local_version]
    if new_versions:
        click.secho("### Available updates: {}".format(len(new_versions)), fg="magenta")

        # TODO: insufficient API, files could be included in versions,
        # or we should be able to request project_info at specific version
        server_files = c.project_info(project_name)["files"]

        # changes between current files and last version on server
        # changes = project_changes(local_files, server_files)

        # changes between versions on server
        changes = project_changes(project_info["files"], server_files)

        click.echo()
        click.secho("### Changes:", fg="magenta")
        pretty_diff(changes)
        click.echo()

    changes = project_changes(project_info["files"], local_files)
    changes_count = get_changes_count(changes)
    if changes_count:
        click.secho("### Local changes: {}".format(changes_count), fg="magenta")
        pretty_diff(changes)
    else:
        click.secho("No local changes!", fg="magenta")
    # TODO: show conflicts

@cli.command()
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def push(auth):
    """Upload local changes into Mergin repository"""

    c = _init_client(url, auth)
    try:
        c.push_project(os.getcwd())
        click.echo('Done')
    except InvalidProject:
        click.echo('Invalid project directory')
    except Exception as e:
        click.secho(str(e), fg='red')

@cli.command()
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def pull(auth):
    """Fetch changes from Mergin repository"""

    c = _init_client(url, auth)
    try:
        c.pull_project(os.getcwd())
        click.echo('Done')
    except InvalidProject:
        click.secho('Invalid project directory', fg='red')

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


if __name__ == '__main__':
    cli()
