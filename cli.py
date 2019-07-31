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


def _init_client():
    url = os.environ.get('MERGIN_URL')
    auth_token = os.environ.get('MERGIN_AUTH')
    return MerginClient(url, auth_token='Bearer {}'.format(auth_token))


@click.group()
def cli():
    pass


@cli.command()
@click.argument('url')
@click.option('--login', prompt=True)
@click.option('--password', prompt=True, hide_input=True)
def login(url, login, password):
    """Fetch new authentication token"""
    c = MerginClient(url)
    session = c.login(login, password)
    print('export MERGIN_URL="%s"' % url)
    print('export MERGIN_AUTH="%s"' % session['token'])
    print('export MERGIN_AUTH_HEADER="Authorization: %s"' % session['token'])


@cli.command()
@click.argument('project')
@click.argument('directory', type=click.Path(exists=True), required=False)
@click.option('--public', is_flag=True, default=False, help='Public project, visible to everyone')
def init(project, directory, public):
    """Initialize new project from existing DIRECTORY name"""

    if directory:
        directory = os.path.abspath(directory)
    c = _init_client()

    try:
        c.create_project(project, directory, is_public=public)
        click.echo('Done')
    except Exception as e:
        click.secho(str(e), fg='red')


@cli.command()
@click.argument('project')
@click.argument('directory', type=click.Path(), required=False)
def download(project, directory):
    """Download last version of mergin project"""

    c = _init_client()
    directory = directory or os.path.basename(project)
    click.echo('Downloading into {}'.format(directory))
    try:
        c.download_project(project, directory)
        click.echo('Done')
    except Exception as e:
        click.secho(str(e), fg='red')


def num_version(name):
    return int(name.lstrip("v"))

@cli.command()
def status():
    """Show all changes in project files - upstream and local"""

    try:
        project_info = inspect_project(os.getcwd())
    except InvalidProject:
        click.secho('Invalid project directory', fg='red')
        return

    project_name = project_info["name"]
    c = _init_client()

    local_version = num_version(project_info["version"])
    local_files = list_project_directory(os.getcwd())

    try:
        versions = c.project_versions(project_name)
    except Exception as e:
        click.secho(str(e), fg='red')
        return

    click.echo("Current version: {}".format(project_info["version"]))
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
def push():
    """Upload local changes into Mergin repository"""

    c = _init_client()
    try:
        c.push_project(os.getcwd())
        click.echo('Done')
    except InvalidProject:
        click.echo('Invalid project directory')
    except Exception as e:
        click.secho(str(e), fg='red')


@cli.command()
def pull():
    """Fetch changes from Mergin repository"""

    c = _init_client()
    try:
        c.pull_project(os.getcwd())
        click.echo('Done')
    except InvalidProject:
        click.secho('Invalid project directory', fg='red')


@cli.command()
def version():
    """Check and display server version"""
    c = _init_client()
    serv_ver = c.server_version()
    ok = c.check_version()
    click.echo("Server version: %s" % serv_ver)
    if not ok:
        click.secho("Server doesn't meet the minimum required version: %s" % c.min_server_version, fg='yellow')


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
@click.argument('project', required=False)
def remove(project):
    """Remove project from server and locally (if exists)."""
    local_info = None
    if not project:
        from mergin.client import inspect_project
        try:
            local_info = inspect_project(os.path.join(os.getcwd()))
            project = local_info['name']
        except InvalidProject:
            click.secho('Invalid project directory', fg='red')
            return

    c = _init_client()
    try:
        c.delete_project(project)
        if local_info:
            import shutil
            shutil.rmtree(os.path.join(os.getcwd()))
        click.echo('Done')
    except Exception as e:
        click.secho(str(e), fg='red')


if __name__ == '__main__':
    cli()
