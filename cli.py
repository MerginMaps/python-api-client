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
    MerginProject,
    InvalidProject
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


def pretty_summary(summary):
    for k, v in summary.items():
        click.secho("Details " + k)
        click.secho("".join("layer name - " + d["table"] + ": inserted: " + str(d["insert"]) + ", modified: " +
                            str(d["update"]) + ", deleted: " + str(d["delete"]) + "\n" for d in v['geodiff_summary'] if d["table"] != "gpkg_contents"))


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
    if not c.is_server_compatible():
        click.secho(str('This client version is incompatible with server, try to upgrade'), fg='red')
        return
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
@click.option('--parallel/--no-parallel', default=True, help='Download by sending parallel requests')
def download(project, directory, parallel):
    """Download last version of mergin project"""
    c = _init_client()
    directory = directory or os.path.basename(project)
    click.echo('Downloading into {}'.format(directory))
    try:
        c.download_project(project, directory, parallel)
        click.echo('Done')
    except Exception as e:
        click.secho(str(e), fg='red')


def num_version(name):
    return int(name.lstrip("v"))


@cli.command()
def status():
    """Show all changes in project files - upstream and local"""

    try:
        mp = MerginProject(os.getcwd())
    except InvalidProject:
        click.secho('Invalid project directory', fg='red')
        return

    c = _init_client()
    pull_changes, push_changes, push_changes_summary = c.project_status(os.getcwd())
    click.secho("### Server changes:", fg="magenta")
    pretty_diff(pull_changes)
    click.secho("### Local changes:", fg="magenta")
    pretty_diff(push_changes)
    click.secho("### Local changes summary ###")
    pretty_summary(push_changes_summary)


@cli.command()
@click.option('--parallel/--no-parallel', default=True, help='Upload by sending parallel requests')
def push(parallel):
    """Upload local changes into Mergin repository"""

    c = _init_client()
    try:
        c.push_project(os.getcwd(), parallel)
        click.echo('Done')
    except InvalidProject:
        click.echo('Invalid project directory')
    except Exception as e:
        click.secho(str(e), fg='red')


@cli.command()
@click.option('--parallel/--no-parallel', default=True, help='Download by sending parallel requests')
def pull(parallel):
    """Fetch changes from Mergin repository"""

    c = _init_client()
    try:
        c.pull_project(os.getcwd(), parallel)
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
