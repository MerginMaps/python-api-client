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

# TODO: use config file
url = 'http://localhost:5000'


def pretty_diff(diff):
    added = diff["added"]
    removed = diff["removed"]
    updated = diff["updated"]
    renamed = diff["renamed"]
    total_changes = sum([len(l) for l in (added, removed, updated, renamed)])
    if total_changes == 0:
        print("No local changes!")
        return

    print("Local changes:", total_changes)

    if renamed:
        print("\n>>> Renamed:")
        for f in renamed:
            print(f["path"], "->", f["new_path"])

    if removed:
        print("\n>>> Removed:")
        print("\n".join("- " + f["path"] for f in removed))

    if added:
        print("\n>>> Added:")
        print("\n".join("+ " + f["path"] for f in added))

    if updated:
        print("\n>>> Modified:")
        print("\n".join("M " + f["path"] for f in updated))


def _init_client(url, auth):
    if auth:
        username, password = auth.split(':', 1)
        return MerginClient(url, username, password)

    return MerginClient(url)


@click.group()
def cli():
    pass

@cli.command()
# @click.argument('url')
@click.argument('project', type=click.Path(exists=True))
@click.option('--public', default=False, help='Public project, visible to everyone')
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def init(project, public, auth):
    """Initialize new project from existing PROJECT directory name"""

    c = _init_client(url, auth)
    c.create_project(project, project, is_public=False)
    click.echo('Done')


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
    c.download_project(project, directory)
    click.echo('Done')


def num_version(name):
    return int(name.lstrip("v"))

@cli.command()
# @click.argument('url')
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def status(auth):
    """Show local changes in project files"""

    try:
        project_info = inspect_project(os.getcwd())
    except InvalidProject:
        click.echo('Invalid project directory')
        return

    project_name = project_info["name"]
    c = _init_client(url, auth)

    local_version = num_version(project_info["version"])
    versions = c.project_versions(project_name)
    last_version = versions[-1]

    new_versions = [v for v in versions if num_version(v["name"]) > local_version]
    if new_versions:
        print("Updates from server:", len(new_versions))
        print()

    server_files = c.project_info(project_name)["files"]
    local_files = list_project_directory(os.getcwd())
    changes = project_changes(server_files, local_files)
    pretty_diff(changes)
    # TODO: show conflicts

@cli.command()
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def push(auth):
    """Upload local changes into Mergin repository"""

    c = _init_client(url, auth)
    try:
        c.push_project(os.getcwd())
    except InvalidProject:
        click.echo('Invalid project directory')
    except Exception as e:
        click.echo(str(e))

@cli.command()
@click.option('--auth', '-a', help='Authentication in form <username>:<password>')
def pull(auth):
    """Fetch changes from Mergin repository"""

    c = _init_client(url, auth)
    try:
        c.pull_project(os.getcwd())
    except InvalidProject:
        click.echo('Invalid project directory')

@cli.command()
@click.argument('directory', required=False)
def modtime(directory):
    """
    Show files modification time info. For debug purposes only.
    """
    from datetime import datetime
    directory = directory or os.getcwd()
    for root, dirs, files in os.walk(directory):
        for entry in dirs + files:
            abs_path = os.path.abspath(os.path.join(root, entry))
            print(entry)
            print('\tatime', datetime.fromtimestamp(os.path.getatime(abs_path)))
            print('\tmtime', datetime.fromtimestamp(os.path.getmtime(abs_path)))
            print('\tctime', datetime.fromtimestamp(os.path.getctime(abs_path)))


if __name__ == '__main__':
    cli()
