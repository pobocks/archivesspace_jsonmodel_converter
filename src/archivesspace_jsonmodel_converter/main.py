import click
from .logger import get_logger

@click.group()
def main():
    pass

@main.command()
def create_subjects():
    log = get_logger('main.subjects')
    log.info("Subject creation goes here")
    print("Create some subjects already!")
