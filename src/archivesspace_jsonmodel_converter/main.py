import click
from .logger import get_logger
@click.command()
def main():
    log = get_logger('main')
    log.info("Running main entry point", extra_var="added some stuff")
    print("Hello, world!")
