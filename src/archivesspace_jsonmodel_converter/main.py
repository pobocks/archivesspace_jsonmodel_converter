import click
from .logger import get_logger
from .configurator import AJCConfig

CONFIG = None

@click.group()
@click.option('--config-file')
def main(config_file):
    global CONFIG
    if config_file:
        CONFIG = AJCConfig(config_file)
    else:
        CONFIG = AJCConfig()

@main.command()
def create_subjects():
    log = get_logger('main.subjects')
    log.info("Subject creation goes here")
    print("Create some subjects already!")
