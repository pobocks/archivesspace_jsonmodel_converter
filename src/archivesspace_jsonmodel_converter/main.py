import click
from .logger import get_logger
from .configurator import AJCConfig
from .subjects import subjects_create

CONFIG = None

def config(config_file):
    global CONFIG
    if config_file:
        CONFIG = AJCConfig(config_file)
    else:
        CONFIG = AJCConfig()

@click.group()
@click.option('--config-file', help="Path to yaml configuration file")
def main(config_file):
    config(config_file)

@main.command()
def create_subjects():
    log = get_logger('main.subjects')
    log.info("Subject creation goes here")
    print("Create some subjects already!")
    subjects_create(CONFIG)
