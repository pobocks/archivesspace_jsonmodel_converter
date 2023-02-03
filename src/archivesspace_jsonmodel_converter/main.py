import click
from .logger import setup_logging, get_logger
from .configurator import AJCConfig
from .subjects import subjects_create
from .enumerations import convert_enums

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
    setup_logging(**CONFIG['logging_config'])

@main.command()
def create_subjects():
    log = get_logger('main.subjects')
    log.info("Subject creation goes here")
    print("Create some subjects already!")

    CONFIG.dynamic_configuration()
    subjects_create(CONFIG, log)

@main.command()
def enum_conversion():
    log = get_logger('main.enums')
    log.info("Enum conversion")
    CONFIG.dynamic_configuration()
    convert_enums(CONFIG, log)
