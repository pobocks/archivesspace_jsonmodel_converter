import click
from .logger import setup_logging, get_logger
from .configurator import AJCConfig
from .subjects import subjects_create
from .resources import resources_create
from .archival_objects import archival_objects_create
from .enumerations import convert_enums
from .name_xwalk import crosswalk_names


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

@main.command()
def create_resources():
    log = get_logger('main.resources')
    log.info("Resource creation goes here")
    print("Create some resources!")

    CONFIG.dynamic_configuration()
    resources_create(CONFIG, log)

@main.command()
def create_archival_objects():
    log = get_logger('main.archival_objects')
    log.info("Archival Object creation goes here")
    print("Create some archival_objects!")

    CONFIG.dynamic_configuration()
    archival_objects_create(CONFIG, log)

@main.command()
def create_name_crosswalk():
    log = get_logger('main.xwalk_names')
    log.info("Name crosswalk")
    CONFIG.dynamic_configuration()
    crosswalk_names(CONFIG, log)

@main.command()
def whole_shebang():
    log = get_logger('main.whole_shebang')
    log.info("Do all the things! In the right order!")
    CONFIG.dynamic_configuration()
    subjects_create(CONFIG, log)
    convert_enums(CONFIG, log)
    crosswalk_names(CONFIG, log)
    resources_create(CONFIG, log)
    archival_objects_create(CONFIG, log)
