import click
from .logger import setup_logging, get_logger
from .configurator import AJCConfig
from .subjects import subjects_create
from .resources import resources_create
from .enumerations import convert_enums
from .name_xwalk import crosswalk_names
from .crosswalker import crosswalk_export


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
def create_name_crosswalk():
    log = get_logger('main.xwalk_names')
    log.info("Name crosswalk")
    CONFIG.dynamic_configuration()
    crosswalk_names(CONFIG, log)
    
@click.command()
@click.option('--csv-file', help="Path to an output csv file")
@click.option('--xw-table', help="Name of a Crosswalk table")
def export_crosswalk_table(csv_file, xw_table):
    log = get_logger('main.export_crosswalk_table')
    log.info(f"Exporting {xw_table} to {csv_file}")
    CONFIG.dynamic_configuration()
    crosswalk_export(CONFIG, log, csv_file, xw_table)
    
@main.add_command(export_crosswalk_table)

