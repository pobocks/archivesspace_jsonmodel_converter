import click
from .logger import setup_logging, get_logger
from .configurator import AJCConfig
from .subjects import subjects_create
from .resources import resources_create
from .archival_objects import archival_objects_create, produce_excel_template
from .enumerations import convert_enums
from .name_xwalk import crosswalk_names
from .crosswalker import crosswalk_export
from .crosswalker import crosswalk_list_tables
from .crosswalker import crosswalk_reinitialize
from .crosswalker import crosswalk_delete_table

from .agents import agents_create


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
@click.option('--null-itemname-only', '-n', is_flag=True, default=False, help="Omit records with itemName defined")
@click.option('--batch-size', '-b', default=None, type=int, help="batch into different files with INTEGER records each")
@click.option('--output', '-o', prompt="Filename for excel?", type=click.Path(exists=False), help="filename to use for output, multiple batches will have an integer appended")
def make_ao_template(null_itemname_only, batch_size, output):
    log = get_logger('main.make_ao_template')
    log.info('Make AO Excel template')
    CONFIG.dynamic_configuration()
    produce_excel_template(CONFIG, null_itemname_only, batch_size, output, log)

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
    


@main.command()
def list_crosswalk_tables():
    log = get_logger('main.list_xwalk_tables')
    log.info("List Crosswalk Tables")
    CONFIG.dynamic_configuration()
    crosswalk_list_tables(CONFIG, log)
    
@main.command()
def reinitialize_crosswalk():
    log = get_logger('main.xwalk_reinit')
    CONFIG.dynamic_configuration()
    crosswalk_reinitialize(CONFIG, log)
    

@click.command()
@click.option('--csv-file', help="Path to an output csv file")
@click.option('--xw-table', help="Name of a Crosswalk table")
def export_crosswalk_table(csv_file, xw_table):
    log = get_logger('main.export_crosswalk_table')
    log.info(f"Exporting {xw_table} to {csv_file}")
    CONFIG.dynamic_configuration()
    crosswalk_export(CONFIG, log, csv_file, xw_table)

main.add_command(export_crosswalk_table)
@click.command()
@click.option('--really-create', is_flag=True, show_default=True, default=False, help="Actually create Aspace records")
def create_agents(really_create): 
    report_only = not really_create
    log = get_logger('main.agents')
    log.info("Create Agents")
    CONFIG.dynamic_configuration()
    agents_create(CONFIG, log, report_only)
    
main.add_command(create_agents)

@click.command()
@click.option('--xw-table', help="Name of a Crosswalk table")
def delete_crosswalk_table(xw_table):
    log = get_logger('main.delete_crosswalk_table')
    log.info(f"Deleting {xw_table} ")
    CONFIG.dynamic_configuration()
    crosswalk_delete_table(CONFIG, log, xw_table)
    
main.add_command(delete_crosswalk_table)


