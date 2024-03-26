''' Create a crosswalk for normalizing agent names using a spreadsheet'''
import csv
from csv import reader
import re

xw = None
log = None
HEADERS = ["original","convert"]
  
def file_legit():
    try:
        with open(filepath,'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            # make sure we got the right file
            try:
                for h in HEADERS:
                    if h not in header:
                        raise Exception(f"Badly formed or missing header {header}")  
            except Exception as e:
                raise e
    except Exception as e:
        raise e
def add_to_crosswalk(line):
    added = False
    orig = re.sub(r"\s+", " ", line["original"]).strip().strip(',')  # we'll strip and compress the originating name
    conv = line["convert"].strip()
    if conv != '':
        if xw.get_row('Names', conv) is None:
            xw.add_or_update('Names', conv, conv, '') # make sure that the convert-to name has an entry in the table
    else:
        conv = orig   
    added = xw.add_or_update('Names', orig, conv, '')
    return added
        
        
def crosswalk_names(config, input_log):
    '''Process a CSV file that has been hand-created to crosswalk names in the  tblCreator/Place to a normalized form'''
    global filepath, xw, log
    filepath = config["name_xw_config"]["filepath"]
    xw = config["d"]["crosswalk"]
    log = input_log
    ct = 0
    try:
        xw.create_crosswalk()
        xw.delete_table(input_log, 'Names')
        file_legit()
        with open(filepath, mode ='r', encoding='utf-8-sig') as file:  
        # reading csv file
            csv_dict = csv.DictReader(file)
            for line in csv_dict:
                added = add_to_crosswalk(line)
                if added:
                    ct = ct +1
    except Exception as e:
        log.error(f"Unable to process '{filepath}' ", error =e , exc_info = True)
    log.info(f"{ct} names added or updated")
