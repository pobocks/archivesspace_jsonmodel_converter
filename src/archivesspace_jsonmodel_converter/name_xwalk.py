''' Create a crosswalk for normalizing agent names using a spreadsheet'''
import csv
from csv import reader

xw = None
log = None
HEADERS = ["original","convert to"]
  
def file_legit():
    try:
        with open(filepath,'r') as f:
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
    added = xw.add_or_update('Names', line["original"], line["convert to"], '')
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
        file_legit()
        with open(filepath, mode ='r') as file:  
        # reading csv file
            csv_dict = csv.DictReader(file)
            for lines in csv_dict:
                added = add_to_crosswalk(lines)
                if added:
                    ct = ct +1
    except Exception as e:
        log.error(f"Unable to process '{filepath}' ", error =e , exc_info = True)
    log.info("{ct} names added or updated")
