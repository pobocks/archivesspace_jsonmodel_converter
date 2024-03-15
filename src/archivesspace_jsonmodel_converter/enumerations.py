import csv
from csv import reader

xw = None
log = None
HEADERS = ["typeid","type","valid","val","enum"]
  
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
    if line["enum"].startswith('**'):
       # some values have been marked with '***' to indicate that they've been looked at, but determination of the associated enum is pending
        log.warn(f"Type {line['type']} Value{line['val']} ( {line['valid']}) is pending but added to crosswalk")
    added = xw.add_or_update('Enums', line["valid"], line["val"], line["enum"])
    return added
        
        
def convert_enums(config, input_log):
    '''Process a CSV file that has been hand-created to crosswalk data from the tblLookupValues to the enums in ArchivesSpace'''
    global filepath, xw, log
    filepath = config["enum_config"]["filepath"]
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
                # not all values have enums
                if lines["enum"] == ' ':
                    continue
                added = add_to_crosswalk(lines)
                if added:
                    ct = ct +1
    except Exception as e:
        log.error(f"Unable to process '{filepath}' ", error =e , exc_info = True)
    log.info(f"{ct} enums added or updated")
