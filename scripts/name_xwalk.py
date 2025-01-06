''' Create a crosswalk for normalizing agent names using a spreadsheet'''
import sys
import os
import argparse
import csv
from archivesspace_jsonmodel_converter.crosswalker import  Crosswalk
from archivesspace_jsonmodel_converter.logger import get_logger

xw = None
log = None
HEADERS = ["ORIG_ID","VALUE", "MISC"]
  
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
        print("broke at file legit")
        raise e
    
def add_to_crosswalk(line):
    '''
       line consists of three keys:
        ORIG_ID	 what's in the MDB 
        VALUE  what Nik has determined is the correct VALUE
        MISC    'P', 'C', 'F', 'X'
     '''
    added = False
    orig = line['ORIG_ID'].strip()
    conv = line['VALUE'].strip()
    if conv != orig:
        if xw.get_row('Names', conv) is None:
            xw.add_or_update('Names', conv, conv, '', line['MISC'] ) # make sure that the convert-to name has an entry in the table
    added = xw.add_or_update('Names', orig, conv, '', line['MISC'])
    return added
        
if __name__ == "__main__":
    global filepath 
    # command line arguments
    parser = argparse.ArgumentParser(description='Get options.')
    parser.add_argument("filename", help="Path to input csv")
    parser.add_argument("-xwalk", default="crosswalk",help="Name of crosswalk db; default 'crosswalk'")
    parser.add_argument("-wd", default=".", help="Crosswalk working directory; default '.'")
    parser.add_argument("-c", action="store_true", help="Clear Names in crosswalk")
    args = parser.parse_args()
    filepath = args.filename
    print(filepath)
    if not os.path.exists(filepath):
        sys.exit(f"{filepath} not found")
    config = { 
                'crosswalk_config': {
                    'name': args.xwalk,
                },
                'working_directory':  args.wd,
                'logging_config': {
                    'level': "DEBUG",
                    'stream_json': "true"
                }
            }
    
    ctr = 0
    linectr = 0
    try:
        xw = Crosswalk(config)
        xw.create_crosswalk()
        if args.c:        
            xw.delete_table(get_logger('crosswalk'), 'Names')
        file_legit()
        with open(filepath, mode='r') as file: 
            # reading csv file
            csv_dict = csv.DictReader(file)
            for line in csv_dict:
                try:
                    if add_to_crosswalk(line):
                        ctr +=1
                except Exception as e:
                    print(f"Error on line {linectr}: {e}")
                linectr += 1
    except Exception as e:
        print(f"Unable to process '{filepath}' :{e}")
    print(f"{linectr} lines read. {ctr} names added or updated")
