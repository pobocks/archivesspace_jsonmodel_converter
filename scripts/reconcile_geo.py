'''
Compares the entries in crosswalk's creator places with entries in the tblGeo, proposes matches
'''
import sys
import os
import argparse
import csv
from archivesspace_jsonmodel_converter.crosswalker import  Crosswalk
from archivesspace_jsonmodel_converter.logger import get_logger

HEADERS = ['creatorplace', 'id', 'geoplace']
xw = None
log = None
c_ctr = 0
output = []

def get_geos(value, id):
    global g_ctr
    r_ctr = 0
    for row in xw.fetch_rows(log,'tblGeoPlaces', f" value like \"{value.split(',')[0]}%\""):
        output.append([value, id, row['value']])
        r_ctr += 1
        g_ctr += 1
    if r_ctr == 0:
        output.append([value, id, "** NOTHING!"])
        
def write_to_csv(list, file):
    try:
        with open(file, "w", newline='', encoding='utf-8') as outfile:
            wr = csv.writer(outfile) 
            wr.writerow(HEADERS)
            for row in list:
                if row is not None:
                    wr.writerow(row)
    except Exception as e:
        print(f"Problem found in writing to {file}: {e.__class__.__doc__} [{e.__class__.__name__}]")
        
if __name__ == "__main__":
    global g_ctr
    g_ctr = 0
    # command line arguments
    parser = argparse.ArgumentParser(description='Get options.')
    parser.add_argument("filename", help="Path to output csv")
    parser.add_argument("-xwalk", default="crosswalk",help="Name of crosswalk db; default 'crosswalk'")
    parser.add_argument("-wd", default=".", help="Crosswalk working directory; default '.'")
    args = parser.parse_args()
    outfile = args.filename
    config = { 
                'crosswalk_config': {
                    'name': args.xwalk,
                },
                'working_directory':  args.wd
            }
    log = get_logger()
    xw = Crosswalk(config)
    xw.create_crosswalk() 
    places = []
    for row in xw.fetch_rows(log,'tblCreatorPlaces'):
        places.append([row['value'],row['orig_id']])
    places.sort()
    for place in places:                 
        get_geos(place[0],place[1])
    write_to_csv(output, outfile )
        
    