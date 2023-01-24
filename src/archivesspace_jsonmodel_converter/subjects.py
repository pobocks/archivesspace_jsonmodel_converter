# Subjects
from asnake.jsonmodel import JM

import sys
import re

# Create and authorize the client

#need from tblLcshs and tblGeoPlaces
pattern =  "\|([a-z])"

# HARDCODED DICTIONARY for LCSH subfields
SUBFIELD_DICT = {"a": "topical", "b": "topical", "c" : "geographic", "d": "temporal", "v": "genre_form", "x": "topical", "y": "temporal", "z": "geographic"}

client = None
xw = None
conn = None # postgres connection

def add_to_aspace(orig_id, subject):
    ''' Add a subject to ArchivesSpace'''
    new_id = None
    response = client.post('subjects', json=subject).json()
    if 'status' in response and response['status'] == 'Created':
        new_id = response['uri']
    elif 'error' in response:
        err = response['error']
        # treat a conflicting record as a win
        if 'conflicting_record' in err:
            new_id = err['conflicting_record'][0]
            # print("{} Already exists as {}".format(orig_id, new_id))
        else:
            # look for a reason for the error
            error = err
            if 'source' in err:
                error = err['source'][0]
            print("Error detected for ID {}: {}".format(orig_id, error))
    else:
        print("Item {} not created for unknown reasons: {}".format(orig_id, response))
    return new_id

def create_terms(subject,firstfield):
    '''creates a list of terms in jsonmodel format'''
    trmlist = re.split(pattern,subject)
    trmlist.insert(0, firstfield)  # the first subfield indicator is passed through
    term_dict  = map(lambda i: (trmlist[i], trmlist[i+1]), range(len(trmlist)-1)[::2])
    terms = []
    for sub,term in term_dict:
        if term != '':
            try:
                entry = JM.term(vocabulary="/vocabularies/1", term_type=SUBFIELD_DICT[sub], term= term)
                terms.append(entry)
            except Exception as e:
                raise e
    return terms

def create_subject_json(orig_subj, firstfield):
    terms = create_terms(orig_subj, firstfield)
    subject = JM.subject(publish="true", source="lcsh",vocabulary="/vocabularies/1", terms=terms)
    return subject

def process_subjects(tablename, firstfield):
    ''' Take the values and add them to ArchivesSpace '''
    if conn is None:
        return None

    try:
        # create a cursor
        cur = conn.cursor()
        cur.execute("SELECT * from {}".format(tablename))
        while True:
            row = cur.fetchone()
            if row == None or len(row) < 2:
                break
            orig_id = row[0]
            orig_val = row[1]
            try:
                subject = create_subject_json(orig_val, firstfield)
                new_id = add_to_aspace(orig_id, subject)
                if new_id is not None:
                    xw.add_or_update(tablename, orig_id,orig_val,new_id)
                #TBD: what do we do with None new_ids?
                else:
                    print("{} '{}' was not converted".format(orig_id, orig_val))
            except Exception as e:
                print("Exception '{}' triggered on {} '{}', which will not be converted".format(e, orig_id, orig_val))
            finally:
                continue
    except Exception as e:
        print(e)
        print(sys.exc_info()[2])


def subjects_create(config):
    global client, xw, conn
    client = config["d"]["asnake"].client
    client.authorize()
    xw = config["d"]["crosswalk"]
    xw.create_crosswalk()
    conn = config["d"]["postgres"]
    for table in ("tblLcshs,a", "tblGeoPlaces,c"):
        x = table.split(',')
        process_subjects(x[0],x[1])

# if __name__ == '__main__':
#     #connect()
#     process_lcshs('c:/Users/rlynn/aspacelinux/temp/lcshs.csv')
#     process_geoplaces('c:/Users/rlynn/aspacelinux/temp/geoplaces.csv')
