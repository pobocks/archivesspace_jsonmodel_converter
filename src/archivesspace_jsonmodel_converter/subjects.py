# Subjects
from asnake.jsonmodel import JM

import sys
import re
import traceback

# Create and authorize the client

#need from tblLcshs and tblGeoPlaces
pattern =  "\|([a-z])"

# HARDCODED DICTIONARY for LCSH subfields
SUBFIELD_DICT = {"a": "topical", "b": "topical", "c" : "geographic", "d": "temporal", "v": "genre_form", "x": "topical", "y": "temporal", "z": "geographic"}

client = None
xw = None
conn = None # postgres connection
log = None

def add_to_aspace(orig_id, subject):
    ''' Add a subject to ArchivesSpace'''
    aspace_id = None
    response = client.post('subjects', json=subject).json()
    if 'status' in response and response['status'] == 'Created':
        aspace_id = response['uri']
    elif 'error' in response:
        err = response['error']
        # treat a conflicting record as a win
        if 'conflicting_record' in err:
            aspace_id = err['conflicting_record'][0]
        else:
            # look for a reason for the error
            error = err
            if 'source' in err:
                error = err['source'][0]
            log.error("Error detected for ID {}: {}".format(orig_id, error))
    else:
        log.error("Item {} not created for unknown reasons: {}".format(orig_id, response))
    return aspace_id

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

def create_subject_json(orig_subj, firstfield, source ):
    terms = create_terms(orig_subj, firstfield)
    subject = JM.subject(publish="true", source=source, vocabulary="/vocabularies/1", terms=terms)
    return subject

def process_subjects(tablename, firstfield, source):
    ''' Take the values and add them to ArchivesSpace '''
    if conn is None:
        print("No conm in process subjects")
        return None
    ct = 0
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
                subject = create_subject_json(orig_val, firstfield, source)
                aspace_id = add_to_aspace(orig_id, subject)
                if aspace_id is not None:
                    xw.add_or_update(tablename, orig_id,orig_val,aspace_id)
                #TBD: what do we do with None aspace_ids?
                else:
                    log.warn("{} ({}) was not converted".format(orig_id, orig_val))
            except Exception as e:
                print(traceback.print_exc(e))
                log.error("Exception  triggered on {} ({}), which will not be converted".format( orig_id, orig_val), error=e)
            finally:
                ct = ct + 1
                if ct < 4:
                    continue
                else:
                    break
    except Exception as e:
        log.error("{}: exc_info:{}".format(e, sys.exc_info()[2]))


def subjects_create(config,input_log):
    global client, xw, conn, log
    log = input_log
    client = config["d"]["aspace"]
    client.authorize()
    xw = config["d"]["crosswalk"]
    xw.create_crosswalk()
    conn = config["d"]["postgres"]
 #   log = config["d"]["subjectlog"]
    for table in ("tblLcshs,a,lcsh", "tblGeoPlaces,c,lcsh", "tblCreatorPlaces,c,local"):
        x = table.split(',')
        process_subjects(x[0],x[1], x[2])
    if conn:
        conn.close()
        
# if __name__ == '__main__':
#     #connect()
#     process_lcshs('c:/Users/rlynn/aspacelinux/temp/lcshs.csv')
#     process_geoplaces('c:/Users/rlynn/aspacelinux/temp/geoplaces.csv')
