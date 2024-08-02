# Subjects
from asnake.jsonmodel import JM

import re
import traceback

# Create and authorize the client

#need from tblLcshs and tblGeoPlaces
pattern =  r"\|([a-z])"

# HARDCODED DICTIONARY for LCSH subfields
SUBFIELD_DICT = {"a": "topical", "b": "topical", "c" : "geographic", "d": "temporal", "v": "genre_form", "x": "topical", "y": "temporal", "z": "geographic"}

client = None
xw = None
conn = None # postgres connection
log = None

def add_to_aspace(tablename, orig_id, subject, aid):
    ''' Add/update a subject to ArchivesSpace'''
    aspace_id = None
    response = None
    asubj = None
    if aid is not None:
        try:
            asubj = client.get(aid)
            if asubj.status_code == 200:
                subj = asubj.json()
                subject['lock_version'] = subj['lock_version']
                log.info(f"subject lock version: {subj['lock_version']}")
                for inx, term in enumerate(subj['terms']):
                    if inx < len(subject['terms']) and term['term'] == subject['terms'][inx]['term']:
                        subject['terms'][inx]['lock_version'] = term['lock_version']
                        log.info(f"\tterm lock: {term['lock_version']}")
            else:
                log.warn(f"URI {aid} not found")
                aid = None
        except Exception as e:
            log.error(f"unable to correctly retrieve lock_version for uri {aid} asubj: {asubj}")
            return None
    if aid is not None:
        response = client.post(aid, json=subject).json()
    else:
        response = client.post('subjects', json=subject).json()
    if 'status' in response and (response['status'] == 'Created' or response['status'] == 'Updated'):
        aspace_id = response['uri']
    elif 'error' in response:
        err = response['error']
         # treat a conflicting record as a win  (i.e., it's a duplicate in the original db)
        if 'conflicting_record' in err:
            aspace_id = err['conflicting_record'][0]
            log.warn(f'Possible duplicate for ID {orig_id} in {tablename} found with URI of {aspace_id}')
        else:
        # look for a reason for the error
            error = err
            if 'source' in err:
                error = err['source'][0]
            log.error(f"Error detected for ID {orig_id} in {tablename}",error=error)
    else:
        log.error(f"Item {orig_id} in {tablename} not created for unknown reasons", error=response)
    return aspace_id

def create_terms(subject,firstfield):
    '''creates a list of terms in jsonmodel format'''
    terms = []
    try:
        trmlist = re.split(pattern,subject)
        trmlist.insert(0, firstfield)  # the first subfield indicator is passed through
        term_dict  = map(lambda i: (trmlist[i], trmlist[i+1]), range(len(trmlist)-1)[::2])

        for sub,term in term_dict:
            if term != '':
                try:
                    entry = JM.term(vocabulary="/vocabularies/1", term_type=SUBFIELD_DICT[sub], term= term)
                    terms.append(entry)
                except Exception as e:
                    raise e
    except Exception as e:
        log.error(f"Unable to process term list {subject} ", error=e)
    return terms

def create_subject_json(orig_subj, firstfield, source ):
    ''' Create a json object for the subject
         firstfield tells us which category the first piece of the input subject is
         source tells us the source to use (e.g.: local or lcsh)'''
    terms = create_terms(orig_subj, firstfield)
    subject = JM.subject(publish="true", source=source, vocabulary="/vocabularies/1", terms=terms)
    return subject

# does the actual walking of the postgres db
def walk_db(tablename, firstfield, source, select, cur):
    ct = 0
    cur.execute(select)
    while True:
        row = cur.fetchone()
        if row == None or len(row) < 2:
            break
        orig_id = row[0]
        orig_val = row[1]
        if orig_val is None:
            log.warn(f"Original ID {orig_id} in {tablename} has 'None' as a value!")
            break
        if tablename == 'tblCreatorPlaces':
            # we don't try to create a Subject for this at this point;
            xw.add_or_update(tablename, orig_id, orig_val, '')
            continue
        try:
            subject = create_subject_json(orig_val, firstfield, source)
            aid = xw.get_aspace_id(tablename, orig_id)
            aspace_id = add_to_aspace(tablename, orig_id, subject, aid)
            if aspace_id is not None:
                added = xw.add_or_update(tablename, orig_id,orig_val,aspace_id)
                if added:
                    ct = ct + 1
            #TODO: what do we do with None aspace_ids?
            else:
                log.warn(f"{orig_id} in {tablename} ({orig_val}) was not converted")
        except Exception as e:
            traceback.print_exc(e)
            log.error(f"Exception  triggered on {orig_id} ({orig_val}), which will not be converted", error=e)
    return(ct)

def process_subjects(tablename, firstfield, source):
    ''' Take the values and add them to ArchivesSpace '''
    if conn is None:
        log.error("No conn in process subjects")
        return None
    selectct = f"SELECT COUNT(*) from {tablename}"
    select = f"SELECT * from {tablename}"
    if tablename == "tblLookupValues":
        selectct = "select count(*) from tbllookupvalues where lookuptypeid in (select lookuptypeid from tbllookuptypes where lookuptype='Genre')"
        select = "select lookupvalueid,lookupvalue from tbllookupvalues where lookuptypeid in (select lookuptypeid from tbllookuptypes where lookuptype='Genre')"
    try:
        # create a cursor
        cur = conn.cursor()
        # get a count
        cur.execute(selectct)
        count = cur.fetchone()
        log.info(f"Table {tablename} has {count[0]} entries")
        ct = walk_db(tablename, firstfield, source, select, cur)

    except Exception as e:
        log.error("Unexpected exception", error=e,exc_info=True)
    log.info(f"{ct} entries processed correctly")


def subjects_create(config,input_log):
    global client, xw, conn, log
    log = input_log
    client = config["d"]["aspace"]
    client.authorize()
    xw = config["d"]["crosswalk"]
    xw.create_crosswalk()
    conn = config["d"]["postgres"]
 #   log = config["d"]["subjectlog"]
    for table in ("tblLcshs,a,lcsh", "tblGeoPlaces,c,lcsh", "tblCreatorPlaces,c,local", "tblLookupValues,v,local"):
        x = table.split(',')
        process_subjects(x[0],x[1], x[2])
    if conn:
        conn.close()

# if __name__ == '__main__':
#     #connect()
#     process_lcshs('c:/Users/rlynn/aspacelinux/temp/lcshs.csv')
#     process_geoplaces('c:/Users/rlynn/aspacelinux/temp/geoplaces.csv')
