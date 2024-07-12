

# Agents
'''Agents come from the Creator/Places table.  Creators don't have their own table 
    we only want archives (dept_code_id == 48)
    
  Because Creator names were entered free-hand, there are a lot of variations in spelling, punctuation, etc. for individual names that may actually refer to the same Creator.  Some cleanup work was done in advance, with a spreadsheet being created that is processed by name_xwalk.py.  
  
  Also, because there was only one text field for creator name, if there were multiple creators, they were entered together, with a variety of syntax strategies (e.g.: using semi colons, commas).  
  
  This script processes creator names by the following rules:
    1. determine whether the name belongs to a person or a corporation
    2. determine if the "name" represents multiple entities (if so, write to a csv file)
    3. lookup the name in the creators crosswalk; if it's there, we're done
    4. lookup the name in the name crosswalk table; if it exists, grab the "convert to" name
    5. lookup the convert to name to see if there already is an archivesspace ID; if so, we're done with that name
    6. if there is no agent id, create a new agent.
    
'''
import re
import csv
from asnake.jsonmodel import JM
from .utils import get_name_from_xwalk, get_agent_uri

NAME_QUERY= 'SELECT DISTINCT creator  from "tblcreator/place" ORDER_BY creator ASC'
PARENS_GROUP = '\((.*?)\)'  # used in findall, so not compiled
ET_AL_PAT = re.compile("et?(\.) al?(\.)|\swith\s|(\s|\,)eds?(\.|$|\s)+", re.IGNORECASE)
ED_PAT = re.compile("(\s|\,)eds?(\.|$|\s)+", re.IGNORECASE)
JR_PAT = re.compile('jr\.?$', re.IGNORECASE)
END_COMMA = re.compile(',$')
LINE_TAB_PAT = re.compile('\\n|\\t')
client = None
xw = None
conn = None # postgres connection
log = None
problem_list = []  # [problem_type, name, creatortype_id, creatortype_value]  problem type: "type" | "corp?" | "name"
missing_list = {}
report_only = True

def create_per_name_json(name):
    if name is None:
        return None
    nms = name.split(',',1)
    prim = nms[0]
    rest = ""
    if len(nms) > 1:
        rest = nms[1].strip()
    nmjson = JM.name_person(
        primary_name=prim,
        rest_of_name=rest,
        name_order='inverted',
        sort_name_auto_generate=True,
        source= 'local',
        publish=True
    )
    return nmjson

def create_corp_nm_json(name, is_conference):
    nmjson = JM.name_corporate_entity(primary_name=name,
                                        conference=is_conference,
                                        sort_name_auto_generate=True,
                                        source= 'local',
                                        publish=True)
    return nmjson
    
def create_place_json(subject):
        place = ''
        if subject:
            place = JM.agent_place(subjects=[{'ref': subject}], 
                publish=True)
        return place
    
def create_person_json(nmjson, placejson):
    pers = JM.agent_person(
        names= [nmjson],
        agent_places=[placejson], publish=True   )
    return pers
def create_corp_json(nmjson, placejson):
    corp = JM.agent_corporate(
        names= [nmjson],
        agent_places=[placejson] , publish=True )
    return corp

def add_to_aspace(orig_value,  agent):
    ''' Add an agent  to ArchivesSpace'''
    aspace_id = None
    response = None
    ag = None
    type = 'agents/people'
    if agent['jsonmodel_type'] == 'agent_corporate':
        type = 'agents/corporate_entities'
    response = client.post(type, json=agent).json()
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
            log.error(f"Error detected for {orig_value}: {error}\n json is:\n{agent}")
    else:
        log.error(f"Item {orig_value} not created for unknown reasons: {response}")
    return aspace_id

def create_agent_json(name,is_person, is_conference, placejson):
    agent_json = None
    if is_person:
        nmjson = create_per_name_json(name)
        if nmjson is not None:
            agent_json = create_person_json(nmjson, placejson)
        else:
            log.error(f"Unable to correctly process {name} as a Person")
    else:
        nmjson = create_corp_nm_json(name, is_conference)
        if nmjson is not None:
            agent_json = create_corp_json(nmjson, placejson)
        else:
            log.error(f"Unable to correctly process {name} as a Corporation")
    return agent_json

def process_person_name(input_name):
    '''
    Look for things in the name that require human intervention, like:
     Returns a list of names, or None if it can't be processed
    examples include:
     Barnes, Thomas C. et. al.
     Bannister, Bryant; Vivian, Gordon; and Mathews, Tom W.  
     Bannister, Brynt; Dean, Jeffrey S.; Robinson, William J.
     Barrett, S. A., and Gifford, E. W.
     Batkin, Jonathan ed.
     Bear, J. Michael (Byrnes, James Michael)
     Beerbower, W. R., engineer
     Begay, Antonio (Tone Chee)
    Adams, Ansel with introduction by Peter Wright and John Armor
    '''
    global xw
    # first, see if we've already processed this successfully
    name = get_name_from_xwalk(xw, input_name)
    if name is not None:
        return [name]
    # ok, we don't have an entry in the names xwalk
    names = input_name.split(";")
    if '' in names:
        names.remove('')
    # look for "and "
    if len(names) > 1:
        final = names[-1].strip()
        if final.startswith("and "):
            names[-1] = final[4:]   
    # look for "bad name" indicators
    bad_name = False
    for i in range(len(names)): 
        tmp = get_name_from_xwalk(xw, names[i])
        if tmp is not None:
            names[i] = tmp
            continue          
        if len(name.split(',')) > 2:
            bad_name = True
            log.warn(f'Muliple commas found in {name} [{input_name}]')
            break
        elif name.find('(') != -1 or name.find(')') != -1:
            bad_name = True
            log.warn(f'Parenthesis found in {name} [{input_name}]')
            break
        elif ET_AL_PAT.search(name) is not None:
            bad_name = True
            log.warn(f'"Editor", "et. al.","with", etc. at end of {name}? [{input_name}]')
            break       
        elif len(name.split(' ')) > 2:
            bad_name = True
            log.warn(f'Suspected extraneous words found in {name} [{input_name}]')
            break
        elif ',' not in name and ' ' in name:
            bad_name = True
            log.warn(f'Name {name} [{input_name}] missing comma; not in lastname, firstname order, or corporation?')
        names[i] = name
    if len(names) > 1:
        final = names[-1]
        if final.startswith("and "):
            names[-1] = final[4:]    
    if bad_name:
        return None
    return names

''' returns a tuple:   is_person, isconference  , placejson '''
def get_agent_info(row):
    #TODO: Look for 'corporate' and 'person' in orig_id
    is_conference = False
    placejson = None
    type_enum = None
    type_val = ''   
    is_person = True
    type_id = row[1]
    type_value = row[3]
    if type_value is not None:
        if 'corporate' in type_value:
            is_person = False 
        elif 'conference' in type_value or type_id == 332:
            is_conference = True
            is_person = False
        elif 'person' in type_value:
            is_person = True
        else:
            if type_id is not None:
                type_row =xw.get_row('Enums', type_id)
                if type_row is not None:
                    type_enum = type_row['aspace_id']
                if type_enum is None:
                    log.error(f"cannot find an enum for type {type_id} [{row[3]}]")
                    problem_list.append( ["type", row[0], type_id, type_value, row[4]])
                    return ([None, None, None])
    # when the original value indicated just an organization, company, etc., the `enum` set was '***'
                if type_enum.startswith('**'):
                    is_person = False 
    place_id = row[2]
    if place_id != '':
        place = xw.get_aspace_id('tblCreatorPlaces', place_id)
        if place is not None:
            placejson = create_place_json(place)
            # log.debug(f"Creating a place {place}")
    return([is_person, is_conference, placejson])
     
        
def process_agents():
    # need to talk to xw
    # to get creator place from 'tblCreatorPlaces' and row from 'Enums'
    ctr = 0
    errctr = 0
    name = ''
    init_name = ''
    cur = conn.cursor()
    # we order by place id and typeid  so that if there are two entries, we get a type in the first instance
    cur.execute(" SELECT creator, creatortypeid, creatorplaceid, LookupValue, itemid  from \"tblcreator/place\",tblLookupValues where itemId in (select itemId from tblItems where deptcodeid = 48) AND tblLookupValues.LookupValueId=creatortypeid order by creator asc, creatorplaceid desc, LookupValue desc;")
    while True:
        row = cur.fetchone()
        if row is None or row[0] is None:
            break
        if 'unknown' in row[0].lower() or 'none' in row[0].lower():
                log.warn(f'"None or unknown" detected in "{row[0]}" ')
                continue
        # if we have a uri already, we can stop right here!
        agent_uri = get_agent_uri(xw, row[0])
        if agent_uri is not None:
            # further processing not needed!
            log.debug(f"Have agent id for '{row[0]}'")
            continue
        # first let's see if there's a match in the Names table
        init_name = get_name_from_xwalk(xw, row[0])
        if init_name is None:
            init_name = re.sub(r"\s+", " ", row[0]).strip().strip(',')
            log.warning(f"'{init_name}' (item {row[4]}) not found in 'Names' table; skipping")
            if init_name not in missing_list:
                missing_list[init_name] = row[3]
            problem_list.append([ "missing",init_name, row[2],row[3], row[4]])
            continue
         # skip the repeats
        if name == init_name:
            continue
        else:
            name = init_name
        is_person, is_conference, placejson = get_agent_info(row)
        # log.debug(f"[{init_name}]  ({row[1]}) is person: {is_person}, is conference: {is_conference}")
        if is_person is None:
            log.error(f"Unable to continue to process '{row[0]}' [{row[3]}] because cannot determine person or corporate")
            continue  # stop right there and move on
        elif is_person:
            names = process_person_name(name)
            if names is not None and len(names) > 1:
                log.info(f"'{name} contains more than one name")
        else:
            names = [name]    # we don't really process corporate names
        if names is None:
            problem_list.append([ "name",init_name, row[1], row[3]], row[4])
            continue
        
        for name in names:
            # log.debug(f"Processing '{name}' from'{init_name}")
            aspace_id = get_agent_uri(xw, name)
            if aspace_id is not None:
                continue
            tmp = get_name_from_xwalk(xw, name)
            if tmp is None:
                log.warn(f"Can't find '{name}' in Crosswalk; skipping")
                continue  # we'll ignore these
            else:
                name = tmp
            # temp
            if len(name.split(" ")) > 1 and ',' not in name and is_person:
                log.warn(f"[{name}] has is_person but seems to be a company?, [{row[1]}] [{row[3]}]")
                problem_list.append(["corp?", name, row[1], row[3], row[4]])
                continue
            
            json = create_agent_json(name,is_person, is_conference, placejson)
            if json:
                if not report_only:
                    aspace_id = add_to_aspace(name, json)
                    if aspace_id is not None:
                        xw.add_or_update('Creators', name, name, aspace_id )
                        ctr = ctr + 1
                        log.info(f"Created: {aspace_id} \n with \n{json}") 
            else:
                errctr = errctr + 1
                if errctr  > 4:
                    break
    log.info(f"Processing complete with {ctr} names processed")
    if len(problem_list) > 0:
        log.warn(f"{len(problem_list)} problems parsing names or agent_info encountered")
        
    if errctr > 0:
        log.warn(f"At least {errctr} errors detected while creating json objects")
        
def write_missing_files(filepath):
    headerlist = ["orig","convert", "type"]  
    with open(filepath, "w", newline='', encoding='utf-8') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(headerlist)
        for name in missing_list:
            wr.writerow([name, "", missing_list[name]])  
                 
def write_problems_file(filepath):
    headerlist = ["type","name", "type_id","type_value", "itemId"]
    with open(filepath, "w", newline='', encoding='utf-8') as myfile:
        wr = csv.writer(myfile)
        # add header here!!
        wr.writerow(headerlist)
        for line in problem_list:
            wr.writerow(line)

def agents_create(config, input_log, only_report):
    global client, xw, conn, log, problem_list, report_only
    report_only = only_report
    log = input_log
    client = config["d"]["aspace"]
    client.authorize()
    xw = config["d"]["crosswalk"]
    xw.create_crosswalk()
    conn = config["d"]["postgres"]
    problem_output_filepath =  config["agents_config"]["problem_filepath"]
    missing_output_filepath = config["agents_config"]["missing_filepath"]
    process_agents()
    if len(problem_list) > 0:
        write_problems_file(problem_output_filepath)
    if len(missing_list) > 0:
        write_missing_files(missing_output_filepath)
    if conn:
        conn.close()
        
