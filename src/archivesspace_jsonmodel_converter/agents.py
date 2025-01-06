

# Agents
'''
    We generate agents from the 'Names' table, which is populated from
    a spreadsheet vetted by the client.

    Because agent names were entered free-hand in the original database, the spreadsheet is used to determine which of the various name is the one to be used.  The 'Names' table uses columns as follows:
            orig_id     The name as it was entered into the database (surrounding white space removed)
            value       The name that the orig_id maps to.
            misc        P for person, C for corporate/organization, F for family, X for "do not use"
    After agents are created, the Names table will be used to map agents to items.  This will be done in a separate step.

'''
import re
import csv
from asnake.jsonmodel import JM
from .utils import get_real_name_from_xwalk, get_agent_uri

FETCH_SUFFIX = "orig_id = value and misc in ('P','F', 'C') order by orig_id"


client = None
xw = None
log = None
problem_list = []  # [problem_type, name, creatortype_id, creatortype_value]  problem type: "type" | "corp?" | "name"
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

def create_corp_nm_json(name):
    nmjson = JM.name_corporate_entity(primary_name=name,
                                        sort_name_auto_generate=True,
                                        source= 'local',
                                        publish=True)
    return nmjson
def create_family_nm_json(name):
     nmjson = JM.name_family(family_name=name,
                                sort_name_auto_generate=True,
                                source='local',
                                publish=True)
     return nmjson

# def create_place_json(subject):
#         place = ''
#         if subject:
#             place = JM.agent_place(subjects=[{'ref': subject}],
#                 publish=True)
#         return place

def create_person_json(nmjson):
    pers = JM.agent_person(
        names= [nmjson], publish=True   )
    return pers
def create_corp_json(nmjson):
    corp = JM.agent_corporate(
        names= [nmjson], publish=True )
    return corp
def create_family_json(nmjson):
    fam = JM.agent_family(
            names=[nmjson],
            publish=True
    )
    return fam

# def modified_agent_json(name, agent):
#     # check to see if the agent already exists, whether need to add a placejson
#     aspace_id = get_agent_uri(xw,name)
#     if aspace_id:
#         old = client.get(aspace_id)
#         if old.status_code !=  200:
#             return None
#         oldj = old.json()
#         if agent['agent_places'] and  not oldj['agent_places']:
#             oldj['agent_places'] = agent['agent_places']
#             log.warn(f"Adding agent places to already created agent {aspace_id}")
#         return(aspace_id, oldj)



def add_to_aspace(name,  agent):
    ''' Add an agent  to ArchivesSpace'''
    aspace_id = None
    response = None
    ag = None
    type = 'agents/people'
    # mods = modified_agent_json(name, agent)
    # if mods:
    #     response = client.post(mods[0], json=mods[1]).json()
    # else:
    if agent['jsonmodel_type'] == 'agent_corporate':
        type = 'agents/corporate_entities'
    elif agent['jsonmodel_type'] == 'agent_family':
        type = 'agents/families'
    response = client.post(type, json=agent).json()
    if 'status' in response and response['status'] in ['Created', 'Updated']:
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
            else:
                error = response
            log.error(f"Error detected for {name}: {error}\n json is:\n{agent}")
    else:
        log.error(f"Item {name} not created for unknown reasons: {response}")
    return aspace_id

def create_agent_json(name,misc):
    agent_json = None
    if misc == 'P':
        nmjson = create_per_name_json(name)
        if nmjson is not None:
            agent_json = create_person_json(nmjson)
        else:
            log.error(f"Unable to correctly process {name} as a Person")
    elif misc == 'C':
        nmjson = create_corp_nm_json(name)
        if nmjson is not None:
            agent_json = create_corp_json(nmjson)
        else:
            log.error(f"Unable to correctly process {name} as a Corporation")
    else:
        nmjson = create_family_nm_json(name)
        if nmjson is not None:
            agent_json = create_family_json(nmjson)
        else:
            log.error(f"Unable to correctly process {name} as a Family")
    return agent_json


# ''' returns a tuple:   is_person, isconference  , placejson '''
# def get_agent_info(row):
#     #TODO: Look for 'corporate' and 'person' in orig_id
#     is_conference = False
#     placejson = None
#     type_enum = None
#     type_val = ''
#     is_person = True
#     type_id = row[1]
#     type_value = row[3]
#     if type_value is not None:
#         if 'corporate' in type_value:
#             is_person = False
#         elif 'conference' in type_value or type_id == 332:
#             is_conference = True
#             is_person = False
#         elif 'person' in type_value:
#             is_person = True
#         else:
#             if type_id is not None:
#                 type_row =xw.get_row('Enums', type_id)
#                 if type_row is not None:
#                     type_enum = type_row['aspace_id']
#                 if type_enum is None:
#                     log.error(f"cannot find an enum for type {type_id} [{row[3]}]")
#                     problem_list.append( ["type", row[0], type_id, type_value, row[4]])
#                     return ([None, None, None])
#     # when the original value indicated just an organization, company, etc., the `enum` set was '***'
#                 if type_enum.startswith('**'):
#                     is_person = False
#     place_id = row[2]
#     if place_id != '':
#         place = xw.get_aspace_id('tblCreatorPlaces', place_id)
#         if place is not None:
#             placejson = create_place_json(place)
#             # log.debug(f"Creating a place {place}")
#     return([is_person, is_conference, placejson])

def process_agents():
    # need to talk to xw
    #
    ctr = 0
    errctr = 0
    name = ''
    ctr = 0
    try:
        for row in xw.fetch_rows(log, "Names",FETCH_SUFFIX):
            ctr += 1
            name = row['orig_id'].strip()
            type = row['misc']
            #print(f"{row['orig_id']},{row['value']},{row['misc']}")
            # SPECIAL CASE!  we can't do multiple names here
            if type == 'P' and (name.find(' and ') != -1 or name.find(' & ') != -1):
                problem_list.append(['> 1 name', name])
                continue
            agent_uri = get_agent_uri(xw, name, log)
            if agent_uri is not None:
                # further processing not needed!
                log.debug(f"Have agent id for '{row['orig_id']}'")
                continue
            json = create_agent_json(name, type)
            log.debug(f"Json for type {type} '{name}', {json['jsonmodel_type']}")
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
    except Exception as e:
            print(f"error getting names: {e}")

    log.info(f"Processing complete with {ctr} names processed")
    if len(problem_list) > 0:
        log.warn(f"{len(problem_list)} problems parsing names or agent_info encountered; output to '{problem_output_filepath}")

    if errctr > 0:
        log.warn(f"At least {errctr} errors detected while creating json objects")



def write_problems_file(filepath):
    headerlist = ["type","name", "type_id","type_value", "itemId"]
    with open(filepath, "w", newline='', encoding='utf-8') as myfile:
        wr = csv.writer(myfile)
        # add header here!!
        wr.writerow(headerlist)
        for line in problem_list:
            wr.writerow(line)

def agents_create(config, input_log, only_report):
    global client, xw, conn, log, problem_list, report_only, problem_output_filepath
    report_only = only_report
    log = input_log
    client = config["d"]["aspace"]
    client.authorize()
    xw = config["d"]["crosswalk"]
    xw.create_crosswalk()
    problem_output_filepath =  config["agents_config"]["problem_filepath"]
    process_agents()
    if len(problem_list) > 0:
        write_problems_file(problem_output_filepath)
