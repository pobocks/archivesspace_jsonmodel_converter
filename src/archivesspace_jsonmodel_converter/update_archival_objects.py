""" Some possible postgres statements  (replacing the "itemid in ..."):

select * from tblitemlcshs NATURAL FULL JOIN tblitemgeoplaces NATURAL FULL JOIN "tblcreator/place" where tblitemgeoplaces.itemid in ( select itemid from tblitems where deptcodeid = 48 limit 100);

select * from tblitemlcshs L NATURAL FULL JOIN tblitemgeoplaces G NATURAL FULL JOIN "tblcreator/place" C where L.itemid in ( select itemid from tblitems where deptcodeid = 48 limit 100) or G.itemid in ( select itemid from tblitems where deptcodeid = 48 limit 100) or C.itemid in ( select itemid from tblitems where deptcodeid = 48 limit 100);

"""
import csv
import re
from asnake.jsonmodel import JM
from psycopg.rows import dict_row
from .utils import get_real_name_from_xwalk, get_agent_uri


# Dynamic globals, set via config
client = None
xw = None
conn = None
log = None

# Static globals
creator_table = '\"tblCreator/Place\"'
corp_table = 'tblItemNamesCorp'
pers_table = 'tblItemNamesPer'

docs_table = 'tblItemDocs'

'''  For each AO in the crosswalk:

TO ASSOCIATE AGENTS:
from tblCreator/Places get:
  (by itemId)
   1. Creator
   3. CreatorTypeId
   4. CreatorPlaceId

from tblItemNamesCorp get
   (by itemId):
   1. NameCorp
from tblItemNamesPers get
   (by itemId):
    1.  NamePers

#TODO:
TO ASSOCIATE EXTERNAL DOCS:  
from tblItemDocs get
   (by itemId):
      2.DocDate
      4. DocName
      5. DocDesc
'''
has_new_agent = False # used to track whether the item should be updated
  

def add_place_to_agent(agent_id, place_name):
   ''' "agent_places": [
        {
            "publish": true,
            "created_by": "admin",
            "last_modified_by": "admin",
            "create_time": "2024-12-27T17:19:21Z",
            "system_mtime": "2024-12-27T17:19:21Z",
            "user_mtime": "2024-12-27T17:19:21Z",
            "lock_version": 0,
            "place_role": "other_assoc",
            "jsonmodel_type": "agent_place",
            "dates": [],
            "notes": [],
            "subjects": [
                {
                    "ref": "/subjects/2829"
                }
            ]
        }
    ]
    '''  
   #TODO
   # get place url
   # get agent json
   # make sure the url isn't already there
   # add to agent_places subjects
   return True

'''
Lookup relation type in enums; create a relator if there is one
'''
def get_relator(rel_id):
   relator = None
   if rel_id is not None and rel_id != '':
      row = xw.get_row('Enums', rel_id)
      if row is not None:
         relator = row['aspace_id']
         if relator =='*':
            relator = None
   return(relator)

def create_linked_agent(agent_id, role, relator, itemId):
   json = None
   if relator is None:
      relator = ''
   json = {'role': role, 'relator': relator, 'terms': [], 'ref': agent_id}
   return json

def get_real_agent(agent_name):
   mod_agent = re.sub(r'\s+', ' ', agent_name.strip())
   real_agent = get_real_name_from_xwalk(xw, mod_agent, log)
   if real_agent == 'X':
      # this isn't an agent we care about
      return 'X'
   if real_agent is None:
      # see if the name is given as "{first} {last}"; try to find as "{last}, {first}"
      if mod_agent.find(',') == -1:
         parts = mod_agent.split(' ')
         mod_agent = parts[-1] + ", " + " ".join(parts[0:-1])
         try:
            real_agent = get_real_name_from_xwalk(xw, mod_agent, log)
            if real_agent is not None:
               log.warn(f"Using '{mod_agent}' for'{agent_name}': found'{real_agent}' ")
         except Exception as e:
            log.error("Error getting real agent", agent=agent_name)
            return None
            
   return real_agent

def matched_link(link, oldlink):
   ret_val = True
   #oldlink['ref'] == agent_uri and oldlink['role'] == role and oldlink['relator'] == relator
   if oldlink['ref'] != link['ref'] or oldlink['role'] != link['role']:
      ret_val = False
   else:
      oldrel = oldlink['relator'] if 'relator' in oldlink else ''
      newrel = link['relator'] if 'relator' in link else ''
      if oldrel != newrel:
         ret_val = False      
   return ret_val
         
def process_agent(itemId, role, rel_id, agent_name, repository_note, linked):
   global has_new_agent
   if agent_name is None:
      return repository_note, linked
   real_agent = get_real_agent(agent_name)
   if real_agent == 'X':
      log.debug("Ignore", agent=agent_name)
      return repository_note, linked
   if real_agent is None:
      log.debug("not in CrossWalk", itemId=itemId, agent=agent_name )
      if agent_name.strip() not in missing:
         missing.append(agent_name.strip())
      return repository_note, linked   
   agent_uri = get_agent_uri(xw, real_agent, log)
   if agent_uri is None:
      log.debug("No agent URI found ", itemId=itemId, agent=f"{real_agent} ('{agent_name}')")
      return repository_note, linked
   relator = get_relator(rel_id)
   link = create_linked_agent(agent_uri, role, relator, itemId)
   if link is None:
      log.warn("link problem", itemId=itemId,agent=f"'{real_agent}' ('{agent_name}')", role=role, relator=relator)      
      return repository_note, linked
   # don't add if it's already there!
   #TODO: refactor
   if not any(matched_link(link,oldlink) for oldlink in linked):
      linked.append(link)
      has_new_agent = True
      if real_agent != agent_name:
         repository_note += f"; '{real_agent}' listed as '{agent_name}' in legacy system"
   else:
      log.debug("Agent already linked", itemId=itemId, agent=agent_name, relator=rel_id)
   return repository_note, linked

def process_creator_places(itemId, repository_note, linked ):
   ctr = 0
   with conn.cursor(row_factory=dict_row) as cur:
        for row in cur.execute(f"SELECT * from \"tblcreator/place\" where itemId='{itemId}'"):
           ctr += 1
           creator = row['creator']
           rel_id = row['creatortypeid']
           if creator is None:
               continue
           repository_note, linked = process_agent(itemId, 'creator', rel_id, creator,repository_note, linked)
           place = row['creatorplaceid']
           # TODO: process place
#   if report_only and ctr > 0:  
#  log.debug("creators found", itemId=itemId, creators_found=ctr)
   return repository_note, linked

def process_item_names(itemId,repository_note, linked):
   global stage
   ctr = 0
   for what in ('pers', 'corp'):
      stage = f"{what} Item Names"
      with conn.cursor(row_factory=dict_row) as cur:
         for row in cur.execute(f"SELECT * from tblItemNames{what} where itemId='{itemId}'"):            
            ctr += 1
            agent = row[f"name{what}"]
            repository_note, linked = process_agent(itemId, 'subject',"", agent, repository_note, linked)  
   # if report_only and ctr > 0:  
   #    log.debug( "item names found", itemId=itemId, names_found = ctr)
      return repository_note, linked
   
            
def get_json(as_id):
   json = None
   try:
      json = client.get(as_id)
      if json.status_code == 200:
         json = json.json()
      else:
         log.error(f"URI {as_id} not found")
   except Exception as e:
      log.error(f"Problem trying to access URI {as_id}: {e}")
   return json
      
            
def process_items():
   global stage
   global has_new_agent
   ctr = 0
   for row in xw.fetch_rows(log, 'tblitems'):
      has_new_agent = False
      itemId = row['orig_id']
      ctr += 1
      item_uri = row['aspace_id']
      if item_uri is None:
         log.debug("missing aspaceURI",itemId=itemId)
         continue
      item_json = get_json(item_uri)
      if item_json is None:
         log.warn("no item json", itemId=itemId, itemURI=item_uri)
         continue
      repository_note = "" 
      if 'repository_processing_note' in item_json:
         repository_note = item_json["repository_processing_note"]    
      linked = item_json["linked_agents"]
      if linked is None:
         linked = []
      stage = "CreatorPlaces"
      repository_note, linked = process_creator_places(itemId,repository_note, linked)
      repository_note, linked = process_item_names(itemId, repository_note, linked)
      item_json["linked_agents"] = linked
      repository_note = repository_note.lstrip(";").strip()
      item_json["repository_processing_note"] = repository_note
      if repository_note != '' and report_only:
         log.debug("Repository Note",itemId=itemId,repo_note=repository_note)
      if has_new_agent and not report_only :  # don't bother updating if there's no new agent!
         # try to update the item
         response = client.post(item_uri, json=item_json).json()
         if 'status' in response and response['status'] == 'Updated':
            log.debug("updated", itemId=itemId)
         elif 'error' in response:
            err = response['error']
            log.error(f"Error updating {itemId}, error=err ")

def archival_objects_update(config, input_log, only_report = True):
   global client, xw, conn, log, missing, report_only, stage
   stage = ""
   report_only = only_report
   log = input_log
   client = config["d"]["aspace"]
   client.authorize()
   xw = config["d"]["crosswalk"]
   xw.create_crosswalk()
   conn = config["d"]["postgres"]
   missing = []
   log.info(f"Report only? {report_only}.")
   process_items()
