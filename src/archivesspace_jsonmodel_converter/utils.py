# Utilities
'''Currently only contains functions for agents, but may later include other functions'''
import re

# The 'Names' table contains the crosswalk between typo-fyed names and
# the name determined by the client
# because of the way the 'Names' table was constructed, we can't
# rely on only one level of indirection
def get_real_name_from_xwalk(xw,input_name, log, depth = 0):
    if input_name is None:
        return None
    init_row = xw.get_row('Names', input_name)
    row = None
    if init_row is not None:
        # if the row is marked as 'X', it isn't a legit agent
        if init_row['misc'] == 'X':
            return 'X'
        row = xw.get_row('Names', init_row['value'])    
    if row is None:
        return None
    elif row['misc'] == 'X':
        return 'X'
    elif row['value'] == row['orig_id']:
        return row['orig_id']
    elif row['value'] == '':
        log.error("Row value is EMPTY", agent=f"'row['orig_id']' ('input_name')")
        return None
    else:
        if depth > 3:
            log.error("Recursion on Name", agent=f"{input_name} val: {row['value']}")
            return None
        else:
            return get_real_name_from_xwalk(xw, row['value'],log, depth + 1)
        

def get_agent_uri(xw, name, log):
    uri = None
    uri = xw.get_aspace_id('agents', name)
    return uri
    