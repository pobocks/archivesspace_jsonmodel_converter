# Utilities
'''Currently only contains functions for agents, but may later include other functions'''
import re

# The 'Names' table contains the crosswalk between typo-fyed names and
# the name determined by the client
def get_real_name_from_xwalk(xw,input_name, log):
    if input_name is None:
        return None
    row = xw.get_row_by_value('Names', input_name)
    
    if row is None:
        return None
    return row['orig_id']

def get_agent_uri(xw, name, log):
    uri = None
    uri = xw.get_aspace_id('Creators', name)
    return uri
    