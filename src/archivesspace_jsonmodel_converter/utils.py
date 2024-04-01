# Utilities
'''Currently only contains functions for agents, but may later include other functions'''
import re

def get_name_from_xwalk(xw,input_name):
    if input_name is None:
        return None
    name = re.sub(r"\s+", " ", input_name).strip().strip(',')
    row = xw.get_row('Names', name)
    if row is None:
        return None
    return row['value']

def get_agent_uri(xw, input_name):
    uri = None
    name = get_name_from_xwalk(xw, input_name)
    if name is not None:
        uri = xw.get_aspace_id('Creators', name)
    return uri
    