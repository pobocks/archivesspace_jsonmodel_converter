'''Supports the storage and retrieval of ID mapping between the origin system and ArchivesSpace'''

import sqlite3
from sqlite3 import Error

ADD_NEW = 'INSERT INTO Crosswalk(orig_table, orig_id, value, aspace_id) VALUES(?, ?, ?, ?)'
UPDATE = 'UPDATE Crosswalk SET aspace_id="{}", value="{}" WHERE orig_table="{}" AND orig_id="{}"'
UPDATE_MATCH = "SELECT id,orig_table,orig_id, value FROM Crosswalk WHERE orig_table={} AND orig_id='{}'"
DBNAME=""
FETCH = 'SELECT aspace_id FROM Crosswalk WHERE orig_table="{}" AND orig_id="{}"'
def init_dbname(name):
    '''This will be replaced with something from the yml file'''
    DBNAME = name
def sql_connection():
    try:
        conn = sqlite3.connect(DBNAME + '.db')
    except Error as e:
        print(e)
        conn.close()
        conn = None
    finally:
        return conn
   
def crosswalk_exists():
    conn = sql_connection()
    cursorObj = conn.cursor()
    retval = False
    try:
        bar =cursorObj.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Crosswalk'").fetchall()
        retval = bar != []
    except Error as e:
        print("unable to determine if Crosswalk exists: {}".format(e))
    return(retval)

def create_crosswalk():
    retval = False
    try:
        conn = sql_connection()
        if not crosswalk_exists():
            cursorObj = conn.cursor()
            cursorObj.execute("CREATE TABLE Crosswalk(id integer PRIMARY KEY, orig_table text, orig_id text, value text, aspace_id text, UNIQUE(orig_table,orig_id))")
            conn.commit()
        else:
            print("already exists!")
        retval = True
    except Error as e:
        print("unable to create Crosswalk: {}".format(e))
    finally:
        conn.close()
    return retval

def add_or_update(orig_table, orig_id, value, aspace_id):
    '''Add a crosswalk row if it doesn't already exist; otherwise update'''
    conn = sql_connection()
    cursorObj = conn.cursor()
    entities = [orig_table, orig_id, value, aspace_id]
    try:
        cursorObj.execute(ADD_NEW,entities)
        conn.commit()
    except Error as e:
        if str(e).startswith('UNIQUE constraint failed'):
            try:
                cursorObj.execute(UPDATE.format(aspace_id, value, orig_table, orig_id))
                conn.commit()
            except Error as e:
                print("Couldn't even update: {}".format(e))
        else:
            print("Problem adding {}: {}".format(entities,e))
    finally:
        conn.close()

def get_aspace_id(orig_table, orig_id):
    ''' returns the ArchivesSpace URL corresponding the the original table/original ID mapping'''
    conn = sql_connection()
    cursorObj = conn.cursor()
    aspace_id = ""
    try:
        cursorObj.execute(FETCH.format(orig_table, orig_id))
        rows = cursorObj.fetchall()
        if len(rows) > 1:
            raise Exception("PROBLEM: MORE THAN ONE ROW!")
        if len(rows) == 1:
            aspace_id = rows[0][0]
    except Error as e:
        print(e)
    finally:
        conn.close()
    return aspace_id

def drop_crosswalk():
    '''Drop the Crosswalk table all together'''
    conn = sql_connection()
    cursorObj = conn.cursor()
    cursorObj.execute("DROP TABLE Crosswalk")

def execute_select_command(command):
    ''' Execute an input SELECT & fetchall command '''
    fetched = None
    try:
        conn = sql_connection()
        cursorObj = conn.cursor()
        fetched = cursorObj.execute(command).fetchall()
    except Error as e:
        print("Unable to execute command \"{}\": {}".format(command, e))
    finally:
        conn.close()
    return fetched

