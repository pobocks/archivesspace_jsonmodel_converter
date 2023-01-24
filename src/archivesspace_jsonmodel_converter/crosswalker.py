'''Supports the storage and retrieval of ID mapping between the origin system and ArchivesSpace'''

import sqlite3
from .logger import get_logger
log = get_logger('crosswalk')

UPSERT = """INSERT INTO Crosswalk(orig_table, orig_id, value, aspace_id) VALUES(?, ?, ?, ?) ON CONFLICT
            DO UPDATE SET value = excluded.value, aspace_id = excluded.aspace_id"""
FETCH = 'SELECT aspace_id FROM Crosswalk WHERE orig_table=? AND orig_id=?'
class Crosswalk():
    def __init__(self, dbname):
        try:
            self.conn = sqlite3.connect(f'{dbname}.db', row_factory=sqlite3.Row)
        except sqlite3.Error as e:
            log.error('sqlite error', error=e)
            print(f'sqlite error: {e}')

    def __del__(self):
        self.conn.close();

    def crosswalk_exists(self):
        cursorObj = self.conn.cursor()
        with self.conn:
            # returns name of table (truthy) or None
            return cursorObj.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Crosswalk'").fetchone()
        # If an error happens, context manager swallows it, implicitly returns None


    def create_crosswalk(self):
        retval = False
        try:
            with self.conn:
                if not self.crosswalk_exists():
                    cursorObj = self.conn.cursor()
                    cursorObj.execute("""CREATE TABLE Crosswalk(id integer PRIMARY KEY,
                                                                orig_table text,
                                                                orig_id text,
                                                                value text,
                                                                aspace_id text,
                                                                UNIQUE(orig_table,orig_id))""")

                    retval = True
                else:
                    print("already exists!")

        except Error as e:
            print("unable to create Crosswalk: {}".format(e))

        return retval

    def add_or_update(self, orig_table, orig_id, value, aspace_id):
        '''Add a crosswalk row if it doesn't already exist; otherwise update'''

        cursorObj = self.conn.cursor()
        entities = [orig_table, orig_id, value, aspace_id]
        try:
            with conn:
                cursorObj.execute(UPSERT,entities)

        except Error as e:
            print("Couldn't even update: {}".format(e))

    def get_aspace_id(self, orig_table, orig_id):
        ''' returns the ArchivesSpace URL corresponding the the original table/original ID mapping'''
        cursorObj = self.conn.cursor()
        aspace_id = ""
        try:
            with conn:
                cursorObj.execute(FETCH, [orig_table, orig_id])
                row = cursorObj.fetchone() # index ensures uniqueness so this is sole result or None
                if row:
                    return row['aspace_id']
        except Error as e:
            print(e)


    def drop_crosswalk(self):
        '''Drop the Crosswalk table all together'''
        cursorObj = conn.cursor()
        with conn:
            cursorObj.execute("DROP TABLE Crosswalk")
