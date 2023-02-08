'''Supports the storage and retrieval of ID mapping between the origin system and ArchivesSpace'''

import sqlite3
import traceback
from .logger import get_logger
log = get_logger('crosswalk')
ADD_NEW = 'INSERT INTO Crosswalk(orig_table, orig_id, value, aspace_id) VALUES(?, ?, ?, ?)'
UPDATE = 'UPDATE Crosswalk SET aspace_id="{}", value="{}" WHERE orig_table="{}" AND orig_id="{}"'

UPSERT = """INSERT INTO Crosswalk(orig_table, orig_id, value, aspace_id) VALUES(?, ?, ?, ?) ON CONFLICT
            DO UPDATE SET value = excluded.value, aspace_id = excluded.aspace_id"""
FETCH_ROW = 'SELECT * FROM Crosswalk WHERE orig_table=? AND orig_id=?'
FETCH = 'SELECT aspace_id FROM Crosswalk WHERE orig_table=? AND orig_id=?'
class Crosswalk():
    def __init__(self, dbname):
        try:
            self.conn = sqlite3.connect(f'{dbname}.db')
            self.conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            log.error('sqlite error', error=e)
            print(f'sqlite error: {traceback.format_exc(e)}')

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

        except sqlite3.Error as e:
            log.error("Sqlite3 error: unable to create Crosswalk:", error=e, exc_info=True )
        return retval

    def add_or_update(self, orig_table, orig_id, value, aspace_id):
        '''Add a crosswalk row if it doesn't already exist; otherwise update'''
        cursorObj = self.conn.cursor()
        entities = [orig_table, orig_id, value, aspace_id]
        retval = False
        try:
            with self.conn:
                cursorObj.execute(ADD_NEW,entities)
                self.conn.commit()
                retval = True
                log.info("Added {} {} {} {}".format(orig_table, orig_id, value, aspace_id))
        except sqlite3.Error as e:
            if str(e).startswith('UNIQUE constraint failed'):
                try:
                    with self.conn:
                        cursorObj.execute(UPDATE.format(aspace_id, value, orig_table, orig_id))
                        self.conn.commit()
                        retval = True
                        log.info("Updated {} {} {}".format(orig_table, value, aspace_id))
                except sqlite3.Error as e:
                    log.error("Couldn't even update: {} with sqlite3 error ".format(entities),error=e, exc_info=True )
            else:
                log.error("Problem adding {}: {}".format(entities),error=e)
        return retval
    
     
    def get_row(self, orig_table, orig_id):
        '''Returns the row corresponding to the original table/original ID mapping'''
        cursorObj = self.conn.cursor()
        row = None
        try:
            with self.conn:
                cursorObj.execute(FETCH_ROW, [orig_table, orig_id])
                row = cursorObj.fetchone() # index ensures uniqueness so this is sole result or None
        except sqlite3.Error as e:
            log.error("Unable to retrieve id for {}, {} with sqlite3 error".format(orig_table,orig_id), error=e)
    
        return row
    
    def get_aspace_id(self, orig_table, orig_id):
        ''' returns the ArchivesSpace URL corresponding to the original table/original ID mapping'''
        aspace_id = None
        
        row = self.get_row(orig_table, orig_id)
        if row:
            aspace_id = row['aspace_id']
        return aspace_id
    
    def drop_crosswalk(self):
        '''Drop the Crosswalk table all together'''
        cursorObj = self.conn.cursor()
        with self.conn:
            cursorObj.execute("DROP TABLE Crosswalk")
