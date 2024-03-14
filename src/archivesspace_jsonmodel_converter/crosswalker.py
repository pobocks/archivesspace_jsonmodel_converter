'''Supports the storage and retrieval of ID mapping between the origin system and ArchivesSpace'''

import sqlite3
import traceback
from os import path
from .logger import get_logger
import csv
import sys
log = get_logger('crosswalk')
ADD_NEW = 'INSERT INTO Crosswalk(orig_table, orig_id, value, aspace_id) VALUES(?, ?, ?, ?)'
UPDATE = 'UPDATE Crosswalk SET aspace_id=?, value=? WHERE orig_table=? AND orig_id=?'

UPSERT = """INSERT INTO Crosswalk(orig_table, orig_id, value, aspace_id) VALUES(?, ?, ?, ?) ON CONFLICT
            DO UPDATE SET value = excluded.value, aspace_id = excluded.aspace_id"""
FETCH_ROW = 'SELECT * FROM Crosswalk WHERE orig_table=? AND orig_id=?'
FETCH = 'SELECT aspace_id FROM Crosswalk WHERE orig_table=? AND orig_id=?'
FETCH_BY_AID = 'SELECT * FROM Crosswalk WHERE aspace_id=?'
FETCH_TABLE_CONTENTS = 'SELECT * FROM Crosswalk WHERE orig_table=?'

class Crosswalk():
    def __init__(self, config):
        db_path = path.join(config['working_directory'],
                            config['crosswalk_config']['name'] + '.sqlite')
        try:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            log.info(f"Crosswalk path is {db_path}")
        except sqlite3.Error as e:
            log.error('sqlite error', error=e)
            log.error(f'sqlite error: {traceback.format_exc(e)}')

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
                log.info(f"Added to {orig_table} [{orig_id}] [{value}] [{aspace_id}]")
        except sqlite3.Error as e:
            if str(e).startswith('UNIQUE constraint failed'):
                try:
                    with self.conn:
                        cursorObj.execute(UPDATE, [aspace_id, value, orig_table, orig_id])
                        self.conn.commit()
                        retval = True
                        log.info(f"Updated {orig_table} [{orig_id}] [{value}] [{aspace_id}]")
                except sqlite3.Error as e:
                    log.error(f"Couldn't even update: {entities} with sqlite3 error ",error=e, exc_info=True )
            else:
                log.error(f"Problem adding {entities}",error=e)
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
            log.error(f"Unable to retrieve id for {orig_table}, {orig_id} with sqlite3 error", error=e)

        return row
    
    def get_row_by_aspace_id(self, aspace_id):
        '''Returns the row corresponding to the aspace_id'''
        cursorObj = self.conn.cursor()
        row = None
        try:
            with self.conn:
                cursorObj.execute(FETCH_ROW, [aspace_id])
                row = cursorObj.fetchone() # index ensures uniqueness so this is sole result or None
        except sqlite3.Error as e:
            log.error(f"Unable to retrieve id for {aspace_id} with sqlite3 error", error=e)

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
    
    def fetch_xwtable(self,table):
        cursor = self.conn.cursor()
        try:
            with self.conn:
                cursor.execute(FETCH_TABLE_CONTENTS, table)
                for row in cursor:  
                    yield(row)  
        except sqlite3.Error as e:
            log.error(f"Problem accessing table {table} with error ",error=e, exc_info=True )

    def export_table(self, csv_file, table):
        ''' export a Crosswalk table to a csvfile'''
        try:
            with open(csv_file, "w", newline='', encoding='utf-8') as outfile:
                wr = csv.writer(outfile)
                for row in fetch_xwtable(table):
                    if row is not None:
                        wr.writerow(row)
        except Exception as e:
            log.error(f"Problem found in writing to {csv_file}: {e.__class__.__doc__} [{e.__class__.__name__}]")

'''called by main'''
def crosswalk_export(config,csv_file, table):
    xw = config["d"]["crosswalk"]
    xw.export_table(csv_file, table)
    
                    
            
        
    