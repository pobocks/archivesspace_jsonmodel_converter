# Archival objects
from asnake.jsonmodel import JM
from psycopg.rows import dict_row

client = None
xw = None
conn = None
log = None

def process_archival_objects(tablename, dept_id):
    with conn.cursor(row_factory=dict_row) as cur:
        # get a count
        cur.execute(f'''SELECT COUNT(*) FROM "{tablename}"
                        WHERE deptcodeid = {dept_id}''')
        count = cur.fetchone()['count']
        log.info(f"Processing {count} archival objects in table {tablename}")
        for row in cur.execute(f'''SELECT collid, itemid, itemname, itemdesc, datecreated'

def archival_objects_create(config, input_log):
