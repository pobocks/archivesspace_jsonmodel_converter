# Archival objects
from asnake.jsonmodel import JM
from psycopg.rows import dict_row

client = None
xw = None
conn = None
log = None

# Each AO needs:
#   to be associated with its resource on creation
#   that resource to have its link added to its instances
#   TODO to be associated with its accession on creation
#   TODO that accession to be added to its instances

# Required AO Fields:
#  THEORETICALLY only title, resource, and level
def process_archival_objects(tablename, resource_tablename, dept_id):
    with conn.cursor(row_factory=dict_row) as cur:
        # get a count
        cur.execute(f'''SELECT COUNT(*) FROM "{tablename}"
                        WHERE deptcodeid = {dept_id}''')
        count = cur.fetchone()['count']
        log.info(f"Processing {count} archival objects in table {tablename}")
        for row in cur.execute(
                f'''SELECT collid, itemid, title, itemdesc
                    FROM {tablename}
                    WHERE collid IS NOT null
                      AND deptcodeid = {dept_id}
                 ORDER BY collid ASC, datecreated ASC'''):
            aid = xw.get_aspace_id(resource_tablename, row["collid"])
            if aid is None:
                log.error(f"Resource with original collid of {row['collid']} missing from crosswalk for item {row['itemid']}, skipping")
                continue
            yield row['collid'], row['itemid'], JM.archival_object(
                component_id=row['itemid'],
                title=row['title'],
                level="item",
                resource={"ref":aid},
                external_ids=[
                    JM.external_id(external_id="COLLID: " + str(row['collid']), source="access"),
                    JM.external_id(external_id=str(row['itemid']), source="access")
                ],
                publish=True
            )

good_statii = frozenset(('Created', 'Updated',))
def archival_objects_create(config, input_log):
    global client, xw, conn, log
    log = input_log
    client = config['d']['aspace']
    client.authorize()
    xw = config['d']['crosswalk']
    xw.create_crosswalk()
    conn = config["d"]["postgres"]
    tablename = 'tblitems'
    resource_tablename = 'tblcolls'
    dept_id = 48
    for collid, orig_id, json in process_archival_objects(tablename, resource_tablename, dept_id):
        ao_id = xw.get_aspace_id(tablename, orig_id)
        if ao_id is not None:
            ao = client.get(ao_id).json()
            json['lock_version'] = ao['lock_version']
            res = client.post(ao_id, json=json).json()
        else:
            res = client.post('repositories/2/archival_objects', json=json).json()
        if 'status' in res and res['status'] in good_statii:
            aspace_uri = res['uri']
            xw.add_or_update(tablename, orig_id, 'archival_object', aspace_uri)
            log.info(f'Added or updated {tablename} {orig_id}')
        elif 'error' in res:
            error = res['error']
            if 'conflicting_record' in error:
                aspace_id = error['conflicting_record'][0]
                log.warn(f'Possible duplicate for ID {orig_id} in {tablename} found with URI of {aspace_id}')
            if 'source' in error:
                error = error['source'][0]
                log.error(f"Error detected for ID {orig_id} in {tablename}",error=error)
        else:
            log.error(f"Collection {orig_id} in {tablename} not created for unknown reasons", error=res)
