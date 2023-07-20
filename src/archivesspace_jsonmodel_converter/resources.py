# Resources
from asnake.jsonmodel import JM
from psycopg.rows import dict_row

client = None
xw = None
conn = None
log = None

def process_resources(tablename):
    with conn.cursor(row_factory=dict_row) as cur:
        # get a count
        cur.execute(f'''SELECT COUNT(*) FROM "{tablename}"
                        WHERE collId IN (SELECT DISTINCT collId FROM tblItems WHERE deptcodeid=48) AND "coll##" IS NOT NULL''') # Deal with special "disposition colls"; select only Archives
        count = cur.fetchone()['count']
        log.info(f"Processing {count} resources in table {tablename}")
        for row in cur.execute(f'SELECT "coll##", collid, pre, "1st yr"::varchar, "last yr"::varchar, "collection title" FROM "{tablename}" WHERE collId IN (SELECT DISTINCT collId FROM tblItems WHERE deptcodeid=48)'):
            if not row['coll##']: continue # FIXME: we'll deal with missing coll##s later
            id_fields = {f'id_{idx}':segment for idx, segment in enumerate(row['coll##'].split('-'))}
            # Prepend prefix to id_0 to guarantee uniqueness
            id_fields['id_0'] = row['pre'] + id_fields['id_0']
            date_expr = ''
            if row['1st yr']:
                date_expr += str(row['1st yr'])
            if row['last yr']:
                date_expr += f'-{row["last yr"]}'
            if not date_expr:
                date_expr = 'undated'

            yield row['collid'], JM.resource(
                **id_fields,
                repository={"ref": "/repositories/2"},
                level="collection",
                finding_aid_language="eng",
                finding_aid_script="Latn",
                lang_materials=[
                    JM.lang_material(
                        language_and_script=JM.language_and_script(
                            language="eng",
                            script="Latn"
                        )
                    )
                ],
                extents=[
                    JM.extent(
                        portion="whole",
                        number="1",
                        extent_type="volumes"
                    )
                ],
                dates=[
                    JM.date(
                        begin=str(row['1st yr']) if row['1st yr'] else None,
                        end=str(row['last yr']) if row['last yr'] else None,
                        expression=date_expr,
                        date_type="range",
                        label="existence"
                    )
                ],
                title=row['collection title'],
                external_ids=[
                    JM.external_id(external_id=str(row['collid']), source="access")
                ]
            )

def resources_create(config, input_log):
    global client, xw, conn, log
    log = input_log
    client = config['d']['aspace']
    client.authorize()
    xw = config['d']['crosswalk']
    xw.create_crosswalk()
    conn = config["d"]["postgres"]
    tablename = "tblcolls"
    for orig_id, json in process_resources(tablename):
        aid = xw.get_aspace_id(tablename, orig_id)
        if aid is not None:
            rsc = client.get(aid).json()
            json['lock_version'] = rsc['lock_version']
            res = client.post(aid, json=json).json()
        else:
              res = client.post('repositories/2/resources', json=json).json()
        if 'status' in res and (res['status'] == 'Created' or res['status'] == 'Updated'):
            aspace_uri = res['uri']
            xw.add_or_update(tablename, orig_id, 'resource', aspace_uri)
            log.info(f'Added or updated {tablename} {orig_id}')
        elif 'error' in res:
            error = res['error']
            if 'conflicting_record' in res['error']:
                aspace_id = error['conflicting_record'][0]
                log.warn(f'Possible duplicate for ID {orig_id} in {tablename} found with URI of {aspace_id}')
                if 'source' in error:
                    error = error['source'][0]
                log.error(f"Error detected for ID {orig_id} in {tablename}",error=error)
        else:
            log.error(f"Collection {orig_id} in {tablename} not created for unknown reasons", error=res)
    
