# Subjects
from asnake.aspace import ASpace
# Bring in the client to work at a very basic level.
from asnake.client import ASnakeClient
from asnake.jsonmodel import JM
import psycopg
import sys
import re
from configparser import ConfigParser
import crosswalk_reader as xw

# Create and authorize the client
client = ASnakeClient()
client.authorize()

#need from tblLcshs and tblGeoPlaces
pattern =  "\|([a-z])"

def config(filename='database.ini', section='postgresql'):
	# create a parser
	parser = ConfigParser()
	# read config file
	parser.read(filename)

	# get section, default to postgresql
	db = {}
	if parser.has_section(section):
		params = parser.items(section)
		for param in params:
			db[param[0]] = param[1]
	else:
		raise Exception('Section {0} not found in the {1} file'.format(section, filename))

	return db

def clean_up(conn):
	if conn is not None:
		conn.close()
		print('Database connection closed.')

def get_connection():
	conn = None
	try:
		# read connection parameters
		params = config()
		# connect to the PostgreSQL server
		print('Connecting to the PostgreSQL database...')
		conn = psycopg.connect(**params)
	except (Exception, psycopg.DatabaseError) as error:
		print(error)
		clean_up(conn)
			
	return conn

def connect():
	""" Connect to the PostgreSQL database server """
	conn = None
	try:
		# read connection parameters
		params = config()

		# connect to the PostgreSQL server
		print('Connecting to the PostgreSQL database...')
		conn = psycopg.connect(**params)
		
		# create a cursor
		cur = conn.cursor()
		
	# execute a statement
		print('PostgreSQL database version:')
		cur.execute('SELECT version()')

		# display the PostgreSQL database server version
		db_version = cur.fetchone()
		print(db_version)
		
	# close the communication with the PostgreSQL
		cur.close()
	except (Exception, psycopg.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')

def add_to_aspace(orig_id, subject):
	''' Add a subject to ArchivesSpace'''
	new_id = None
	response = client.post('subjects', json=subject).json()
	if 'status' in response and response['status'] == 'Created':
		new_id = response['id']	
	elif 'error' in response:
		err = response['error']
		# treat a conflicting record as a win
		if 'conflicting_record' in err:
			rec = err['conflicting_record'][0]
			new_id = rec.split('/')[-1]
			print("{} Already exists as {}".format(orig_id, new_id))
		else:
			# look for a reason for the error
			error = err
			if 'source' in err:
				error = err['source'][0]
			print("Error detected for ID {}: {}".format(orig_id, error))
	else:
		print("Item {} not created for unknown reasons: {}".format(orig_id, response))	
	return new_id

def create_terms(subject):
	'''creates a list of terms in jsonmodel format'''
	subfield_dict = {"a": "topical", "b": "topical", "c" : "geographic", "d": "temporal", "v": "genre_form", "x": "topical", "y": "temporal", "z": "geographic"} # HARDCODED DICTIONARY.
	trmlist = re.split(pattern,subject)
	trmlist.insert(0, 'a')  # we're assuming the first entry is 'topical'
	term_dict  = map(lambda i: (trmlist[i], trmlist[i+1]), range(len(trmlist)-1)[::2])
	terms = []
	for sub,term in term_dict:
		try:
			entry = JM.term(vocabulary="/vocabularies/1", term_type=subfield_dict[sub], term= term)
			terms.append(entry)
		except Exception as e:
			raise e
	return terms


def create_subject_json(orig_subj):
	terms = create_terms(orig_subj)
	subject = JM.subject(publish="true", source="lcsh",vocabulary="/vocabularies/1", terms=terms)
	return subject

def process_lcshs(xwalk_file_path):
	''' Take the LCSH values and add them to ArchivesSpace '''
	# create a crosswalk csv
	lcshs_xw = xw.CrosswalkReader('LCSHS',xwalk_file_path, True)
	conn = get_connection()
	if conn is None:
		return None
	try:
		# create a cursor
		cur = conn.cursor()
		cur.execute('SELECT * from tblLcshs')
		while True:
			row = cur.fetchone()
			if row == None or len(row) < 2:
				break
			orig_id = row[0]
			orig_val = row[1]
			try:
				subject = create_subject_json(orig_val)
				new_id = add_to_aspace(orig_id, subject)
				if new_id is not None:
					lcshs_xw.add(orig_id, orig_val, new_id)
				#TBD: what do we do with None new_ids?
				else:
					print("{} '{}' was not converted".format(orig_id, orig_val))
			except Exception as e:
				print("Exception '{}' triggered on {} '{}', which will not be converted".format(e, orig_id, orig_val))
			finally:
				continue
	except Exception as e:
		print(e)
		print(sys.exc_info()[2])
	finally:
		clean_up(conn)
		lcshs_xw.write_out()


if __name__ == '__main__':
	#connect()
	process_lcshs('c:/Users/rlynn/aspacelinux/temp/lcshs.csv')
