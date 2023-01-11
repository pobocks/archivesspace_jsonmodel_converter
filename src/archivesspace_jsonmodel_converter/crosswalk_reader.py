

import csv
from csv import reader

class CrosswalkReader:
    ''' Read in a csv to create a crosswalk of IDs between the original system and ArchivesSpace'''
    # this presumes a CSV with three columns:  Original ID, the Value, and the Aspace ID
    # the first row after the header contains a human-readable string identifying the type of crosswalk in the second column

    HEADERS = ['orig_id','value','aspace_id']


    def __init__(self, type, filepath, create = False):
        self.crosses = {}
        self.type = type
        self.filepath = filepath
        if create:
            try:
                with open(filepath, 'w',  encoding='UTF-8') as file:
                    writer =  csv.writer(file)
                    writer.writerow(self.HEADERS)
                    writer.writerow(['',type,''])
            except Exception as e:
                raise Exception("Problem creating file '{}'\n{}").format(type, e)
            finally:
                file.close()
        else:
            # we have a file, and we're trying to read it
            try:
                with open(filepath, 'r') as f:
                    csv_reader = csv.reader(f)
                    header = next(csv_reader)
                    try: 
                        for h in self.HEADERS:
                            if h not in header:
                                raise Exception("Badly formed or missing header {}".format(header))
                        title = next(csv_reader)
                        while title == []:
                            title = next(csv_reader)
                        if title[1].strip() != type:
                            raise Exception("File is not '{}'".format(self.type) )
                    except Exception as e:
                        raise e
                    finally:
                        f.close()
            except Exception as e:
                raise e
        
            # opening the CSV file for read
            with open(filepath, mode ='r') as file:  
                # reading csv file
                csv_dict = csv.DictReader(file)
                for lines in csv_dict:
                    if lines["orig_id"] == '':
                        continue
                    self.crosses[lines["orig_id"]] = {
                        'orig_id':lines["orig_id"],
                        'value': lines["value"].strip(),
                        'aspace_id': lines['aspace_id']
                    }
            file.close()

    def aspace_id(self,orig_id):
        ''' Retrieve the ArchivesSpace ID based on the original ID '''
        try:
            ret_val = self.crosses[orig_id]['aspace_id']
            return ret_val if ret_val else None
        except KeyError as k:
            print("Original ID {} does not exist".format(orig_id))
            raise k

    def pretty(self,orig_id):
        ''' Present a human readable version of the entry based on the original ID'''
        ret = 'Original ID {} is not found'
        try:
            hash = self.crosses[orig_id]
            ret = "Original ID {}: '{}' maps to ArchiveSpace ID ({}) ".format(orig_id, hash['value'],  hash['aspace_id'])
        except KeyError:
            return(ret)
        return(ret)

    def add(self, orig_id, value, aspace_id):
        ''' add a hash to the list of crosses '''
        if orig_id in self.crosses:
            raise Exception("This id ({}) already has been defined as {}".format(orig_id, pretty(orig_id)))
        self.crosses[orig_id] = {
            'orig_id': orig_id,
            'value': value.strip(),
            'aspace_id': aspace_id
        }
    def write_out(self):
        ''' write out all the rows to the csv file '''
        try:
            with open(self.filepath, 'a',  encoding='UTF-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                for key, row in self.crosses.items():
                    writer.writerow(row)
        except Exception as e:
            raise Exception("Problem writing to file '{}'\n\t{}".format(self.filepath, e))
        finally:
            f.close()

