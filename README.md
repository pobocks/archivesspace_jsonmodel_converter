# ArchivesSpace JSONModel Converter

## Installation

Navigate to the top level of this repository, and run:

```bash
pip install -e .
```

After installation, the tool should be available as `ajc`, running `ajc` will emit a help page describing any extant subcommands.

## Configuration

AJC is configured via a YAML file. Some values are hardcoded as defaults and can be omitted - currently, these include:

- `asnake_config` - ASnake configuration to be passed to the client
- `postgres_config` - Configuration values to be passed directly to psycopg
- `crosswalk_config` - Config for the Crosswalk class in this application. Currently only supports `name`, which is the name of a sqlite3 database.

By default, AJC looks for a `.archivesspace_jsonmodel_converter.yml` in the user's home directory.  A different file location can be specified via the AJC_CONFIG_FILE variable, or by providing a path to the `ajc` commend via the `--config-file` option.



## Conversion of Access DB to postgresql DB

For various reasons, it's more convenient to move the data from access to another data format before trying to convert it to ASpace JSONModel objects.  For this example, we will convert the data to a postgresql-compatible SQL file.  Additionally, we will deal with some issues specific to the Fort Lewis DB but that will commonly occur when converting data from MDB format.

## Tools
The tools we'll be using are:

- [mdbtools](https://github.com/mdbtools/mdbtools) - a set of command line tools for accessing and working with Access DBs
- a Linux shell with mdbtools installed
- psql
- a text editor of the user's choice

## Process
### Step 1
First, we need to create an SQL script that represents the access DB's schema.  We do this with the following commend:
```bash
mdb-schema my_access_file.mdb postgres > my_postgres_schema.sql
```

### Step 2
Next, we need to add insert statements for all the tables in the schema.  This can be done via a shell loop, thusly:
```bash
for name in `mdb-tables my_access_file.mdb`; do mdb-export -I postgres my_access_file.mdb $name >> my_postgres_schema.sql; done
```

At this point, we have a schema with all tables, constraints, and data from the Access db - if we're _very lucky_, this might just import cleanly into postgres at this point.

### Step 3
Assuming postgres is already set up and the user account you're logged in as can create databases:
```bash
createdb my_access_db
psql my_access_db < my_postgres_schema.sql &> psql_import_output.txt
```

After this completes, check the input - there's a good chance you'll see some errors - Postgres (and MySQL, and most things for that matter) are more strict about foreign keys and indexes than Access, and don't let you do things like embed a whole word document as a column value. If there are errors, open the SQL file in a text editor that supports large files, correct the errors or drop the overly strict indexes, and then drop and recreate the table and run Step 3 again. When it runs without errors, you've successfully migrated the data.
