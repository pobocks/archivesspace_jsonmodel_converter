﻿
# Technical Approach

The current home-grown system was developed in Microsoft **Access**(tm).  We have decided to convert that system _as-is_ into **PostgreSQL** as a first step.   We then run our **python**-based conversion scripts from there,  taking advantage of the [ArchivesSnake](https://github.com/archivesspace-labs/ArchivesSnake/) package that uses the ArchivesSpace API.

## Converting the database to PostgreSQL

We have documented the  [steps for converting from the **Access** database to **PostgreSQL**](db_conversion.md).


## Creating data crosswalks

Because the structure of the home-grown database is quite different from that of ArchivesSpace, we are creating a separate database that maps data in the original system with analogous data in ArchivesSpace.

An immediate example is the controlled value lists of ArchivesSpace; _e.g._ for the _Linked Agent Archival Record Relators_ list in ArchivesSpace, we need to identify mapping from the **tblLookupValues** for "Creator Type" with the appropriate **enum** in the list.

Further, as we develop processing of disparate parts of the archive, such as _subjects_, we want to keep track of the analogous IDs in ArchivesSpace as we associate those with other parts, such as _resources_, to reduce lookup speeds.

We have created a **Crosswalk** database for this purpose, using [SQLite](https://www.sqlite.org/index.html), which is available with the Python installation.

### Crosswalk database

Currently this database contains only the **Crosswalk** table:

|  Column | Data Type | comment |
| :--------- | :---------- | :---------- |
| id | integer | |
| orig_table | text | 'enums' for enumerator, 'Names' for the Creator and itemNames crosswalk; 'agent' for data from the Creator/places and itemNames* table; otherwise the Access table name |
| orig_id | text | the ID in the originating table * |
| value | text | the text value in the originating table * |
| aspace_id | text | for enumerators, the enum value; otherwise the full ArchivesSpace URL* |
| misc | text | an additional information field for some tables* |

All interactions with the database are managed by the  **Crosswalk** class defined in [crosswalk.py](../src/archivesspace_jsonmodel_converter/crosswalker.py)

### * Special Handling for 'Names' and 'agents'

While the same Crosswalk table is used for the 'Names' and 'agents' tables, the columns are repurposed as described below.

## Individual Conversions

### Enumerations (_tblLookupValues)_

Most of the values in  **tblLookupValues** will just be used as free text within the conversion, if at all.  For example, "Cultural Affiliation" values will be entered as part of a note on the Resource or Accession.  Others may require expanding the enumeration values in ArchivesSpace. There are some lookup types, however, that just need "translation" into already-defined ArchivesSpace controlled value lists.  For these last types, a [values](../values2enums.csv) spreadsheet in has been created, which is processed by [enumerations.py](../src/archivesspace_jsonmodel_converter/enumerations.py)

### Subjects

Three Access tables are processed to be converted into ArchivesSpace subjects: **tblLcshs**, **tblGeoPlaces**, and **tblCreatorPlaces**.  The conversion is handled in 
[subjects.py](../src/archivesspace_jsonmodel_converter/subjects.py).

### Agents

Creators and itemNames do not have their own table in the Access database.  We are using the **tblcreator/place**, **tblItemNamesCorp**, and **tblItemNamesPers** tables, filtering by looking up the item_id in the **tblItems** crosswalk table.

Because the creator and agent names were entered free-hand, a lot of typos, duplicate-but-not-quite names, etc. entered into the system.  An archivist reviewed all the names and created a CSV (Comma Separated Values) file, **ORIG_ID**, **VALUE**, and **MISC** (which has one of three values: 'P' (person), 'C' (company), or 'F' (family) ) . 

[name_xwalk.py](../src/archivesspace_jsonmodel_converter/name_xwalk.py) processes this file; if the **VALUE** column is empty, it assigns the value in the **ORIG_ID** column.  This process populates the 'Names' entries in Crosswalk, where, _orig_id_ is where the name as found in the Access table is placed,  _value_ contains the converted name, and _misc_ determines what kind of ArchivesSpace agent is created

This script must be run before [agents.py](../src/archivesspace_jsonmodel_converter/agents.py)

If more names are added, this script can be run again, followed by re-running [agents.py](../src/archivesspace_jsonmodel_converter/agents.py)

The agent name is then found in the 'Names' entries to find the converted name.  When an agent is created in ArchivesSpace, an entry is created in the 'agents' table in the Crosswalk, with _orig_id_ and _value_ containing the "converted" name, and _aspace_id containing the URI.


### Resources
































