##### This is for Oracle Schema generation and comparision with another schama file.

**Usage:**

        python <file_name>.py tnsentry username passwd [OPTIONS]

**Options:**
    
        -o[=file_name]                  send results to file instead of stdout.
        -i[=in_file_name]               provide the input file for comparision purpose.
        -d[=db_file_name]               provide the db file for comparision purpose.
        -r[=results_file_name]          provide this file to store comparision results to this file. But, if not provided by default it will print the results to console.
        --compare-schemas               enable this flag for comparing the input and output files provided.
        --only-compare                  only compares two schema files without generating schema files from DB.

**Example:**

        ==> For generating the schema from the db :
        python oracle_schema.py 127.0.0.1:1521/dsnname usr pwd -o=out_db.txt

        ==> For generating the schema from the db and providing the comparision results at last:
        python oracle_schema.py 127.0.0.1:1521/dsnname usr pwd -o=out_db.txt -i=in_db.txt --compare-schemas

        ==> For only comparing the files without generating any schema files from db:
        python oracle_schema.py -d=out_db.txt -i=in_db.txt --only-compare

        ==> For comparing the files without generating any schema files and store the results to a results file
        python oracle_schema.py -d=out_db.txt -i=in_db.txt -r=results_file.txt --only-compare

# diff2HtmlCompare

A python script that takes two files and compares the differences between them (side-by-side) in an HTML format. Supports both python2 and python3.

### Installation
```
pip install -r requirements.txt
```

### Usage
```
diff2HtmlCompare.py [-h] [-s] [-v] file1 file2

positional arguments:
  file1       file to compare ("before" file).
  file2       file to compare ("after" file).

optional arguments:
  -h, --help  show this help message and exit
  -s, --show  show html in a browser.
  -v          show verbose output.
```
### Example Output