#!/usr/bin/env python
# -*- coding: utf8 -*-

__version__ = 'v1.0'

# Used for Generating the Schema from a Oracle DB
# and even used for comparing two schema files
#
# author: Naresh, Surisetty <naresh.surisetty@one.verizon.com>

OPTIONS = """[OPTIONS]
Options:
-o[=file_name]     		send results to file instead of stdout.
-i[=in_file_name]	  	provide the input file for comparision purpose.
-d[=db_file_name]		provide the db file for comparision purpose.
-r[=results_file_name]		provide this file to store comparision results to this file. But, if not provided by default it will print the results to console.
--compare-schemas		enable this flag for comparing the input and output files provided.
--only-compare			only compares two schema files without generating schema files from DB.
"""

USAGE = """Usage:
	python <file_name>.py tnsentry username passwd %s
Example:
	==> For generating the schema from the db :
	python oracle_schema.py 127.0.0.1:1521/dsnname usr pwd -o=out_db.txt

	==> For generating the schema from the db and providing the comparision results at last:
	python oracle_schema.py 127.0.0.1:1521/dsnname usr pwd -o=out_db.txt -i=in_db.txt --compare-schemas

	==> For only comparing the files without generating any schema files from db:
	python oracle_schema.py -d=out_db.txt -i=in_db.txt --only-compare

	==> For comparing the files without generating any schema files and store the results to a results file
	python oracle_schema.py -d=out_db.txt -i=in_db.txt -r=results_file.txt --only-compare

"""% OPTIONS


import codecs
import sys
import os
import os.path
import time
import re
try:
	import cx_Oracle
except:
	ex = sys.exc_info()
	serr = 'Exception: %s: %s' % (ex[0], ex[1])
	print serr
	print """\nModule: cx_Oracle not found. Please make sure the module is installed before using this program.
	This Module is used to connect and use this program with Oracle db.
	"""
	exit()
# import cx_Oracle

USE_JYTHON = 0

TABLES_ONLY = 0

SCHEMA_DIR = 'db_schema'
if '--date-dir' in sys.argv:
	SCHEMA_DIR += time.strftime("_%y%m%d_%H%M%S", time.localtime())
TABLES_INFO_DIR    = SCHEMA_DIR + '/tables'
VIEWS_INFO_DIR     = SCHEMA_DIR + '/views'
SEQUENCES_INFO_DIR = SCHEMA_DIR + '/sequences'
FUNCTIONS_INFO_DIR = SCHEMA_DIR + '/functions'
PROCEDURES_INFO_DIR = SCHEMA_DIR + '/procedures'
PACKAGES_INFO_DIR = SCHEMA_DIR + '/packages'
INVALID = '_invalid'

CREATED_FILES = []


DB_ENCODINGS = ('cp1250', 'iso8859_2', 'utf8')

OUT_FILE_ENCODING = 'UTF8'


TABLE_NAMES_SQL = """SELECT DISTINCT table_name
FROM user_tables
WHERE INSTR(table_name, 'X_') <> 1
AND INSTR(table_name, '$') = 0
AND NOT table_name IN (SELECT view_name FROM user_views)
AND NOT table_name IN (SELECT mview_name FROM user_mviews)
ORDER BY table_name
"""

TTABLE_COLUMNS = """SELECT column_name, data_type, nullable,
decode(default_length, NULL, 0, 1) hasdef,
decode(data_type,
	'DATE', '11',
	'NUMBER', data_precision || ',' || data_scale,
	data_length) data_length,
	data_default,
	char_length
FROM user_tab_columns
WHERE table_name='%s'
"""

TTABLE_COLUMNS_SQL = TTABLE_COLUMNS + " ORDER BY column_id "

TPRIMARY_KEYS_INFO_SQL = """SELECT ucc.column_name
FROM user_constraints uc, user_cons_columns ucc
WHERE uc.constraint_name = ucc.constraint_name
AND uc.constraint_type = 'P'
AND uc.table_name='%s'
"""


TFOREIGN_KEYS_INFO_SQL = """
SELECT uc.table_name, ucc.column_name, ucc.position
, fc.table_name, uic.column_position, uic.column_name
, uc.delete_rule, uc.constraint_name
FROM user_cons_columns ucc
,user_constraints fc
,user_constraints uc
,user_ind_columns uic
WHERE  uc.constraint_type = 'R'
AND    uc.constraint_name = ucc.constraint_name
AND    fc.constraint_name = uc.r_constraint_name
AND uic.index_name=fc.constraint_name
AND uc.table_name='%s'
ORDER BY uc.constraint_name, ucc.position, uic.column_position
"""


TINDEXES_COLUMNS_INFO_SQL = """SELECT uic.index_name, uic.column_name, ui.index_type, uie.column_expression, ui.uniqueness, uic.column_position
FROM user_ind_columns uic
LEFT JOIN (user_indexes ui) ON uic.index_name = ui.index_name
LEFT JOIN (user_ind_expressions uie) ON uic.index_name = uie.index_name
WHERE uic.table_name='%s'
ORDER BY uic.index_name, uic.column_position
"""



DB_VERSION_SQL = """SELECT * FROM v$version WHERE banner like 'Oracle%'"""


_CONN = None

CREATED_DIRS = []



RE_INVALID_FNAME = re.compile(r'[^a-z0-9\.\\/]')


def normalize_fname(fname):
	"""replaces to _ strange chars in filename te be created"""
	fname = fname.lower()
	fname = RE_INVALID_FNAME.sub('_', fname)
	return fname


def open_file_write(fname):
	"""opens file for writing in required encoding"""
	CREATED_FILES.append(fname)
	return codecs.open(fname, 'w', OUT_FILE_ENCODING)


def init_db_conn(connect_string, username, passwd):
	"""initializes database connection"""
	global _CONN
	if not _CONN:
		dbinfo = connect_string
		try:
				dbinfo = 'db: %s@%s' % (username, connect_string)
				print('--%s' % (dbinfo))
				_CONN = cx_Oracle.connect('%s/%s@%s' % (username, passwd, connect_string))
		except:
			ex = sys.exc_info()
			serr = 'Exception: %s: %s\n%s' % (ex[0], ex[1], dbinfo)
			print_err(serr)
			return None
	return _CONN


def db_conn():
	"""returns global database connection"""
	return _CONN


def output_str(fout, line):
	"""outputs line to fout trying various encodings in case of encoding errors"""
	if fout:
		try:
			fout.write(line)
		except (UnicodeDecodeError, UnicodeEncodeError):
			try:
				fout.write(line.encode(OUT_FILE_ENCODING))
			except (UnicodeDecodeError, UnicodeEncodeError):
				ok = 0
				for enc in DB_ENCODINGS:
					try:
						line2 = line.decode(enc)
						#fout.write(line2.encode(OUT_FILE_ENCODING))
						fout.write(line2)
						ok = 1
						break
					except (UnicodeDecodeError, UnicodeEncodeError):
						pass
				if not ok:
					fout.write('!!! line cannot be encoded !!!\n')
					fout.write(repr(line))
		fout.write('\n')
		fout.flush()


def output_line(line, fout=None):
	"""outputs line"""
	line = line.rstrip()
	output_str(fout, line)
	output_str(sys.stdout, line)


def print_err(serr):
	"""println on stderr"""
	sys.stderr.write('%s\n' % (serr))


def select_qry(querystr):
	"""executes SQL SELECT query"""
	cur = db_conn().cursor()
	cur.execute(querystr)
	results = cur.fetchall()
	cur.close()
	return results


def run_qry(querystr):
	"""executes SQL update/insert etc"""
	cur = db_conn().cursor()
	cur.execute(querystr)
	cur.close()

def init_session():
	"""initialization of SQL session"""
	run_qry("ALTER SESSION SET nls_numeric_characters = '.,'")


def get_type_length(data_type, data_length, char_length):
	"""get string with length of field"""
	if data_type == 'NUMBER':
		if data_length == ',':
			return ''
		if data_length == ',0':
			return '(*,0)'
		return '(%s)' % (data_length)
	if data_type == 'RAW':
		return ' (%s)' % (data_length)
	if data_type in ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2'):
		return ' (%.0f)' % (char_length)
	return ''


def table_info_row(row):
	"""shows info about table column"""
	column_name = row[0]
	data_type = row[1]
	nullable = row[2]
	hasdef = row[3]
	data_length = row[4]
	data_default = row[5]
	char_length = row[6]
	default_str = nullable_str = ''
	data_length_str = get_type_length(data_type, data_length, char_length)
	if int(hasdef) == 1:
		default_str = ' DEFAULT %s' % (data_default)
	if nullable == 'N':
		nullable_str = ' NOT NULL'
		if default_str.endswith(' '):
			nullable_str = 'NOT NULL'
	if column_name.startswith('_'):
		column_name = '"' + column_name + '"'
	else:
		column_name = column_name.lower()
	return '%(column_name)s %(data_type)s%(data_length)s%(default)s%(nullable)s' % {'column_name': column_name, 'data_type': data_type, 'data_length': data_length_str, 'nullable': nullable_str, 'default': default_str}


def get_table_indices(table, pk_columns=None):
	"""returm table indices"""
	indices_str = ''
	indices = {}
	rs = select_qry(TINDEXES_COLUMNS_INFO_SQL % (table))
	idx_uniques = {}
	for row in rs:
		idx_name = row[0]
		if idx_name.startswith('SYS_'):
			continue
		idx_column = row[1]
		idx_type = row[2]
		idx_expression = row[3]
		idx_unique = row[4]
		if idx_unique != 'UNIQUE':
			idx_unique = ''
		idx_uniques[idx_name] = idx_unique
		if idx_type == 'FUNCTION-BASED NORMAL':
			idx_column = idx_expression
		try:
			indices[idx_name].append(idx_column)
		except KeyError:
			indices[idx_name] = [idx_column, ]
	if indices:
		pk_columns_str = ''
		if pk_columns:
			pk_columns_str = ', '.join(pk_columns).lower()
		idxs = indices.keys()
		idxs.sort()
		idx_lines = []
		for idx in idxs:
			columns_str = ', '.join(indices[idx]).lower()
			if columns_str != pk_columns_str:
				idx_lines.append('CREATE %s INDEX %s ON %s (%s);' % (idx_uniques[idx], idx, table, ', '.join(indices[idx])))
		indices_str = '\n'.join(idx_lines)
	return indices_str


def add_primary_key_ddl(table, lines_ct):
	"""adds information about primary key columns"""
	rs = select_qry(TPRIMARY_KEYS_INFO_SQL % (table))
	pk_columns = []
	for row in rs:
		pk_columns.append(row[0].lower())
	if pk_columns:
		tmp_str = 'PRIMARY KEY (%s)' % (', '.join(pk_columns))
		lines_ct.append(tmp_str)
	return pk_columns


def get_foreign_keys_dict(table):
	"""returns dictionary with info about foreign keys"""
	fk = {}
	rs = select_qry(TFOREIGN_KEYS_INFO_SQL % (table))
	for row in rs:
		_, cn1, _, tn2, _, cn2, dr, cn = row
		try:
			_ = fk[cn][0]
			_ = fk[cn][2]
		except KeyError:
			fk[cn] = [[cn1, ], [tn2, ], [cn2, ], [dr, ]]
	return fk

def add_foreign_key_ddl(table, lines_ct):
	"""adds information about foreign keys"""
	cnt = 0
	rs = select_qry("""SELECT COUNT(*)
		FROM user_constraints
		WHERE constraint_type = 'R' AND table_name='%s'""" % (table))
	for row in rs:
		cnt = int(row[0])
	if cnt > 0:
		fk = get_foreign_keys_dict(table)
		if fk:
			fkk = fk.keys()
			fkk.sort()
			for cn in fkk:
				columns1 = fk[cn][0]
				table2 = fk[cn][1][0]
				columns2 = fk[cn][2]
				dr = fk[cn][3][0]
				if dr == 'CASCADE':
					dr = 'ON DELETE CASCADE'
				else:
					dr = ''
				tmp_str = 'CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) %s ENABLE' % (
				cn, ','.join(columns1), table2, ','.join(columns2), dr)
				lines_ct.append(tmp_str)

def create_create_table_ddl(table):
	"""creates DDL with CREATE TABLE for table"""
	# gets information about columns
	rs = select_qry(TTABLE_COLUMNS_SQL % (table))
	lines_ct = []
	for row in rs:
		lines_ct.append(table_info_row(row).strip())

	pk_columns = add_primary_key_ddl(table, lines_ct)
	add_foreign_key_ddl(table, lines_ct)

	# creates DDL CREATE TABLE instruction
	#- \n, is required when column has comment
	ct = 'CREATE TABLE %s (\n\t %s\n);' % (table.lower(), '\n\t,'.join(lines_ct))
	indices_str = get_table_indices(table, pk_columns)
	# triggers_str = get_table_triggers(table)
	return '%s\n\n%s' % (ct, indices_str)
	# return '%s\n\n%s\n\n%s' % (ct, indices_str, triggers_str)

def save_table_definition(table):
	"""saves DDL in a file"""
	s = create_create_table_ddl(table)
	fname = os.path.join(TABLES_INFO_DIR, '%s.sql' % (normalize_fname(table)))
	f = open_file_write(fname)
	output_line(s, f)
	f.close()
	return 1


def dump_db_info( out_f, stdout):
	"""saves information about database schema in file/files"""
	rs = select_qry(TABLE_NAMES_SQL)
	if rs:
		for row in rs:
			table = row[0]
			save_table_definition(table)
	output_line('\n\n--- the end ---')
	if out_f:
		out_f.close()
		sys.stdout = stdout


def get_option_value(prefix):
	"""returns FILENAME for -o prefix and -oFILENAME or -o=FILENAME"""
	result = None
	for s in sys.argv:
		if s.startswith(prefix):
			result = s[len(prefix):]
			if result.startswith('='):
				result = result[1:]
	return result

def strip_lists(str_list):
	start_indexs, end_indexs = [i for i in range(0, len(str_list)) if 'table' in str_list[i]], [i for i in range(0, len(str_list)) if ');' in str_list[i]]
	string_cat_list = []
	for i in range(0,len(end_indexs)):
		table_item = str_list[start_indexs[i]]
		[string_cat_list.append(table_item+'.'+str_list[i]) for i in range(start_indexs[i],end_indexs[i]) if 'table' not in str_list[i]]
	return string_cat_list

def strip_query(data_file):
	# str = ddl_query.splitlines()
	with data_file:
		str = data_file.readlines()
	tmp_str = []
	for str_single in str:
		if str_single.strip():
			if 'create' in str_single.strip(): tmp_str.append(str_single.strip().split(' ')[1] + '.' + str_single.strip().split(' ')[2].split('(')[0])
			elif str_single.strip().split(' ')[0] not in ['constraint', 'primary key']: tmp_str.append(str_single.strip().split(' ')[0])
	tmp_str = strip_lists(tmp_str)
	# print tmp_str
	return tmp_str

def schema_compare(in_f,out_f):
	cmp1 = strip_query(in_f)
	# print 'INPUT_FILE: %s' % (cmp1)
	cmp2 = strip_query(out_f)
	# print 'DB_FILE: %s' % (cmp2)
	s = set(cmp2)
	results = [x for x in cmp1 if x not in s]
	# print 'RESULTS: %s' %(results)
	return  results

def main():
	"""main function"""

	if '--only-compare' in sys.argv:
		in_f = None
		db_f = None
		in_fn = get_option_value('-i')
		try:
			in_f = open(in_fn, 'r')
		except:
			print(USAGE)
			return 0
		db_fn = get_option_value('-d')
		try:
			db_f = open(db_fn, 'r')
		except:
			print(USAGE)
			return 0
		results = schema_compare(in_f,db_f)
		res_f = None
		res_fn = get_option_value('-r')
		try:
			res_f = open(res_fn, 'w')
			for i in range(0, len(results)):
				res_f.write('%s) 	%s\n' % (i+1,results[i]))
		except:
			print 'RESULTS: %s' %(results)
		return 0

	stdout = sys.stdout
	out_f = None
	out_fn = get_option_value('-o')
	try:
		out_f = open(out_fn, 'w')
	except:
		print (USAGE)
		return 0
	sys.stdout = out_f
	CREATED_FILES.append(out_fn)

	conn_args = [s for s in sys.argv[1:] if not s.startswith('-')]
	if len(conn_args) != 3:
		print(USAGE)
		return 0
	connect_string, username, passwd = conn_args

	if not init_db_conn(connect_string, username, passwd):
		print_err('Something is terribly wrong with db connection')
		return 0
	init_session()
	dump_db_info( out_f, stdout)

	if '--compare-schemas' in sys.argv:
		in_f = None
		db_f = None
		in_fn = get_option_value('-i')
		try:
			in_f = open(in_fn, 'r')
		except:
			print(USAGE)
			return 0
		db_fn = get_option_value('-d')
		try:
			db_f = open(db_fn, 'r')
		except:
			print(USAGE)
			return 0
		results = schema_compare(in_f, db_f)
		res_f = None
		res_fn = get_option_value('-r')
		try:
			res_f = open(res_fn, 'w')
			for i in range(0, len(results)):
				res_f.write('%s) 	%s\n' % (i + 1, results[i]))
		except:
			print 'RESULTS: %s' % (results)


if __name__ == '__main__':
	main()