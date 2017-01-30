#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = 'v1.0'

# Used for Comparing the Schema across two schema files
#
# author: Naresh, Surisetty <naresh.surisetty@one.verizon.com>

import sqlparse
import re
from collections import OrderedDict


def extract_table_definitions(token_list):
    # assumes that token_list is a parenthesis
    definitions = []
    tmp = []
    # grab the first token, ignoring whitespace. idx=1 to skip open (
    try:
        tidx, token = token_list.token_next(1)
    except:
        print token_list
        return definitions
    while token and not token.match(sqlparse.tokens.Punctuation, ')'):
        tmp.append(token)
        # grab the next token, this times including whitespace
        tidx, token = token_list.token_next(tidx, skip_ws=True)
        # split on ",", except when on end of statement
        if token and token.match(sqlparse.tokens.Punctuation, ','):
            definitions.append(tmp)
            tmp = []
            tidx, token = token_list.token_next(tidx)
    if tmp and isinstance(tmp[0], sqlparse.sql.Identifier):
        definitions.append(tmp)
    return definitions

def format_ddls_using_regex(SQL):
    SQL = re.sub('\(\d+.*?\)', '', SQL)
    SQL = re.sub("(DEFAULT\s+'\s+'\s)", '', SQL)
    SQL = re.sub('(NOT NULL)', 'NOT_NULL', SQL)
    SQL = re.sub('(PRIMARY KEY)', 'PRIMARY_KEY', SQL)
    SQL = re.sub('(FOREIGN KEY)', 'FOREIGN_KEY', SQL)
    SQL = re.sub('\);', ',);', SQL)
    SQL = re.sub('(PRIMARY_KEY\s+\()', 'PRIMARY_KEY ', SQL)
    SQL = re.sub('(FOREIGN_KEY\s+\()', 'FOREIGN_KEY ', SQL)
    SQL = re.sub('\),', ',', SQL)
    SQL = re.sub('\)\s+REFERENCES', ' REFERENCES', SQL)
    SQL = re.sub('(ASC)', '', SQL)
    return SQL

def extract_table_comp_info(parsed_sql):
    _, par = parsed_sql.token_next_by(i=sqlparse.sql.Parenthesis)
    rows = extract_table_definitions(par)
    table_name = 'TABLE.' + str(parsed_sql.get_name()) + '.'
    return_list = []
    for row in rows:
        column_string = ''
        for column in row:
            column_string = column_string + str(column) + '.'
        column_string = column_string.replace("KEY ", "KEY.")
        if ',' in column_string:
            for item in column_string.split(','):
                if item[-1] != '.':
                    # print table_name + item.strip() + '.'
                    return_list.extend([str(table_name + item.strip() + '.')])
                else:
                    # print table_name + item.strip()
                    return_list.extend([str(table_name + item.strip())])
        else:
            # print table_name + column_string
            return_list.extend([str(table_name + column_string)])
    return return_list

def extract_index_comp_info(parsed_sql):
    return_list = []
    index, index_name = parsed_sql.token_next_by(i=sqlparse.sql.Identifier)
    def_index, definition = parsed_sql.token_next_by(i=sqlparse.sql.Function)
    # print 'INDEX.' + str(index_name) + '.ON.' + str(parsed_sql.get_name()) + '.' + \
    #       re.findall(r'(?<=\().*?(?=,\))', str(definition))[0].strip() + '.'
    return_list.extend(['INDEX.' + str(index_name) + '.ON.' + str(parsed_sql.get_name()) + '.' + re.findall(r'(?<=\().*?(?=,\))', str(definition))[0].strip() + '.'])
    return return_list

def main_generator(SQL1,SQL2):
    SQL1 = format_ddls_using_regex(SQL1)
    SQL2 = format_ddls_using_regex(SQL2)
    # refs = re.findall(r'(?<=REFERENCES\s).*?(?=\s+\()', SQL)
    # for ref in refs:
    #     print ref
    #     SQL = re.sub('(REFERENCES\s+\w+\s+\(+\w+\))','REFERENCES '+ref,SQL)
    # print SQL
    f1 = lambda sql: sqlparse.format(sql, strip_whitespace=True)
    f2 = lambda sql: sqlparse.format(sql, strip_whitespace=True)
    # print f(SQL)
    parsed1 = sqlparse.parse(f1(SQL1))
    parsed2 = sqlparse.parse(f1(SQL2))
    # table_list = []
    # index_list = []
    # for parsed_sql in parsed:
    #     if 'TABLE' in str(parsed_sql):
    #         table_list.extend(extract_table_comp_info(parsed_sql))
    #     elif 'INDEX' in str(parsed_sql):
    #         index_list.extend(extract_index_comp_info(parsed_sql))
    # print parsed
    table1_dict = {}
    index1_dict = {}
    for item in sorted(parsed1):
        if 'TABLE' in str(item):
            table1_dict[str(item.get_name())]=item
        elif 'INDEX' in str(item):
            index1_dict[str(item.get_name())]=item
    print table1_dict
    print index1_dict

    table2_dict = {}
    index2_dict = {}
    for item in sorted(parsed2):
        if 'TABLE' in str(item):
            table2_dict[str(item.get_name())] = item
        elif 'INDEX' in str(item):
            index2_dict[str(item.get_name())] = item
    print table2_dict
    print index2_dict

    file1 = open('tmp_in_file','w')
    file2 = open('tmp_out_file','w')

    keys_a = set(table1_dict.keys())
    keys_b = set(table2_dict.keys())
    intersection = keys_a & keys_b
    print intersection

    diff_int1 = keys_a - intersection
    diff_int2 = keys_b - intersection
    # print diff_tables1
    common_tables1 = []
    common_tables2 = []
    diff_tables1 = []
    diff_tables2 = []

    for item in intersection:
        value = table1_dict[item]
        common_tables1.append(value)
        value = table2_dict[item]
        common_tables2.append(value)
    for item in diff_int1:
        value = table1_dict[item]
        common_tables1.append(value)
    for item in diff_int2:
        value = table2_dict[item]
        common_tables2.append(value)


    print common_tables1
    for item in common_tables1:
        file1.write(str(item))
        file1.write('\n')
    print common_tables2
    for item in common_tables2:
        file2.write(str(item))
        file2.write('\n')

    # for item in diff_tables1:
    #     file1.write(str(item))
    #     file1.write('\n')
    # for item in diff_tables2:
    #     file2.write(str(item))
    #     file2.write('\n')

if __name__ == '__main__':
    SQL1 = """
    CREATE TABLE aais_edt_dtls_t (
	edt_dtls_id NUMBER(11,0) NOT NULL,
	aais_txn_id NUMBER(11,0) NOT NULL,
	order_id NUMBER(11,0) NOT NULL,
	core_order_num VARCHAR2 (20),
	due_date NUMBER(11,0) NOT NULL,
	tn VARCHAR2 (10) DEFAULT ' ' NOT NULL,
	old_tn VARCHAR2 (10),
	core_cable_name VARCHAR2 (30) DEFAULT ' ' NOT NULL,
	core_cable_no NUMBER(11,0) NOT NULL,
	core_addr_id NUMBER(11,0) NOT NULL,
	oa_cable_name VARCHAR2 (30),
	oa_cable_no NUMBER(11,0),
	ivapp_addr_id NUMBER(11,0) NOT NULL,
	state VARCHAR2 (2) NOT NULL,
	switch_clli VARCHAR2 (30),
	cancel_flag NUMBER(11,0),
	core_txn_id NUMBER(11,0),
	PRIMARY KEY (edt_dtls_id),
	FOREIGN KEY (AAIS_TXN_ID) REFERENCES AAIS_TXN_T
);
CREATE TABLE aais_if_log_t (
	aais_if_log_id NUMBER(11,0) NOT NULL,
	aais_txn_id NUMBER(11,0) NOT NULL,
	txn_msg CLOB NOT NULL,
	PRIMARY KEY (aais_if_log_id),
	FOREIGN KEY (AAIS_TXN_ID)
	        REFERENCES AAIS_TXN_TN_IN
);
CREATE  INDEX XIE1AAIS_EDT_DTLS ON AAIS_EDT_DTLS_T (TN);
CREATE TABLE aais_if_eee_t (
	aais_if_log_id NUMBER(11,0) NOT NULL,
	aais_txn_id NUMBER(11,0) NOT NULL,
	txn_msg CLOB NOT NULL,
	PRIMARY KEY (aais_if_log_id),
	FOREIGN KEY (AAIS_TXN_ID)
	        REFERENCES AAIS_TXN_TN_IN
);CREATE TABLE aais_if_bbb_t (
	aais_if_log_id NUMBER(11,0) NOT NULL,
	aais_txn_id NUMBER(11,0) NOT NULL,
	txn_msg CLOB NOT NULL,
	PRIMARY KEY (aais_if_log_id),
	FOREIGN KEY (AAIS_TXN_ID)
	        REFERENCES AAIS_TXN_TN_IN
);
"""

    SQL2 = """
        CREATE TABLE aais_edt_dtls_t (
    	edt_dtls_id NUMBER(11,0) NOT NULL,
    	aais_txn_id NUMBER(11,0) NOT NULL,
    	order_id NUMBER(11,0) NOT NULL,
    	tn VARCHAR2 (10) DEFAULT ' ' NOT NULL,
    	old_tn VARCHAR2 (10),
    	core_cable_name VARCHAR2 (30) DEFAULT ' ' NOT NULL,
    	core_cable_no NUMBER(11,0) NOT NULL,
    	core_addr_id NUMBER(11,0) NOT NULL,
    	oa_cable_name VARCHAR2 (30),
    	oa_cable_no NUMBER(11,0),
    	ivapp_addr_id NUMBER(11,0) NOT NULL,
    	state VARCHAR2 (2) NOT NULL,
    	switch_clli VARCHAR2 (30),
    	cancel_flag NUMBER(11,0),
    	core_txn_id NUMBER(11,0),
    	PRIMARY KEY (edt_dtls_id),
    	FOREIGN KEY (AAIS_TXN_ID) REFERENCES AAIS_TXN_T
    );
    CREATE TABLE aais_if_log_t (
    	aais_if_log_id NUMBER(11,0) NOT NULL,
    	aais_txn_id NUMBER(11,0) NOT NULL,
    	txn_msg CLOB NOT NULL,
    	PRIMARY KEY (aais_if_log_id),
    	FOREIGN KEY (AAIS_TXN_ID)
    	        REFERENCES AAIS_TXN_TN_IN
    );
    CREATE  INDEX XIE1AAIS_EDT_DTLS ON AAIS_EDT_DTLS_T (TN);

    """
    main_generator(SQL1,SQL2)
