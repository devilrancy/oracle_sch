#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = 'v1.0'

# Used for Comparing the Schema across two schema files
#
# author: Naresh, Surisetty <naresh.surisetty@one.verizon.com>

import sqlparse
import re
import fileinput
import sys

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

    print token_list
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

def normalize(file_input,subs):
    for pattern, replacement in subs:
        file_input = re.sub(pattern, replacement, file_input)
        # print 'PATTERN: %s SUB: %s' % (pattern,replacement)
        # print name
        # print '___________________________________________'
    return file_input

def create_ddl_statement(file1,parsed):
    _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
    rows = extract_table_definitions(par)
    print rows
    keys = []
    columns = []
    for row in rows:
        for column in row:
            if ("KEY") in str(column):
                keys.append(str(column))
            else:
                columns.append(str(column))
    print sorted(columns)
    print keys

    file1.write('CREATE TABLE %s (\n' % (str(parsed.get_name().lower())))
    for column in sorted(columns):
        file1.write('\t %s,\n' % (str(column)))
    keys = sorted(keys)
    for i in range(0, len(keys)):
        if i == len(keys) - 1:
            file1.write('\t %s\n);\n\n' % (str(keys[i])))
        else:
            file1.write('\t %s,\n' % (str(keys[i])))

def extract_column_info(sql,file_name,tables,diff_tables):
    # print sql
    parsed = sqlparse.parse(sql)
    print parsed
    file1 = open(file_name, 'w')
    for table in tables:
        for parsed_sql in parsed:
            if str(table) == str(parsed_sql.get_name()):
                create_ddl_statement(file1,parsed_sql)
    for table in diff_tables:
        for parsed_sql in parsed:
            if str(table) == str(parsed_sql.get_name()):
                create_ddl_statement(file1,parsed_sql)
    file1.close()
    replaceAll(file_name,' := ',' ')
    replaceAll(file_name,'PRIMARY_KEY','PRIMARY KEY')
    replaceAll(file_name,'FOREIGN_KEY','FOREIGN KEY')
    replaceAll(file_name,'_NOT_NULL',' NOT NULL')

def rearrange_ddl_content(in_text,out_txt):
    parsed1 = sqlparse.parse(in_text)
    parsed2 = sqlparse.parse(out_txt)
    table1_list,index1_list = [str(item.get_name()) for item in sorted(parsed1) if 'TABLE' in str(item)],\
                              [str(item.get_name()) for item in sorted(parsed1) if 'INDEX' in str(item)]
    table2_list, index2_list = [str(item.get_name()) for item in sorted(parsed2) if 'TABLE' in str(item)], \
                               [str(item.get_name()) for item in sorted(parsed2) if 'INDEX' in str(item)]
    tab_int = set(table1_list) & set(table2_list)
    return sorted(tab_int),sorted(set(table1_list) - tab_int),sorted(set(table2_list) - tab_int)


def replaceAll(file,searchExp,replaceExp):
    for line in fileinput.input(file, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp,replaceExp)
        sys.stdout.write(line)

if __name__ == '__main__':

    regex_subs = [
        ("\(\d+,\d+\)", ""),
        ("\(\d+\)", ""),
        ("\s+(DEFAULT|default)\s+'\s+'", ""),
        ("\s+(VARCHAR|varchar)"," := VARCHAR"),
        ("\s+(NUMBER|number)"," := NUMBER"),
        ("\s+(CLOB|clob)"," := CLOB"),
        ("'",""),
        ("(NOT NULL|not null)","_NOT_NULL"),
        ("(REFERENCES|references)", ":= REFERENCES :="),
        ("\s+(KEY|key)","_KEY :="),
        ("\s+"," "),
        ("\s+\_", "_"),
        ("\s+,", ","),
        (";",";\n"),
        ("(\_t\(|\_T\(|\_t\,|\_T\,|\_t\s\()","(|(|,|,|(|("),
        ("\s+\)\;", ", );")
    ]

    with open("in_db.txt") as text:
        in_db_txt = normalize(text.read(),regex_subs)
    with open("out_db.txt") as text:
        out_db_txt = normalize(text.read(), regex_subs)
    # with open("in_db.txt", "w") as result:
    #     result.write(new_text)
    comm_tables, diff_tab1, diff_tab2 = rearrange_ddl_content(in_db_txt,out_db_txt)

    extract_column_info(in_db_txt,'tmp_in_db.txt',comm_tables,diff_tab1)

    extract_column_info(out_db_txt,'tmp_out_db.txt',comm_tables,diff_tab2)
