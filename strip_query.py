import sys

from django.test.utils import str_prefix

ddl_query = """
    create table dept(
      deptno     number(2,0),
      dname      varchar2(14),
      loc        varchar2(13),
      constraint pk_dept primary key (deptno)
    );

    create table emp(
      empno    number(4,0),
      ename    varchar2(10),
      job      varchar2(9),
      mgr      number(4,0),
      hiredate date,
      sal      number(7,2),
      comm     number(7,2),
      deptno   number(2,0),
      constraint pk_emp primary key (empno),
      constraint fk_deptno foreign key (deptno) references dept (deptno)
    );
"""

ddl_query2 = """
    create table dept(
      deptno     number(2,0),
      dname      varchar2(14),
      constraint pk_dept primary key (deptno)
    );

    create table emp(
      empno    number(4,0),
      ename    varchar2(10),
      mgr      number(4,0),
      hiredate date,
      sal      number(7,2),
      comm     number(7,2),
      constraint pk_emp primary key (empno),
      constraint fk_deptno foreign key (deptno) references dept (deptno)
    );
"""

def strip_lists(str_list):
    start_indexs, end_indexs = [i for i in range(0, len(str_list)) if 'table' in str_list[i]], [i for i in range(0, len(str_list)) if ');' in str_list[i]]
    string_cat_list = []
    for i in range(0,len(end_indexs)):
        table_item = str_list[start_indexs[i]]
        [string_cat_list.append(table_item+'.'+str_list[i]) for i in range(start_indexs[i],end_indexs[i]) if 'table' not in str_list[i]]
    return string_cat_list

def strip_query(ddl_query):
    str = ddl_query.splitlines()
    tmp_str = []
    for str_single in str:
        if str_single.strip():
            if 'create' in str_single.strip(): tmp_str.append(str_single.strip().split(' ')[1] + '.' + str_single.strip().split(' ')[2].split('(')[0])
            elif str_single.strip().split(' ')[0] not in ['constraint', 'primary key']: tmp_str.append(str_single.strip().split(' ')[0])
    tmp_str = strip_lists(tmp_str)
    return tmp_str

def main():
    cmp1 = strip_query(ddl_query)
    cmp2 = strip_query(ddl_query2)
    s = set(cmp2)
    temp3 = [x for x in cmp1 if x not in s]
    print temp3

if __name__ == "__main__":
    main()