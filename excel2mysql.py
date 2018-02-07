#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import pymysql
import codecs


def main(csvfile):
    try:
        conn = getconn()
    except pymysql.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])

    cursor = conn.cursor()

    num = loadcsv(cursor,csvfile,conn)

    cursor.close()
    conn.close()
    return num

def getconn():
    # adjust according to your mysql config
    conn = pymysql.connect(host="localhost",
                           user='xxxx',
                           passwd="xxxx",
                           db='xxxx',charset="utf8")
    return conn


def nullify(L):
    """Convert empty strings in the given list to None."""
    list =  L.split(',')
    # helper function
    def f(x):
        if (x == ""):
            return ''
        else:
            return x

    return [f(x) for x in list]

def loadcsv(cursor,filename,conn):
    """
    Open a csv file and load it into a sql table.
    Assumptions:
     - the first line in the file is a header
    """
    f = codecs.open(filename, 'r', 'utf-8')
    s = f.readlines()
    header = s[:1]
    for head in header:
        # calculate the header
        numfields = len(head.split(','))

    query = buildInsertCmd(3)
    s = s[1:]
    i = 0
    for line in s:
        i=i+1
        vals = nullify(line)[:3]
        cursor.execute(query, vals)
        if i == 100:
            i = 0
            conn.commit()

    return len(s)


def buildInsertCmd(numfields):
    """
    Create a query string with the given table name and the right
    number of format placeholders.
    example:
    >>> buildInsertCmd("foo", 3)
    'insert into foo values (%s, %s, %s)'
    """
    assert (numfields > 0)
    placeholders = (numfields - 1) * "%s, " + "%s"
    # todo adjust according to your table structure
    query = ("insert into t_user_info(user_name,crt_id,gender)") + (" values (%s)" % placeholders)
    return query


if __name__ == '__main__':
    # commandline execution
    print '***************开始导入***************'
    print '......'
    # with path
    filename = '/Users/hongqiangren./Downloads/5000-csv.csv'
    num = main(filename)
    print '***************成功导入%s条数据********' % num