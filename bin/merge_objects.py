#!/usr/bin/env python

# $Id: merge_objects.py 11430 2014-01-17 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import argparse
from despydb import desdbi

def parseTableName(inname):
    tablename = None
    schemaname = None
    arr = inname.split('.',1)
    if len(arr) > 1:
        schemaname,tablename = arr
    else:
        schemaname = None
        tablename = inname
    return [schemaname, tablename]

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Merge objects into main table')
    parser.add_argument('-request',action='store')
    parser.add_argument('-temptable',action='store')
    parser.add_argument('-targettable',action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    errors = []
    if args['targettable'] == None:
        errors.append("targettable is required")
    if args['temptable'] == None and args['request'] == None:
        errors.append("either temptable or request is required")
    if len(errors) > 0:
        sys.stderr.write("ERROR: " + "; ".join(errors) + "\n")
        exit(1)

    targetschema, targettable = parseTableName(args['targettable'])
    temptable = args['temptable']
    tempschema = None

    if temptable == None:
        temptable = "DESSE_REQNUM%07d" % int(args['request'])
        tempschema = targetschema
    else:
        tempschema, temptable = parseTableName(args['temptable'])

    if tempschema:
        print "Merging %s into %s..." % (tempschema + '.' + temptable, args['targettable'])
    else:
        print "Merging %s into %s..." % (temptable, args['targettable'])

    dbh = desdbi.DesDbi()
    cursor = dbh.cursor()
    if targetschema == None:
        cursor.callproc("pMergeObjects",[temptable,targettable,tempschema,targetschema])
    else:
        cursor.callproc("%s.pMergeObjects" % targetschema,[temptable,targettable,tempschema,targetschema])

    cursor.close()
    print "Merge complete"




