#!/usr/bin/env python

# $Id: create_ingest_table.py 11430 2014-01-17 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import argparse
from coreutils import desdbi


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create ingest temp table')
    parser.add_argument('--temptable',action='store')
    parser.add_argument('--targettable',action='store')
    parser.add_argument('--tablespace',action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    errors = []
    if args['targettable'] == None:
        errors.append("targettable is required")
    if args['temptable'] == None:
        errors.append("temptable is required")
    if args['tablespace'] == None:
        errors.append("tablespace is required")
    if len(errors) > 0:
        sys.stderr.write("ERROR: " + "; ".join(errors) + "\n")
        exit(1)

    dbh = desdbi.DesDbi()
    cursor = dbh.cursor()
    print "Creating tablespace %s ..." % args['tablespace']
    cursor.callproc("des_admin.createObjectsTablespace",[args['tablespace']])

    print "Creating temp table %s using %s as a template..." % (args['temptable'], args['targettable'])
    cursor.execute("create table %s tablespace %s as select * from %s where 1=0" % (args['temptable'], args['tablespace'],args['targettable']))
    cursor.close()
    print "Temp table %s created successfully" % args['temptable']

