#!/usr/bin/env python
# $Id: dessubmit 39403 2015-07-23 16:11:33Z mgower $
# $Rev:: 39403                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-07-23 11:11:33 #$:  # Date of last commit.

""" Ingest non-metadata from file into DB table """

import argparse

from despydmdb.desdmdbi import DesDmDbi
import databaseapps.datafile_ingest_utils as dfiutils



def main():
    """ Program entry point """

    parser = argparse.ArgumentParser(description='Ingest non-metadata from file into DB table')
    parser.add_argument('--filename', action='store', required=True)
    parser.add_argument('--filetype', action='store', required=True)
    parser.add_argument('--section', '-s', help='db section in the desservices file')
    parser.add_argument('--des_services', help='desservices file')

    args = parser.parse_args()
    args = vars(args)

    fullname = args['filename']
    filetype = args['filetype']

    dbh = None
    try:
        print "datafile_ingest.py: Preparing to ingest " + fullname
        dbh = DesDmDbi(args['des_services'], args['section'])
        [tablename, didatadefs] = dbh.get_datafile_metadata(filetype)

        numrows = dfiutils.datafile_ingest_main(dbh, filetype, fullname, tablename, didatadefs)
        if numrows == None or numrows == 0:
            print "datafile_ingest.py: warning - 0 rows ingested from " + fullname
        else:
            dbh.commit()
            print "datafile_ingest.py: ingest of " + fullname + ", %s rows, complete" % numrows
    finally:
        if dbh is not None:
            dbh.close()


if __name__ == '__main__':
    main()
