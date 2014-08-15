#!/usr/bin/env python

from coreutils.desdbi import DesDbi
import coreutils.miscutils as miscutils
import sys, os
from collections import OrderedDict
from databaseapps.xmlslurp import Xmlslurper
import pyfits
import argparse


DI_COLUMNS = 'columns'
DI_DATATYPE = 'datatype'
DI_FORMAT = 'format'


# a case-insensitive dictionary getter
def ci_get(myDict, myKey):
    for k,v in myDict.iteritems():
        if myKey.lower() == k.lower():
            return v
    return None
# end ci_get

def printNode(indict, level, filehandle):
    leveltabs = ""
    for i in range(level):
        leveltabs = leveltabs + "\t"
    else:        
        for key, value in indict.iteritems():
            if type(value) in (dict,OrderedDict):
                print >>filehandle, leveltabs + "<" + str(key) + ">"
                printNode(value,level+1,filehandle)
                print >>filehandle, leveltabs + "</" + str(key) + ">"
            else:
                print >>filehandle, leveltabs + str(key) + "=" + str(value)
# end printNode

def ingest_datafile_contents(sourcefile,filetype,dataDict,dbh):
    [tablename, metadata] = dbh.get_datafile_metadata(filetype)
    if tablename == None or metadata == None:
        sys.stderr.write("ERROR: no metadata in database for filetype=%s. Check OPS_DATAFILE_TABLE and OPS_DATAFILE_METADATA\n" % filetype)
        exit(1)
    print "datafile_ingest.py: destination table=" + tablename
    #printNode(metadata,0,sys.stdout)
    columnlist = []
    data = []
    indata = []
    if hasattr(dataDict,"keys"):
        indata.append(dataDict)
    else:
        indata=dataDict

    dateformat = None

    for hdu,attrDict in metadata.iteritems():
        for attribute,cols in attrDict.iteritems():
            for indx, colname in enumerate(cols[DI_COLUMNS]):
                columnlist.append(colname)
                # handle timestamp format; does not support multiple formats in one input file
                if cols[DI_DATATYPE] == 'date':
                    if dateformat and dateformat != cols[DI_FORMAT]:
                        sys.stderr.write("ERROR: Unsupported configuration for filetype=%s: Multiple different date formats found\n" % filetype)
                        exit(1)
                    dateformat = cols[DI_FORMAT]
                ###
        columnlist.append('filename')

    # handle timestamp format; does not support multiple formats in one input file
    if dateformat:
        cur = dbh.cursor()
        cur.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '%s'" % dateformat)

    for hdu,attrDict in dataDict.iteritems():
        indata = []
        if hasattr(attrDict,"keys"):
            indata.append(attrDict)
        else:
            indata=attrDict
        for inrow in indata:
            row = {}
            for attribute,coldata in metadata[hdu].iteritems():
                for indx, colname in enumerate(coldata[DI_COLUMNS]):
                    attr = None
                    if isinstance(inrow,dict):
                        attr = ci_get(inrow,attribute)
                    else:
                        fitscols = indata.columns.names
                        for k in fitscols:
                            if k.lower() == attribute.lower():
                                attr = inrow.field(k)
                                break
                    if attr:
                        if type(attr) is list:
                            if indx < len(attr):
                                row[colname] = attr[indx]
                            else:
                                row[colname] = None
                        else:
                            if indx == 0:
                                if coldata[DI_DATATYPE] == 'int':
                                    row[colname] = int(attr)
                                elif coldata[DI_DATATYPE] == 'float':
                                    row[colname] = float(attr)
                                else:
                                    row[colname] = attr
                            else:
                                row[colname] = None
                    else:
                        row[colname] = None
            if(len(row) > 0):
                row["filename"] = sourcefile
                data.append(row)
    if(len(data) > 0):
        dbh.insert_many_indiv(tablename,columnlist,data)
        return len(data)
# end ingest_datafile_contents


def getSectionsForFiletype(filetype,dbh):
    sqlstr = "select distinct hdu from OPS_DATAFILE_METADATA where filetype=%s"
    sqlstr = sqlstr % dbh.get_named_bind_string('ftype');
    
    result = []
    curs = dbh.cursor()
    curs.execute(sqlstr,{"ftype":filetype})
    for row in curs:
        result.append(row[0])
    curs.close()
    return result
# end getSectionsForFiletype


def isInteger(s):
    try:
        int(s)
        return True
    except ValueError:
        return False
# end isInteger


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create ingest temp table')
    parser.add_argument('-filename',action='store')
    parser.add_argument('-filetype',action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    fullname = None
    filetype = None
    dbh = None

    if args['filename']:
        fullname = args['filename']
    else:
        sys.stderr.write("Missing required parameter 'filename'. Must include filename and filetype\n")
        exit(1)
    if args['filetype']:
        filetype = args['filetype']
    else:
        sys.stderr.write("Missing required parameter 'filetype'. Must include filename and filetype\n")
        exit(1)

    try:
        print "datafile_ingest.py: Preparing to ingest " + fullname
        dbh = DesDbi()
        mydict = None
        sectionsWanted = getSectionsForFiletype(filetype,dbh)
        if 'xml' in filetype:
            mydict = Xmlslurper(fullname,sectionsWanted).gettables()
        else:
            if len(sectionsWanted) > 1:
                sys.stderr.write("Database is calling for data from multiple sections, which is not yet supported\n")
                exit(1)
            hdu = None
            if (isInteger(sectionsWanted[0])):
                hdu = int(sectionsWanted[0])
            else:
                hdu = sectionsWanted[0]
            hduList = pyfits.open(fullname)
            mydict = {}
            mydict[sectionsWanted[0]] = hduList[hdu].data

        filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME) 
        numrows = ingest_datafile_contents(filename,filetype,mydict,dbh)
        dbh.commit()
        print "datafile_ingest.py: ingest of " + fullname + ", %s rows, complete" % numrows
    finally:
        if dbh is not None:
            dbh.close()

