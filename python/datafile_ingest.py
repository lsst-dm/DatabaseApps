#!/usr/bin/env python

from coreutils.desdbi import DesDbi
import intgutils.wclutils as wclutils
import sys, os
from collections import OrderedDict
import xmlslurp

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
    print "datafile_ingest.py: destination table=" + tablename
    #printNode(metadata,0,sys.stdout)
    columnlist = []
    data = []
    indata = []
    if hasattr(dataDict,"keys"):
        indata.append(dataDict)
    else:
        indata=dataDict

    for hdu,attrDict in metadata.iteritems():
        for attribute,cols in attrDict.iteritems():
            for indx, colname in enumerate(cols):
                columnlist.append(colname)
        columnlist.append('filename')

    for hdu,attrDict in dataDict.iteritems():
        indata = []
        if hasattr(attrDict,"keys"):
            indata.append(attrDict)
        else:
            indata=attrDict
        for inrow in indata:
            row = {}
            for attribute,cols in metadata[hdu].iteritems():
                for indx, colname in enumerate(cols):
                    if attribute in inrow.keys():
                        if type(inrow[attribute]) is list:
                            if indx < len(inrow[attribute]):
                                row[colname] = inrow[attribute][indx]
                            else:
                                row[colname] = None
                        else:
                            if indx == 0:
                                row[colname] = inrow[attribute]
                            else:
                                row[colname] = None
                    else:
                        row[colname] = None
        if(len(row) > 0):
            row["filename"] = sourcefile
            data.append(row)
    if(len(data) > 0):
        dbh.insert_many_indiv(tablename,columnlist,data)
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


if __name__ == '__main__':

    filename = None
    filetype = None
    dbh = None

    if(len(sys.argv) > 1):
        filename = sys.argv[1]
        filetype = sys.argv[2]
    else:
        exit(1)

    try:
        print "datafile_ingest.py: Preparing to ingest " + filename
        dbh = DesDbi()
        sectionsWanted = getSectionsForFiletype(filetype,dbh)
        mydict = xmlslurp.xmlslurper(filename,sectionsWanted).gettables()
        ingest_datafile_contents(filename,filetype,mydict,dbh)
        dbh.commit()
        print "datafile_ingest.py: ingest of " + filename + " complete"
    finally:
        if dbh is not None:
            dbh.close()

