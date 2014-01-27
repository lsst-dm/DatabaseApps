#!/usr/bin/env python

# $Id: catalog_ingest.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import pyfits
import re
import subprocess
import time
from collections import OrderedDict
from coreutils import desdbi
from coreutils import serviceaccess
import convutils
import argparse

CATINGEST_COLUMN_NAME = 0
CATINGEST_DERIVED = 1
CATINGEST_DATATYPE = 2
CATINGEST_POSITION = 3
CATINGEST_VALUE = 0
CATINGEST_QUOTE = 1

funcdict = { 'BAND': convutils.func_getband, 'NITE': convutils.func_getnite }


def dataTypeMap():
    return {'E':'float external','D':'decimal external','I':'integer external','J':'integer external'}

def getObjectColumns(filetype):
    dbh = desdbi.DesDbi()
    results = OrderedDict()
    sqlstr = '''
        select hdu, UPPER(attribute_name), position, column_name, derived, datafile_datatype
        from ops_datafile_metadata
        where filetype=:ftype
        order by 1,2,3'''
    cursor = dbh.cursor()
    cursor.execute(sqlstr,{'ftype':filetype})
    records = cursor.fetchall()
    for rec in records:
        if rec[0] not in results:
            results[rec[0]] = {}
        if rec[1] not in results[rec[0]]:
            results[rec[0]][rec[1]] = [[rec[3]],rec[4],rec[5],[str(rec[2])]]
        else:
            results[rec[0]][rec[1]][CATINGEST_COLUMN_NAME].append(rec[3])
            results[rec[0]][rec[1]][CATINGEST_POSITION].append(str(rec[2]))
    cursor.close()
    dbh.close()
    return results


def getValuesFromHeader(constDict,dbDict,hduName,hduList):
    value = None
    quoteit = None
    hdr = None
    if hduName == 'LDAC_IMHEAD':
        data = hdu.data
        cards = []
        for cd in data[0][0]:
            cards.append(pyfits.Card.fromstring(cd))
        hdr = pyfits.Header(cards)
    else:
        hdr = hduList[hduName].header
    for attribute, dblist in dbDict.iteritems():
        for col in dblist[CATINGEST_COLUMN_NAME]:
            if dblist[CATINGEST_DERIVED] == 'c':
                value = funcdict[col](hdr[attribute.upper()])
            elif dblist[CATINGEST_DERIVED] == 'h':
                value = hdr[attribute.upper()]
            if dblist[CATINGEST_DATATYPE] == 'char':
                quoteit = True
            else:
                quoteit = False
            constDict[col] = [value,quoteit]


def writeControlfileHeader(controlfile, constDict, tablename):
    controlfile.write('LOAD DATA\nINFILE "-"\n')
    controlfile.write("INTO TABLE " + tablename + "\nAPPEND\nFIELDS TERMINATED BY ','\n(\n")
    for colname, val in constDict.iteritems():
        if val[CATINGEST_QUOTE]:
            controlfile.write(colname + " CONSTANT '" + val[CATINGEST_VALUE] + "',\n")
        else:
            controlfile.write(colname + " CONSTANT " + str(val[CATINGEST_VALUE]) + ",\n")


def writeControlfileFooter(controlfile):
    controlfile.write(")")

def parseFitsTypeLength(formatsByColumn):
    colsizes = OrderedDict()
    coltypes = OrderedDict()
    for col, dtype in formatsByColumn.iteritems():
        m = re.search('^(\d*)(.*)$',dtype)
        colsizes[col] = int(m.group(1))
        coltypes[col] = m.group(2)
    return [colsizes, coltypes]


def writeControlFile(controlFileName, constDict, dbObjectData, hduList, tablename):
    dt = dataTypeMap()
    controlfile = file(controlFileName, 'w')
    for hduName in dbObjectData.keys():
        if hduName not in ['LDAC_OBJECTS','WCL']:
            getValuesFromHeader(constDict, dbObjectData[hduName], hduName, hduList)
    writeControlfileHeader(controlfile, constDict, tablename)
    data = hduList["LDAC_OBJECTS"].data
    dbdata = dbObjectData["LDAC_OBJECTS"]
    orderedFitsColumns = data.columns.names
    (colsizes, coltypes) = parseFitsTypeLength(dict(zip(data.columns.names,data.columns.formats)))
    filerows = []
    for headerName in orderedFitsColumns:
        if headerName in dbdata.keys():
            if coltypes[headerName] in dt.keys():
                for idx in range(0,colsizes[headerName]):
                    row = []
                    row.append(dbdata[headerName][CATINGEST_COLUMN_NAME][idx])
                    row.append(dt[coltypes[headerName]])
                    filerows.append(" ".join(row))
            else:
                print "fits datatype " + coltypes[headerName] + " not mapped to sqlldr datatype!"
    controlfile.write(",\n".join(filerows))
    writeControlfileFooter(controlfile)
    controlfile.close()


def catalogIngest(hduList,constDict,tablename,filetype):
    controlfilename = 'catingest.ctl'
    logfile = 'catingest.log'
    badrowsfile = 'badrows.bad'
    discardfile = 'discarded.bad'
    connectinfo = serviceaccess.parse(None,None,'DB')
    connectstring = connectinfo["user"] + "/" + connectinfo["passwd"] + "@" + connectinfo["name"]
    sqlldr_command = []
    sqlldr_command.append("sqlldr")
    sqlldr_command.append(connectstring)
    sqlldr_command.append("control=" + controlfilename)
    sqlldr_command.append("bad=" + badrowsfile)
    sqlldr_command.append("discard=" + discardfile)
    sqlldr_command.append("silent=header,feedback,partitions")

    dbdata = getObjectColumns(filetype)
    writeControlFile(controlfilename, constDict, dbdata, hduList, tablename)
    print("sqlldr control file " + controlfilename + " created")
    columnsToCollect = dbdata["LDAC_OBJECTS"]
    sqlldr = None
    try:
        print("invoking sqlldr with control file " + controlfilename)
        sqlldr = subprocess.Popen(sqlldr_command,shell=False,stdin=subprocess.PIPE)
        
        data = hduList["LDAC_OBJECTS"].data
        orderedFitsColumns = data.columns.names
        (colsizes, coltypes) = parseFitsTypeLength(dict(zip(data.columns.names,data.columns.formats)))
        cols = len(orderedFitsColumns)

        for row in data:
            dbrow = []
            idx = 0
            while idx < cols:
                if orderedFitsColumns[idx] in columnsToCollect.keys():
                    if colsizes[orderedFitsColumns[idx]] == 1:
                        dbrow.append(str(row[idx]))
                    else:
                        for counter in range(0,colsizes[orderedFitsColumns[idx]]):
                            if str(counter) in columnsToCollect[orderedFitsColumns[idx]][CATINGEST_POSITION]:
                                dbrow.append(str(row[idx][counter]))
                idx = idx+1
            if sqlldr and sqlldr.poll() == None:
                sqlldr.stdin.write(",".join(dbrow) + "\n")
            else:
                exit("sqlldr exited with errors. See " + logfile + ", " + discardfile + " and " + badrowsfile + " for details")
    finally:
        if sqlldr:
            sqlldr.stdin.close()
   
    if sqlldr and sqlldr.wait():
        exit("sqlldr exited with errors. See " + logfile + ", " + discardfile + " and " + badrowsfile + " for details")
    else:
        if os.path.exists(controlfilename):
            os.remove(controlfilename)
        if os.path.exists(logfile):
            os.remove(logfile)
        if os.path.exists(badrowsfile):
            os.remove(badrowsfile)
        if os.path.exists(discardfile):
            os.remove(discardfile)


def getShortFilename(longname):
    shortname = None
    if '/' in longname:
        idx = longname.rfind('/') + 1
        shortname = longname[idx:]
    else:
         shortname = longname
    return shortname.strip()


def numAlreadyIngested(filename,tablename):
    try:
        dbh = desdbi.DesDbi()
        results = OrderedDict()
        sqlstr = '''
        select count(*), reqnum 
        from %s
        where filename=:fname
        group by reqnum
        '''
        cursor = dbh.cursor()
        cursor.execute(sqlstr % tablename,{"fname":filename})
        records = cursor.fetchall()
    finally:
        dbh.close()
    if(len(records) > 0):
        return records[0]
    else:
        return [0,0]


def getNumObjects(hduList):
    data = hduList["LDAC_OBJECTS"].data
    return len(data)

def checkParam(args,param):
    if args[param]:
        return args[param]
    else:
        sys.stderr.write("Missing required parameter: %s" % param)


if __name__ == '__main__':

    hduList = None

    parser = argparse.ArgumentParser(description='Ingest objects from a fits catalog')
    parser.add_argument('--request',action='store')
    parser.add_argument('--filename',action='store')
    parser.add_argument('--filetype',action='store')
    parser.add_argument('--temptable',action='store')
    parser.add_argument('--targettable',action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    request = checkParam(args,'request')
    filename = checkParam(args,'filename')
    filetype = checkParam(args,'filetype')
    temptable = checkParam(args,'temptable')
    targettable = checkParam(args,'targettable')

    print("Preparing to load " + filename + " of type " + filetype + " into " + temptable)

    constVals = {"FILENAME":[getShortFilename(filename),True], "REQNUM":[request,False]}

    try:
        (numDbObjects,dbReqnum) = numAlreadyIngested(constVals["FILENAME"][CATINGEST_VALUE],targettable)

        hduList = pyfits.open(filename)

        numCatObjects = getNumObjects(hduList)

        if numDbObjects > 0:
            if numDbObjects == numCatObjects:
                print "WARNING: file " + filename + " already ingested with the same number of objects. Original reqnum=" + str(dbReqnum) + ". Aborting new ingest"
                exit(0)
            else:
                errstr = "ERROR: file " + filename + " already ingested, but the number of objects is DIFFERENT: catalog=" + str(numCatObjects) + "; DB=" + str(numDbObjects) + ", Original reqnum=" + str(dbReqnum)
                raise Exception(errstr)
                exit(1)

        catalogIngest(hduList, constVals, temptable, filetype)
    
        print("catalogIngest load of " + str(numCatObjects) + " objects from " + filename + " completed")

    finally:
        if not hduList == None:
            hduList.close()


