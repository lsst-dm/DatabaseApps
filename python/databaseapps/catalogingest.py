#!/usr/bin/env python

# $Id: catalog_ingest.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import fitsio
import subprocess
import time
from collections import OrderedDict
from coreutils import desdbi
from coreutils import serviceaccess
from databaseapps import ingestutils
import argparse


class CatalogIngest:

    COLUMN_NAME = 0
    DERIVED = 1
    DATATYPE = 2
    POSITION = 3
    VALUE = 0
    QUOTE = 1
    SQLLDR = 'sqlldr'
    FILE = 'file'

    dbh = None
    request = None
    fullfilename = None
    shortfilename = None
    filetype = None
    temptable = None
    tempschema = None
    targettable = None
    targetschema = None
    outputfile = 'dataset.dat'
    mode = SQLLDR
    objhdu = 'LDAC_OBJECTS'
    controlfilename = 'catingest.ctl'
    logfile = 'catingest.log'
    badrowsfile = 'badrows.bad'
    discardfile = 'discarded.bad'

    constDict = None
    funcDict = None
    dbDict = None
    fits = None


    def __init__(self, request, filetype, datafile, temptable, 
                    targettable, fitsheader, mode, outputfile):
        
        self.dbh = desdbi.DesDbi()
        self.fits = fitsio.FITS(datafile)
        self.request = request
        self.filetype = filetype
        self.fullfilename = datafile
        self.shortfilename = getShortFilename(datafile)

        if fitsheader:
            if isInteger(fitsheader):
                self.objhdu = int(fitsheader)
            else:
                self.objhdu = fitsheader
        if mode:
            self.mode = mode
        if outputfile:
            self.outputfile = outputfile

        (self.targetschema,self.targettable) = ingestutils.resolveDbObject(targettable,self.dbh)
        if not temptable:
            self.temptable = "DESSE_REQNUM%07d" % int(request)
            self.tempschema = self.targetschema
        else:
            (self.tempschema,self.temptable) = ingestutils.resolveDbObject(temptable,self.dbh)

        self.constDict = {
            "FILENAME":[getShortFilename(filename),True], 
            "REQNUM":[request,False]
            }

        self.dbDict = self.getObjectColumns()

    def __del__(self):
        if self.dbh:
            self.dbh.close()
        if self.fits:
            self.fits.close()


    def getObjectColumns(self):
        results = OrderedDict()
        sqlstr = '''
            select dm.hdu, UPPER(dm.attribute_name), dm.position, dm.column_name, dm.derived,
                case when data_type='NUMBER' and data_scale=0 THEN 'integer external'
                    when data_type='BINARY_FLOAT' THEN 'float external'
                    when data_type in('BINARY_DOUBLE','NUMBER') THEN 'decimal external'
                    when data_type in('VARCHAR2','CHAR') THEN 'char'
                end sqlldr_type
            from ops_datafile_metadata dm, all_tab_columns atc
            where dm.column_name=atc.column_name
                and atc.table_name=:tabname
                and atc.owner=:ownname
                and dm.filetype=:ftype
            order by 1,2,3 '''
        cursor = self.dbh.cursor()
        params = {
            'ftype':self.filetype,
            'tabname':self.targettable,
            'ownname':self.targetschema
            }
        cursor.execute(sqlstr,params)
        records = cursor.fetchall()
        for rec in records:
            hdr = None
            if rec[0].upper() = 'PRIMARY':
                hdr = 0
            else:
                if isInteger(rec[0]):
                    hdr = int(rec[0])
                else:
                    hdr = rec[0]
            if hdr not in results:
                results[hdr] = {}
            if rec[1] not in results[hdr]:
                results[hdr][rec[1]] = [[rec[3]],rec[4],rec[5],[str(rec[2])]]
            else:
                results[hdr][rec[1]][self.COLUMN_NAME].append(rec[3])
                results[hdr][rec[1]][self.POSITION].append(str(rec[2]))
        cursor.close()
        return results


    def getConstValuesFromHeader(self, hduName):
        value = None
        quoteit = None
        hdr = self.fits[hduName].read_header()
        
        for attribute, dblist in self.dbDict[str(hduName)].iteritems():
            for col in dblist[COLUMN_NAME]:
                if dblist[DERIVED] == 'c':
                    value = self.funcDict[col](hdr[attribute])
                elif dblist[DERIVED] == 'h':
                    value = hdr[attribute]
                if dblist[DATATYPE] == 'char':
                    quoteit = True
                else:
                    quoteit = False
                self.constDict[col] = [value,quoteit]


    def writeControlfileHeader(self,controlfile):
        controlfile.write('UNRECOVERABLE\nLOAD DATA\n')
        if self.MODE == self.SQLLDR:
            controlfile.write('INFILE "-"\n')
        elif self.MODE == self.FILE:
            controlfile.write('INFILE "' + self.OUTPUTFILE + '"\n')
        controlfile.write("INTO TABLE " + tablename + "\nAPPEND\nFIELDS TERMINATED BY ','\n(\n")
        for colname, val in self.constDict.iteritems():
            if val[self.QUOTE]:
                controlfile.write(colname + " CONSTANT '" + val[self.VALUE] + "',\n")
            else:
                controlfile.write(colname + " CONSTANT " + str(val[self.VALUE]) + ",\n")


    def writeControlfileFooter(self,controlfile):
        controlfile.write(")")

    def parseFitsTypeLength(self, formatsByColumn):
        colsizes = OrderedDict()
        coltypes = OrderedDict()
        for col, dtype in formatsByColumn.iteritems():
            m = re.search('^(\d*)(.*)$',dtype)
            if m.group(1) and m.group(2) != 'A':
                colsizes[col] = int(m.group(1))
            else:
                colsizes[col] = 1
            coltypes[col] = m.group(2)
        return [colsizes, coltypes]


    def writeControlFile(self):
        controlfile = file(self.controlfilename, 'w')
        for hduName in self.dbData.keys():
            if hduName not in [self.objhdu,'WCL']:
                getConstValuesFromHeader(hduName)
        writeControlfileHeader(controlfile)
        dbobjdata = self.dbData[self.objhdu]
        orderedFitsColumns = self.fits[self.objhdu].get_colnames()
        filerows = []
        for headerName in orderedFitsColumns:
            if headerName.lower() in dbobjdata.keys():
                for colname in dbobjdata[headerName.lower()][self.COLUMN_NAME]:
                    row = []
                    row.append[colname]
                    row.append[dbobjdata[headerName.lower()][self.DATATYPE]
                    filerows.append(" ".join(row))
        controlfile.write(",\n".join(filerows))
        writeControlfileFooter(controlfile)
        controlfile.close()
        print("sqlldr control file " + self.controlfilename + " created")


    def getSqlldrCommand(self):
        connectinfo = serviceaccess.parse(None,None,'DB')
        connectstring = connectinfo["user"] + "/" + connectinfo["passwd"] + "@" + connectinfo["name"]
        sqlldr_command = []
        sqlldr_command.append("sqlldr")
        sqlldr_command.append(connectstring)
        sqlldr_command.append("control=" + self.controlfilename)
        sqlldr_command.append("bad=" + self.badrowsfile)
        sqlldr_command.append("discard=" + self.discardfile)
        sqlldr_command.append("DIRECT=true")
        sqlldr_command.append("parallel=true")
        sqlldr_command.append("silent=header,feedback,partitions")
        return sqlldr_command


    def execute(self,firstrow=1,lastrow=-1):
        if lastrow == -1:
            lastrow = self.fits[self.objhdu].get_nrows()
        attrsToCollect = self.dbData[self.objhdu]
        sqlldr = None
        outfile = None
        try:
            if self.MODE == self.SQLLDR:
                print("invoking sqlldr with control file " + self.controlfilename)
                sqlldr_command = getSqlldrCommand()
                sqlldr = subprocess.Popen(sqlldr_command,shell=False,stdin=subprocess.PIPE)
            elif self.MODE == self.FILE:
                outfile = open(self.outputfile,'w')

            attrs = attrsToCollect.keys()
            orderedFitsColumns = []
            allcols = self.fits[self.objhdu].get_colnames()
            for col in allcols:
                if col.lower() in attrs:
                    orderedFitsColumns.append(col)
            datatypes = self.fits[self.objhdu].get_rec_dtype()[0]

            startrow = firstrow-1
            endrow = firstrow-1
            while endrow < lastrow:
                startrow = endrow
                endrow = min(startrow+1000, lastrow)
                data = fitsio.read(
                        self.fullfilename,
                        rows=range(startrow,endrow),
                        columns=attrs,ext=objhdu
                        )
                for row in data:
                    outrow = []
                    for idx in range(0,len(orderedFitsColumns)):
                        # if this column is an array of values
                        if datatypes[orderedFitsColumns[idx].upper()].subdtype:
                            arrvals = []
                            for pos in attrsToCollect[orderedFitsColumns[idx]][self.POSITION]:
                                arrvals.append(str(row[idx][pos]).strip())
                            outrow.append(','.join(arrvals))
                        # else it is a scalar
                        else:
                            outrow.append(str(row[idx]))
                    # if sqlldr subprocess is still alive, write to it
                    if sqlldr and sqlldr.poll() == None:
                        sqlldr.stdin.write(",".join(dbrow) + "\n")
                    # else if we are writing to a file
                    elif outfile:
                        outfile.write(",".join(dbrow) + "\n")
                    # otherwise some error occurred
                    else:
                        exit("sqlldr or file writing exited with errors. See " + logfile + 
                             ", " + discardfile + " and " + badrowsfile + " for details")
                # end for row in data
            # end while endrow < lastrow
        finally:
            if sqlldr:
                sqlldr.stdin.close()
            if outfile:
                outfile.close()
   
        if sqlldr and sqlldr.wait():
            exit("sqlldr exited with errors. See " + logfile + ", " + discardfile + 
                 " and " + badrowsfile + " for details")
        else:
            if os.path.exists(controlfilename) and self.MODE=='sqlldr':
                os.remove(controlfilename)
            if os.path.exists(logfile):
                os.remove(logfile)
            if os.path.exists(badrowsfile):
                os.remove(badrowsfile)
            if os.path.exists(discardfile):
                os.remove(discardfile)


    def numAlreadyIngested(self):
        try:
            results = OrderedDict()
            sqlstr = '''
            select count(*), reqnum 
            from %s
            where filename=:fname
            group by reqnum
            '''
            cursor = self.dbh.cursor()
            cursor.execute(sqlstr % tablename,{"fname":self.shortfilename})
            records = cursor.fetchall()
        if(len(records) > 0):
            return records[0]
        else:
            return [0,0]


    def getNumObjects(self):
        return self.fits[self.objhdu].get_nrows()


    def createIngestTable(self):
        tablespace = "DESSE_REQNUM%07d_T" % int(request)
        try:
            cursor = self.dbh.cursor()
            print ("Creating tablespace %s and table %s if they do not already exist"
                     % (tablespace,temptable))
            cursor.callproc("createObjectsTable",[temptable,tablespace,targettable])
            cursor.close()
            print "Temp table %s is ready" % temptable


    def isLoaded(self):
        (numDbObjects,dbReqnum) = numAlreadyIngested()
        numCatObjects = self.getNumObjects()
        exitcode = 0
        loaded = False
        if numDbObjects > 0:
            loaded = True
            if numDbObjects == numCatObjects:
                print ("WARNING: file " + fullfilename + " already ingested " +
                        "with the same number of objects. " +
                        "Original reqnum=" + str(dbReqnum) + ". Aborting new ingest")
                exitcode = 0
            else:
                errstr = ("ERROR: file " + fullfilename + " already ingested, but " +
                        "the number of objects is DIFFERENT: catalog=" + 
                        str(numCatObjects) + "; DB=" + str(numDbObjects) + 
                        ", Original reqnum=" + str(dbReqnum))
                raise Exception(errstr)
                exitcode = 1
        return (loaded,exitcode)


