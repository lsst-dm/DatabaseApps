#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev$"

import os
import sys
import fitsio
import subprocess
import time
import re
from collections import OrderedDict
from despydb import desdbi
from despyserviceaccess import serviceaccess
from databaseapps.ingestutils import IngestUtils as ingestutils
import argparse


class CoaddHealpix:

    COLUMN_NAME = 0
    DERIVED = 1
    DATATYPE = 2
    POSITION = 3
    VALUE = 0
    QUOTE = 1
    SQLLDR = 'sqlldr'
    FILE = 'file'

    dbh = None
    filetype = 'coadd_hpix'
    schema = None
    targettable = 'COADD_OBJECT_HPIX'
    fullfilename = None
    shortfilename = None
    band = None
    tilename = None
    objhdu = 'OBJECTS'
    controlfilename = 'coaddhealpix.ctl'
    logfile = 'coaddhealpix.log'
    badrowsfile = 'badrows.bad'
    discardfile = 'discarded.bad'

    constDict = None
    funcDict = None
    dbDict = None
    idDict = {}
    fits = None
    debug = True
    debugDateFormat = '%Y-%m-%d %H:%M:%S'


    def __init__(self, datafile, idDict):

        self.debug("start CoaddHealpix.init() for file %s" % datafile)
        self.dbh = desdbi.DesDbi()

        self.debug("opening fits file")
        self.fits = fitsio.FITS(datafile)
        self.debug("fits file opened")

        self.idDict = idDict

        self.debug("start resolveDbObject() for table: %s" % self.targettable)
        (self.schema,self.targettable) = ingestutils.resolveDbObject(self.targettable,self.dbh)
        self.debug("schema, targettable = %s, %s" % (self.schema, self.targettable))

        self.fullfilename = datafile
        self.shortfilename = ingestutils.getShortFilename(datafile)

        self.constDict = {
            "FILENAME":[self.shortfilename,True],
            }
        self.debug("start getObjectColumns()")
        self.dbDict = self.getObjectColumns()
        self.debug("CoaddHealpix.init() done")


    def __del__(self):
        if self.dbh:
            self.dbh.close()
        if self.fits:
            self.fits.close()


    def debug(self, msg):
        if self.debug:
            print time.strftime(self.debugDateFormat) + " - " + msg


    def info(self, msg):
        print time.strftime(self.debugDateFormat) + " - " + msg


    def getObjectColumns(self):
        results = OrderedDict()
        sqlstr = '''
            select dm.hdu, NVL(UPPER(dm.attribute_name),atc.column_name), NVL(dm.position,0), 
                atc.column_name, NVL(dm.derived,'h'),
                case when data_type='NUMBER' and data_scale=0 THEN 'integer external'
                    when data_type='BINARY_FLOAT' THEN 'float external'
                    when data_type in('BINARY_DOUBLE','NUMBER') THEN 'decimal external'
                    when data_type in('VARCHAR2','CHAR') THEN 'char'
                end sqlldr_type
            from ops_datafile_metadata dm, all_tab_columns atc
            where dm.column_name (+)= atc.column_name
                and atc.table_name = :tabname
                and atc.owner = :ownname
                and dm.filetype (+)= :ftype
            order by 1,2,3 '''
        cursor = self.dbh.cursor()
        params = {
            'ftype':self.filetype,
            'tabname':self.targettable,
            'ownname':self.schema
            }

        cursor.execute(sqlstr,params)
        records = cursor.fetchall()
        for rec in records:
            hdr = None

            if rec[0] == None:
                hdr = self.objhdu
            elif rec[0].upper() == 'PRIMARY':
                hdr = 0
            else:
                if ingestutils.isInteger(rec[0]):
                    hdr = int(rec[0])
                else:
                    hdr = rec[0]

            if hdr not in results:
                results[hdr] = OrderedDict()

            if rec[1] not in results[hdr]:
                results[hdr][rec[1]] = [[rec[3]],rec[4],rec[5],[str(rec[2])]]
            else:
                results[hdr][rec[1]][self.COLUMN_NAME].append(rec[3])
                results[hdr][rec[1]][self.POSITION].append(str(rec[2]))
        cursor.close()
        self.checkForArrays(results)
        return results


    def checkForArrays(self,records):
        results = OrderedDict()

        pat = re.compile('^(.*)_(\d*)$',re.IGNORECASE)
        if self.objhdu in records:
            for k, v in records[self.objhdu].iteritems():
                attrname = None
                pos = 0
                m = pat.match(k)
                if m:
                    attrname = m.group(1)
                    pos = m.group(2)
                    if attrname not in results:
                        results[attrname] = [[k],v[1],v[2],[str(int(pos)-1)]]
                    else:
                        results[attrname][self.COLUMN_NAME].append(k)
                        results[attrname][self.POSITION].append(str(int(pos)-1))
                else:
                    results[k]=v
            records[self.objhdu] = results


    def getConstValuesFromHeader(self, hduName):
        value = None
        quoteit = None
        hdr = self.fits[hduName].read_header()
        
        for attribute, dblist in self.dbDict[hduName].iteritems():
            for col in dblist[self.COLUMN_NAME]:
                if dblist[self.DERIVED] == 'c':
                    value = self.funcDict[col](hdr[attribute])
                elif dblist[self.DERIVED] == 'h':
                    value = str(hdr[attribute]).strip()
                if dblist[self.DATATYPE] == 'char':
                    quoteit = True
                else:
                    quoteit = False
                self.constDict[col] = [value,quoteit]


    def writeControlfileHeader(self,controlfile):
        controlfile.write('LOAD DATA\n')
        controlfile.write('INFILE "-"\n')
        schtbl = self.schema + '.' + self.targettable
        controlfile.write("INTO TABLE " + schtbl + "\nAPPEND\n" +
            "FIELDS TERMINATED BY ','\n(\n")

        for colname, val in self.constDict.iteritems():
            if val[self.QUOTE]:
                controlfile.write(colname + " CONSTANT '" + val[self.VALUE] + "',\n")
            else:
                controlfile.write(colname + " CONSTANT " + str(val[self.VALUE]) + ",\n")



    def writeControlfileFooter(self,controlfile):
        # append COADD_OBJECT_ID spec to end of column list
        controlfile.write(",\nCOADD_OBJECT_ID integer external\n")

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


    def createControlFile(self):
        controlfile = file(self.controlfilename, 'w')
        for hduName in self.dbDict.keys():
            if hduName not in [self.objhdu,'WCL']:
                self.getConstValuesFromHeader(hduName)
        self.writeControlfileHeader(controlfile)
        dbobjdata = self.dbDict[self.objhdu]
        orderedFitsColumns = self.fits[self.objhdu].get_colnames()
        filerows = []
        for headerName in orderedFitsColumns:
            if headerName.upper() in dbobjdata.keys():
                for colname in dbobjdata[headerName.upper()][self.COLUMN_NAME]:
                    row = []
                    row.append(colname)
                    row.append(dbobjdata[headerName.upper()][self.DATATYPE])
                    filerows.append(" ".join(row))

        controlfile.write(",\n".join(filerows))
        self.writeControlfileFooter(controlfile)
        controlfile.close()
        self.info("sqlldr control file " + self.controlfilename + " created")


    def getSqlldrCommand(self):
        connectinfo = serviceaccess.parse(None,None,'DB')
        connectstring = connectinfo["user"] + "/" + connectinfo["passwd"] + "@" + connectinfo["name"]
        sqlldr_command = []
        sqlldr_command.append("sqlldr")
        sqlldr_command.append(connectstring)
        sqlldr_command.append("control=" + self.controlfilename)
        sqlldr_command.append("bad=" + self.badrowsfile)
        sqlldr_command.append("discard=" + self.discardfile)
        sqlldr_command.append("parallel=true")
        sqlldr_command.append("DIRECT=true")
        #sqlldr_command.append("rows=10000")
        sqlldr_command.append("silent=header,feedback,partitions")
        return sqlldr_command


    def executeIngest(self,firstrow=1,lastrow=-1):
        if lastrow == -1:
            lastrow = self.fits[self.objhdu].get_nrows()

        attrsToCollect = self.dbDict[self.objhdu]

        sqlldr = None
        outfile = None

        try:
            self.info("invoking sqlldr with control file " + self.controlfilename)
            sqlldr_command = self.getSqlldrCommand()
            sqlldr = subprocess.Popen(sqlldr_command,shell=False,stdin=subprocess.PIPE)

            attrs = attrsToCollect.keys()
            orderedFitsColumns = []

            allcols = self.fits[self.objhdu].get_colnames()

            for col in allcols:
                # need NUMBER to look up COADD_OBJECT_ID, will be skipped below
                if col.upper() in attrs or col.upper() == "NUMBER":
                    orderedFitsColumns.append(col)

            datatypes = self.fits[self.objhdu].get_rec_dtype()[0]

            startrow = firstrow-1
            endrow = firstrow-1

            while endrow < lastrow:
                startrow = endrow
                endrow = min(startrow+10000, lastrow)

                data = fitsio.read(
                        self.fullfilename,
                        rows=range(startrow,endrow),
                        columns=orderedFitsColumns,ext=self.objhdu
                        )

                for row in data:
                    outrow = []
                    coadd_object_id = None

                    for idx in range(0,len(orderedFitsColumns)):
                        # if this is NUMBER column, look up COADD_OBJECT_ID and
                        # then skip it
                        if orderedFitsColumns[idx].upper() == "NUMBER":
                            coadd_object_id = self.idDict[str(row[idx])]
                            continue

                        # if this column is an array of values
                        if datatypes[orderedFitsColumns[idx].upper()].subdtype:
                            arrvals = []
                            for pos in attrsToCollect[orderedFitsColumns[idx]][self.POSITION]:
                                arrvals.append(str(row[idx][int(pos)]).strip())
                            outrow.append(','.join(arrvals))
                        # else it is a scalar
                        else:
                            outrow.append(str(row[idx]))

                    # manually append COADD_OBJECT_ID to end of row
                    outrow.append(coadd_object_id)

                    # if sqlldr subprocess is still alive, write to it
                    if sqlldr and sqlldr.poll() == None:
                        sqlldr.stdin.write(",".join(outrow) + "\n")
                    # otherwise some error occurred
                    else:
                        exit("sqlldr exited with errors. See " + self.logfile +
                             ", " + self.discardfile + " and " + self.badrowsfile + " for details")
                # end for row in data
            # end while endrow < lastrow
        finally:
            sqlldr.stdin.close()

        if sqlldr and sqlldr.wait():
            exit("sqlldr exited with errors. See " + self.logfile + ", " + self.discardfile +
                 " and " + self.badrowsfile + " for details")
        else:
            if os.path.exists(self.controlfilename):
                os.remove(self.controlfilename)
            if os.path.exists(self.logfile):
                os.remove(self.logfile)
            if os.path.exists(self.badrowsfile):
                os.remove(self.badrowsfile)
            if os.path.exists(self.discardfile):
                os.remove(self.discardfile)


    def numAlreadyIngested(self):
        results = OrderedDict()
        sqlstr = '''
            select count(*), coadd_object_id 
            from %s
            where filename=:fname
            group by coadd_object_id
            '''
        cursor = self.dbh.cursor()
        schtbl = self.schema + '.' + self.targettable
        cursor.execute(sqlstr % schtbl,{"fname":self.shortfilename})
        records = cursor.fetchall()
        
        if(len(records) > 0):
            return records[0]
        else:
            return [0,0]


    def getNumObjects(self):
        return self.fits[self.objhdu].get_nrows()


    def isLoaded(self):
        self.debug("starting isLoaded()")
        loaded = False
        exitcode = 0

        self.debug("starting numAlreadyIngested()")

        (numDbObjects,dbCoaddObjectsId) = self.numAlreadyIngested()
        self.debug("starting getNumObjects()")
        numCatObjects = self.getNumObjects()
        if numDbObjects > 0:
            loaded = True
            if numDbObjects == numCatObjects:
                self.info("WARNING: file " + self.fullfilename + " already ingested " +
                    "with the same number of objects. " +
                    "Original global ID=" + str(dbCoaddObjectsId) + ". Aborting new ingest")
                exitcode = 0
            else:
                errstr = ("ERROR: file " + self.fullfilename + " already ingested, but " +
                    "the number of objects is DIFFERENT: catalog=" + 
                    str(numCatObjects) + "; DB=" + str(numDbObjects) + 
                    ", Original reqnum=" + str(dbCoaddObjectsId))
                raise Exception(errstr)
                exitcode = 1

        self.debug("finished isLoaded()")
        return (loaded,exitcode)