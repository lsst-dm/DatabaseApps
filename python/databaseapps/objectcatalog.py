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
import re
from collections import OrderedDict
from despydb import desdbi
from despyserviceaccess import serviceaccess
from databaseapps.ingestutils import IngestUtils as ingestutils
import argparse

class Timing(object):
    def __init__(self, name):
        self.start = time.time()
        self.name = name
    
    def end(self):
        return "TIMING: %s finished in %.2f seconds" % (self.name, time.time()-self.start)

class ObjectCatalog:

    COLUMN_NAME = 0
    DERIVED = 1
    DATATYPE = 2
    POSITION = 3
    VALUE = 0
    QUOTE = 1

    dbh = None
    request = None
    fullfilename = None
    shortfilename = None
    filetype = None
    temptable = None
    tempschema = None
    targettable = None
    targetschema = None
    dump = False
    outputfile = 'dataset.dat'
    objhdu = 'LDAC_OBJECTS'
    controlfilename = 'catingest.ctl'
    logfile = 'catingest.log'
    badrowsfile = 'badrows.bad'
    discardfile = 'discarded.bad'
    keepfiles = False

    constDict = None
    funcDict = None
    dbDict = None
    fits = None
    sqlldr_opts = None
    dodebug = True
    debugDateFormat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, request, filetype, datafile, temptable, targettable,
                    fitsheader, outputfile, dumponly, keepfiles,sqlldr_opts):
        
        self.debug("start CatalogIngest.init()")
        self.dbh = desdbi.DesDbi()

        self.debug("opening fits file")
        self.fits = fitsio.FITS(datafile)
        self.debug("fits file opened")

        self.request = request
        self.filetype = filetype
        self.fullfilename = datafile
        self.shortfilename = ingestutils.getShortFilename(datafile)

        if fitsheader:
            if ingestutils.isInteger(fitsheader):
                self.objhdu = int(fitsheader)
            else:
                self.objhdu = fitsheader
        if outputfile:
            self.outputfile = outputfile
        if dumponly:
            self.dump = True
        else:
            self.dump = False
        if keepfiles:
            self.keepfiles = True
        if sqlldr_opts:
            self.sqlldr_opts = sqlldr_opts

        self.debug("start resolveDbObject() for target: %s" % targettable)
        (self.targetschema,self.targettable) = ingestutils.resolveDbObject(targettable,self.dbh)
        if not temptable:
            self.temptable = "DESSE_REQNUM%07d" % int(request)
            self.tempschema = self.targetschema
        else:
            self.debug("start resolveDbObject() for temp: %s" % temptable)
            (self.tempschema,self.temptable) = ingestutils.resolveDbObject(temptable,self.dbh)
        self.debug("target schema,table = %s, %s; temp= %s, %s" % 
                (self.targetschema,self.targettable,self.tempschema,self.temptable))

        if self.dump:
            self.constDict = {}
        else:
            self.constDict = {
                "FILENAME":[self.shortfilename,True], 
                "REQNUM":[request,False]
                }
        self.debug("start getObjectColumns()")
        self.dbDict = self.getObjectColumns()
        self.debug("CatalogIngest.init() done")

    def __del__(self):
        if self.dbh:
            self.dbh.close()
        if self.fits:
            self.fits.close()

    def debug(self, msg):
        if self.dodebug:
            print time.strftime(self.debugDateFormat) + " - " + msg

    def info(self, msg):
        print time.strftime(self.debugDateFormat) + " - " + msg

    def getObjectColumns(self):
        results = OrderedDict()
        sqlstr = '''
            select hdu, UPPER(attribute_name), NVL(position,0), 
                column_name, NVL(derived,'h'),
                case when datafile_datatype='int' THEN 'integer external'
                    when datafile_datatype='float' THEN 'float external'
                    when datafile_datatype='double' THEN 'decimal external'
                    when datafile_datatype='char' THEN 'char'
                end sqlldr_type
            from ops_datafile_metadata 
            where filetype = :ftype
            order by 1,2,3 '''
        cursor = self.dbh.cursor()
        params = {
            'ftype':self.filetype,
            }
        cursor.execute(sqlstr,params)
        records = cursor.fetchall()
        #print records
        if len(records) == 0:
            exit("No columns listed for filetype %s in ops_datafile_metadata, exiting" % (self.filetype))
        for rec in records:
            #print rec
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

    def loadingTarget(self):
        if self.targettable == self.temptable and self.targetschema == self.tempschema:
            return True
        else:
            return False

    def writeControlfileHeader(self,controlfile):
        if self.dump or not self.loadingTarget():
            controlfile.write('UNRECOVERABLE\n')
        controlfile.write('LOAD DATA\n')
        controlfile.write('INFILE *\n')
        schtbl = self.tempschema + '.' + self.temptable
        controlfile.write("INTO TABLE " + schtbl + "\nAPPEND\n" +
            "FIELDS TERMINATED BY ','\n(\n")
        for colname, val in self.constDict.iteritems():
            if val[self.QUOTE]:
                controlfile.write(colname + " CONSTANT '" + val[self.VALUE] + "',\n")
            else:
                controlfile.write(colname + " CONSTANT " + str(val[self.VALUE]) + ",\n")


    def writeControlfileFooter(self,controlfile):
        controlfile.write(")\n")

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
        controlfile.write("BEGINDATA\n")
        lastrow = self.fits[self.objhdu].get_nrows()
        attrsToCollect = self.dbDict[self.objhdu]


        attrs = attrsToCollect.keys()
        orderedFitsColumns = []
        allcols = self.fits[self.objhdu].get_colnames()
        for col in allcols:
            if col.upper() in attrs:
                orderedFitsColumns.append(col)
        datatypes = self.fits[self.objhdu].get_rec_dtype()[0]
        startrow = 0
        endrow = 0
        self.info("Starting control file write")
        ctlwr = Timing("Control file creation")
        while endrow < lastrow:
            startrow = endrow
            endrow = min(startrow+50000, lastrow)
            ss = time.time()
            data = fitsio.read(
                    self.fullfilename,
                    rows=range(startrow,endrow),
                    columns=orderedFitsColumns,ext=self.objhdu
                    )
            for row in data:
                outrow = []
                for idx in range(0,len(orderedFitsColumns)):
                    # if this column is an array of values
                    if datatypes[orderedFitsColumns[idx].upper()].subdtype:
                        arrvals = []
                        for pos in attrsToCollect[orderedFitsColumns[idx]][self.POSITION]:
                            arrvals.append(str(row[idx][int(pos)]).strip())
                        outrow.append(','.join(arrvals))
                    # else it is a scalar
                    else:
                        outrow.append(str(row[idx]))
                # else if we are writing to a file
                controlfile.write(",".join(outrow) + "\n")
            # end for row in data
        # end while endrow < lastrow
        self.info(ctlwr.end())
        controlfile.close()
        self.info("sqlldr control file " + self.controlfilename + " created")


    def getSqlldrCommand(self):
        connectinfo = serviceaccess.parse(None,None,'DB')
        connectstring = "userid=" + connectinfo["user"] + "@\"\(DESCRIPTION=\(ADDRESS=\(PROTOCOL=TCP\)\(HOST=" + connectinfo["server"] + "\)\(PORT=" + connectinfo["port"] + "\)\)\(CONNECT_DATA=\(SERVER=dedicated\)\(SERVICE_NAME=" + connectinfo["name"] + "\)\)\)\"/PASSWD"

        sqlldr_command = []
        sqlldr_command.append("sqlldr")
        sqlldr_command.append(connectstring)
        sqlldr_command.append("control=" + self.controlfilename)
        sqlldr_command.append("bad=" + self.badrowsfile)
        sqlldr_command.append("discard=" + self.discardfile)
        if self.sqlldr_opts:
            temp = self.sqlldr_opts.split()
            for t in temp:
                sqlldr_command.append(t)
        return sqlldr_command,connectinfo


    def executeIngest(self):
        sqlldr = None
        MAXTRIES = 5
        count = 1
        while count <= MAXTRIES:
            sqlldrtime = Timing('sqlldr ingest')
            try:
                self.info("invoking sqlldr with control file " + self.controlfilename)
                sqlldr_command,connectinfo = self.getSqlldrCommand()
                self.info(" with the command line " + " ".join(sqlldr_command))
                for i,cmd in enumerate(sqlldr_command):
                    if 'userid' in cmd:
                        sqlldr_command[i] = cmd.replace('PASSWD',connectinfo["passwd"])
                        break
                del connectinfo
                sqlldr = subprocess.Popen(sqlldr_command,shell=False)
                count += 1
            except:
                if count == MAXTRIES:
                    raise
   
            if sqlldr and sqlldr.wait():
                if sqlldr.returncode == 2:
                    break  # do not retry on warning
                print "sqlldr exited with non-zero status, try %i/%i. See " % (count,MAXTRIES) + self.logfile + ", " + self.discardfile + \
                      " and " + self.badrowsfile + " for details"
                if count < MAXTRIES:
                    print "  Retrying"
                else:
                    exit(sqlldr.returncode)
                count += 1
                time.sleep(10)
            elif sqlldr: # everything ended normally
                break
        self.info(sqlldrtime.end())
        if not self.keepfiles:
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
            select count(*), reqnum 
            from %s
            where filename=:fname
            group by reqnum
            '''
        count = 0
        while count < 5:
            count += 1
            try:
                cursor = self.dbh.cursor()
                schtbl = self.targetschema + '.' + self.targettable
                cursor.execute(sqlstr % schtbl,{"fname":self.shortfilename})
                records = cursor.fetchall()
        
                if(len(records) > 0):
                    return records[0]
                else:
                    return [0,0]
            except:
                if count == 5:
                    raise
                time.sleep(10)  # sleep 10 seconds and retry


    def getNumObjects(self):
        return self.fits[self.objhdu].get_nrows()

    def getOutputFilename(self):
        return self.outputfile

    def createIngestTable(self):
        tablespace = "DESSE_REQNUM%07d_T" % int(self.request)
        
        cursor = self.dbh.cursor()
        self.info("Creating tablespace %s and table %s.%s if they do not already exist"
                 % (tablespace,self.tempschema,self.temptable))
        cursor.callproc("createObjectsTable",
            [self.temptable,tablespace,self.targettable])
        cursor.close()
        self.info("Temp table %s.%s is ready" % (self.tempschema,self.temptable))


    def isLoaded(self):
        self.debug("starting isLoaded()")
        loaded = False
        exitcode = 0
        # short circuit the checking of loaded objects until a better query can be devised.
        return (loaded,exitcode)
        if self.dump:
            self.debug("dump=True so skipping isLoaded() check")
        else:
            self.debug("starting numAlreadyIngested()")
            (numDbObjects,dbReqnum) = self.numAlreadyIngested()
            self.debug("starting getNumObjects()")
            numCatObjects = self.getNumObjects()
            if numDbObjects > 0:
                loaded = True
                if numDbObjects == numCatObjects:
                    self.info("WARNING: file " + self.fullfilename + " already ingested " +
                        "with the same number of objects. " +
                        "Original reqnum=" + str(dbReqnum) + ". Aborting new ingest")
                    exitcode = 0
                else:
                    errstr = ("ERROR: file " + self.fullfilename + " already ingested, but " +
                        "the number of objects is DIFFERENT: catalog=" + 
                        str(numCatObjects) + "; DB=" + str(numDbObjects) + 
                        ", Original reqnum=" + str(dbReqnum))
                    raise Exception(errstr)
                    exitcode = 1
        self.debug("finished isLoaded()")
        return (loaded,exitcode)


