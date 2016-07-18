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


class CoaddCatalog:

    COLUMN_NAME = 0
    POSITION = 3

    dbh = None
    schema = None
    fits = None
    objhdu = 'OBJECTS'
    filetype = 'coadd_cat'

    # db tables, sequences, etc
    targettable = 'COADD_OBJECT'
    catalogtable = 'CATALOG'
    idsequence = 'COADD_OBJECT_SEQ'

    fullfilename = None
    shortfilename = None

    # data retrieved from catalogtable
    band = None
    tilename = None
    pfw_attempt_id = None

    # dictionary of coadd_object table columns in db
    dbDict = None

    # dictionary of coadd object ids, set from parameter given to __init__ 
    idDict = {}

    debug = True
    debugDateFormat = '%Y-%m-%d %H:%M:%S'


    def __init__(self, ingesttype, datafile, idDict):

        self.debug("start CoaddIngest.init() for file %s" % datafile)
        self.dbh = desdbi.DesDbi()

        self.debug("opening fits file")
        self.fits = fitsio.FITS(datafile)
        self.debug("fits file opened")

        # dictionary of coadd object ids, set from parameter given to __init__ 
        self.idDict = idDict

        # grab current schema by resolving from table. odd, but works.
        self.debug("start resolveDbObject() for table: %s" % self.targettable)
        (self.schema,self.targettable) = ingestutils.resolveDbObject(self.targettable,self.dbh)
        self.debug("schema, targettable = %s, %s" % (self.schema, self.targettable))

        self.fullfilename = datafile
        self.shortfilename = ingestutils.getShortFilename(datafile)

        # grab the band, tile, and pfw_attempt_id for this file
        self.setCatalogInfo(ingesttype)

        self.debug("start getObjectColumns()")
        self.dbDict = self.getObjectColumns()
        self.debug("CoaddIngest.init() done")


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


    ###########################################################################
    #
    # setCatalogInfo:
    #
    # Grab info from catalog table based on the filename, and set as class
    # variables.
    #
    ###########################################################################
    def setCatalogInfo(self, ingesttype):
        sqlstr = '''
            select band, tilename, pfw_attempt_id
            from %s
            where filename=:fname
            '''
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr % self.catalogtable,{"fname":self.shortfilename})
        records = cursor.fetchall()

        if(len(records) > 0):
            (self.band, self.tilename, self.pfw_attempt_id) = records[0]

            # band won't be set for detection image, set band to 'det'
            if ingesttype == 'det':
                self.band = 'det'
            elif self.band == None:
                exit("Can't determine band for file: " + self.shortfilename)

            if self.tilename == None:
                exit("Can't determine tilename for file: " + self.shortfilename)

            if self.pfw_attempt_id == None:
                exit("Can't determine pfw_attempt_id for file: " + self.shortfilename)
        else:
            exit("Can't determine catalog info for file: " + self.shortfilename)


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


    ###########################################################################
    #
    # executeIngest:
    #
    # Ingest all of the data from one FITS file into coadd_object table.
    #
    ###########################################################################
    def executeIngest(self,firstrow=1,lastrow=-1):
        if lastrow == -1:
            lastrow = self.fits[self.objhdu].get_nrows()

        attrsToCollect = self.dbDict[self.objhdu]

        cursor = self.dbh.cursor()

        # array of arrays used to fill bind variables for executemany()
        sqldata = []

        # retrieve all coadd objects ids needed for this band's ingest in one go
        self.info("grabbing block of coadd object ids")
        coadd_recs = self.getCoaddObjectIds(self.fits[self.objhdu].get_nrows())
        coadd_ids = [item[0] for item in coadd_recs]

        try:
            self.info("gathering FITS data")

            attrs = attrsToCollect.keys()
            orderedFitsColumns = []

            allcols = self.fits[self.objhdu].get_colnames()

            # matching up coadd_object column names with FITS attributes
            for col in allcols:
                if col.upper() in attrs:
                    orderedFitsColumns.append(col)

            # FITS attribute datatypes
            datatypes = self.fits[self.objhdu].get_rec_dtype()[0]

            startrow = firstrow-1
            endrow = firstrow-1

            # grab FITS data in 10000-row chunks, and build array of arrays
            # of the values in each row
            while endrow < lastrow:
                startrow = endrow
                endrow = min(startrow+10000, lastrow)

                data = fitsio.read(
                        self.fullfilename,
                        rows=range(startrow,endrow),
                        columns=orderedFitsColumns,ext=self.objhdu
                        )

                for row in data:
                    # IMPORTANT! Must convert numpy array to python list, or
                    # suffer big performance hit. This is due to numpy bug
                    # fixed in more recent version.
                    row = row.tolist()

                    # array to hold values for this FITS row
                    outrow = []

                    # process each value, and add to the outrow array
                    for idx in range(0,len(orderedFitsColumns)):
                        # insert db-generated id at the beginning of the list
                        if orderedFitsColumns[idx] == "NUMBER":
                            if self.idDict.has_key(row[idx]):
                                outrow.insert(0, self.idDict[row[idx]])
                            else:
                                coadd_id = coadd_ids.pop()
                                self.idDict[row[idx]] = coadd_id
                                outrow.insert(0, coadd_id)

                            outrow.append(row[idx])

                        # if this column is an array of values
                        elif datatypes[orderedFitsColumns[idx]].subdtype:
                            arrvals = row[idx]

                            # convert the array to a python list, and append
                            arrvals = arrvals.tolist()
                            for elem in arrvals:
                                outrow.append(elem)
                        # else it is a scalar
                        else:
                            outrow.append(row[idx])

                    # add this array of values to the sql data for bind vars
                    sqldata.append(outrow)

                    #self.info("outrow: " + ",".join(map(str, outrow)))
                # end for row in data
            # end while endrow < lastrow
        finally:
            self.info("constructing SQL string")

            # build sql string for the bind variable inserts, starting with
            # the static part
            sqlstr = '''
                insert into %s (
                    BAND,
                    TILENAME,
                    FILENAME,
                    PFW_ATTEMPT_ID,
                    ID,
                    OBJECT_NUMBER,
                    FLAGS,
                    PARENT_NUMBER,
                    DURATION_ANALYSIS,
                    X_IMAGE,
                    Y_IMAGE,
                    XMIN_IMAGE,
                    XMAX_IMAGE,
                    YMIN_IMAGE,
                    YMAX_IMAGE,
                    X_WORLD,
                    Y_WORLD,
                    XWIN_IMAGE,
                    YWIN_IMAGE,
                    ERRAWIN_IMAGE,
                    ERRBWIN_IMAGE,
                    ERRTHETAWIN_IMAGE,
                    ALPHAWIN_J2000,
                    DELTAWIN_J2000,
                    ERRAWIN_WORLD,
                    ERRBWIN_WORLD,
                    ERRTHETAWIN_J2000,
                    XPEAK_IMAGE,
                    YPEAK_IMAGE,
                    ALPHAPEAK_J2000,
                    DELTAPEAK_J2000,
                    A_IMAGE,
                    B_IMAGE,
                    THETA_IMAGE,
                    A_WORLD,
                    B_WORLD,
                    THETA_J2000,
                    ELONGATION,
                    FLUX_RADIUS,
                    MAG_ISO,
                    MAGERR_ISO,
                    ISOAREA_IMAGE,
                    ISOAREAF_IMAGE,
                    ISOAREA_WORLD,
                    ISOAREAF_WORLD,
                    FLUX_APER_1,
                    FLUX_APER_2,
                    FLUX_APER_3,
                    FLUX_APER_4,
                    FLUX_APER_5,
                    FLUX_APER_6,
                    FLUX_APER_7,
                    FLUX_APER_8,
                    FLUX_APER_9,
                    FLUX_APER_10,
                    FLUX_APER_11,
                    FLUX_APER_12,
                    FLUXERR_APER_1,
                    FLUXERR_APER_2,
                    FLUXERR_APER_3,
                    FLUXERR_APER_4,
                    FLUXERR_APER_5,
                    FLUXERR_APER_6,
                    FLUXERR_APER_7,
                    FLUXERR_APER_8,
                    FLUXERR_APER_9,
                    FLUXERR_APER_10,
                    FLUXERR_APER_11,
                    FLUXERR_APER_12,
                    MAG_APER_1,
                    MAG_APER_2,
                    MAG_APER_3,
                    MAG_APER_4,
                    MAG_APER_5,
                    MAG_APER_6,
                    MAG_APER_7,
                    MAG_APER_8,
                    MAG_APER_9,
                    MAG_APER_10,
                    MAG_APER_11,
                    MAG_APER_12,
                    MAGERR_APER_1,
                    MAGERR_APER_2,
                    MAGERR_APER_3,
                    MAGERR_APER_4,
                    MAGERR_APER_5,
                    MAGERR_APER_6,
                    MAGERR_APER_7,
                    MAGERR_APER_8,
                    MAGERR_APER_9,
                    MAGERR_APER_10,
                    MAGERR_APER_11,
                    MAGERR_APER_12,
                    FLUX_AUTO,
                    FLUXERR_AUTO,
                    MAG_AUTO,
                    MAGERR_AUTO,
                    KRON_RADIUS,
                    FLUX_PETRO,
                    FLUXERR_PETRO,
                    MAG_PETRO,
                    MAGERR_PETRO,
                    PETRO_RADIUS,
                    BACKGROUND,
                    THRESHOLD,
                    MU_THRESHOLD,
                    FLUX_MAX,
                    MU_MAX,
                    MAG_HYBRID,
                    MAGERR_HYBRID,
                    FLUX_HYBRID,
                    FLUXERR_HYBRID,
                    XPSF_IMAGE,
                    YPSF_IMAGE,
                    ERRAPSF_IMAGE,
                    ERRBPSF_IMAGE,
                    ERRTHETAPSF_IMAGE,
                    ALPHAPSF_J2000,
                    DELTAPSF_J2000,
                    ERRAPSF_WORLD,
                    ERRBPSF_WORLD,
                    ERRTHETAPSF_J2000,
                    FLUX_PSF,
                    FLUXERR_PSF,
                    MAG_PSF,
                    MAGERR_PSF,
                    NITER_PSF,
                    CHI2_PSF,
                    XMODEL_IMAGE,
                    YMODEL_IMAGE,
                    ERRAMODEL_IMAGE,
                    ERRBMODEL_IMAGE,
                    ERRTHETAMODEL_IMAGE,
                    ALPHAMODEL_J2000,
                    DELTAMODEL_J2000,
                    ERRAMODEL_WORLD,
                    ERRBMODEL_WORLD,
                    ERRTHETAMODEL_J2000,
                    AMODEL_IMAGE,
                    BMODEL_IMAGE,
                    THETAMODEL_IMAGE,
                    AMODEL_WORLD,
                    BMODEL_WORLD,
                    THETAMODEL_J2000,
                    ELLIP1MODEL_IMAGE,
                    ELLIP2MODEL_IMAGE,
                    ELLIP1MODEL_WORLD,
                    ELLIP2MODEL_WORLD,
                    FLUX_MODEL,
                    FLUXERR_MODEL,
                    MAG_MODEL,
                    MAGERR_MODEL,
                    MU_MAX_MODEL,
                    MU_EFF_MODEL,
                    MU_MEAN_MODEL,
                    SPREAD_MODEL,
                    SPREADERR_MODEL,
                    CLASS_STAR,
                    FLUX_DISK,
                    FLUXERR_DISK,
                    MAG_DISK,
                    MAGERR_DISK,
                    DISK_SCALE_IMAGE,
                    DISK_SCALEERR_IMAGE,
                    DISK_SCALE_WORLD,
                    DISK_SCALEERR_WORLD,
                    DISK_ASPECT_IMAGE,
                    DISK_ASPECTERR_IMAGE,
                    DISK_ASPECT_WORLD,
                    DISK_ASPECTERR_WORLD,
                    DISK_THETA_IMAGE,
                    DISK_THETAERR_IMAGE,
                    DISK_THETA_J2000,
                    DISK_THETAERR_WORLD,
                    CHI2_MODEL,
                    NITER_MODEL,
                    FLAGS_WEIGHT,
                    NLOWWEIGHT_ISO,
                    NLOWDWEIGHT_ISO,
                    FWHMPSF_IMAGE,
                    FWHMPSF_WORLD,
                    ISO0,
                    ISO1,
                    ISO2,
                    ISO3,
                    ISO4,
                    ISO5,
                    ISO6,
                    ISO7,
                    FLUX_DETMODEL,
                    FLUXERR_DETMODEL,
                    MAG_DETMODEL,
                    MAGERR_DETMODEL,
                    FLAGS_DETMODEL,
                    CHI2_DETMODEL,
                    X2_IMAGE,
                    Y2_IMAGE,
                    XY_IMAGE,
                    ERRX2_IMAGE,
                    ERRY2_IMAGE,
                    ERRXY_IMAGE,
                    X2WIN_IMAGE,
                    Y2WIN_IMAGE,
                    XYWIN_IMAGE,
                    ERRX2WIN_IMAGE,
                    ERRY2WIN_IMAGE,
                    ERRXYWIN_IMAGE,
                    FWHM_IMAGE,
                    FWHM_WORLD,
                    IMAFLAGS_ISO
                ) values (
                ''' % self.targettable

            # add constant band, tile, and filename info to sql string
            sqlstr = sqlstr + "'" + self.band + "', '" + self.tilename + "', '" + self.shortfilename + "', " + str(self.pfw_attempt_id) + ", "

            # add bind variable placeholders to sql string
            for i in range(1, len(sqldata[0])+1):
                if i == len(sqldata[0]):
                    sqlstr += ":%d)" % i
                else:
                    sqlstr += ":%d," % i

            #self.info("sqlstr: " + sqlstr)

            self.info("inserting rows")

            # prepare sql string cursor, and execute for each row of FITS values
            # contained in the sqldata array of arrays
            cursor.prepare(sqlstr)
            cursor.executemany(None, sqldata)
            cursor.close()
            self.dbh.commit()
            self.info("row inserts complete")


    ###########################################################################
    #
    # getCoaddObjectsIds:
    #
    # Get block of coadd object ids from db. Number of ids needed is specified
    # by numobjs
    #
    ###########################################################################
    def getCoaddObjectIds(self, numobjs):
        # tricky Oracle sql to get block all at once
        sqlstr = '''
            select %s.nextval from dual
            connect by level < %d
            '''
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr % (self.idsequence, (numobjs+1)))
        records = cursor.fetchall()

        if(len(records) > 0):
            return records
        else:
            return 0


    def numAlreadyIngested(self):
        sqlstr = '''
            select count(*)
            from %s
            where filename=:fname
            '''
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr % self.targettable,{"fname":self.shortfilename})
        count = cursor.fetchone()[0]
        
        return count


    def getNumObjects(self):
        return self.fits[self.objhdu].get_nrows()


    def isLoaded(self):
        self.debug("starting isLoaded()")
        loaded = False
        exitcode = 0

        self.debug("starting numAlreadyIngested()")

        numDbObjects = self.numAlreadyIngested()
        self.debug("starting getNumObjects()")
        numCatObjects = self.getNumObjects()
        if numDbObjects > 0:
            loaded = True
            if numDbObjects == numCatObjects:
                self.info("WARNING: file " + self.fullfilename + 
                          " already ingested with the same number of" +
                          " objects. Aborting new ingest.")
                exitcode = 0
            else:
                errstr = ("ERROR: file " + self.fullfilename +
                          " already ingested, but the number of objects is" +
                          " DIFFERENT: catalog=" + str(numCatObjects) +
                          "; DB=" + str(numDbObjects) + ".")
                raise Exception(errstr)
                exitcode = 1

        self.debug("finished isLoaded()")
        return (loaded,exitcode)
