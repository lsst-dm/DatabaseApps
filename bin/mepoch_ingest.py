#!/usr/bin/env python

import sys
import time
from despydb import desdbi
import argparse
import traceback
from databaseapps.CoaddCatalog import CoaddCatalog
from databaseapps.CoaddHealpix import CoaddHealpix
from databaseapps.Mangle import Mangle
from databaseapps.Wavg import Wavg
from databaseapps.Extinction import Extinction

def checkParam(args,param,required):
    """ Check that a parameter exists, else return None

    """
    if args[param]:
        return args[param]
    else:
        if required:
            sys.stderr.write("Missing required parameter: %s\n" % param)
        else:
            return None

def printinfo(msg):
    """ Generic print statement with time stamp

    """
    print time.strftime(CoaddCatalog.debugDateFormat) + " - " + msg

def getfilelist(file):
    """ Convert a comma separated list of items in a file into a list

    """
    files = []
    f = open(file, 'r')
    lines = f.readlines()
    for line in lines:
        files.append(line.split(","))
        files[-1][-1] = files[-1][-1].strip()
    f.close()
    return files

if __name__ == '__main__':

    # var to hold to COADD_OBJECT_ID's
    coaddObjectIdDict = {}

    retval = 0

    parser = argparse.ArgumentParser(description='Ingest coadd objects from fits catalogs')
    parser.add_argument('--bandcat_list', action='store')
    parser.add_argument('--detcat', action='store', required=True)
    parser.add_argument('--extinct', action='store')
    parser.add_argument('--extinct_band_list', action='store')
    parser.add_argument('--healpix', action='store')
    parser.add_argument('--wavg_list', action='store')
    parser.add_argument('--wavg_oclink_list', action='store')
    parser.add_argument('--ccdgon_list', action='store')
    parser.add_argument('--molygon_list', action='store')
    parser.add_argument('--molygon_ccdgon_list', action='store')
    parser.add_argument('--coadd_object_molygon_list', action='store')
    parser.add_argument('--section', '-s', help='db section in the desservices file')
    parser.add_argument('--des_services', help='desservices file')
    parser.add_argument('--coadd_object_filetype', action='store', default='coadd_cat')
    parser.add_argument('--coadd_hpix_filetype', action='store', default='coadd_hpix')
    parser.add_argument('--wavg_filetype', action='store', default='coadd_wavg')
    parser.add_argument('--wavg_oclink_filetype', action='store', default='coadd_wavg_oclink')
    parser.add_argument('--ccdgon_filetype', action='store', default='mangle_csv_ccdgon')
    parser.add_argument('--molygon_filetype', action='store', default='mangle_csv_molygon')
    parser.add_argument('--molygon_ccdgon_filetype', action='store', default='mangle_csv_molyccd')
    parser.add_argument('--coadd_object_molygon_filetype', action='store', default='mangle_csv_cobjmoly')
    parser.add_argument('--extinct_filetype', action='store', default='coadd_extinct_ebv')
    parser.add_argument('--extinct_band_filetype', action='store', default='coadd_extinct_band')
    parser.add_argument('--alt_section', action='store')
    parser.add_argument('--det_pfwid', action='store')
    parser.add_argument('--alt_table', action='store')


    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    bandcat = checkParam(args,'bandcat_list',False)
    detcat = checkParam(args,'detcat',True)
    extinct = checkParam(args,'extinct',False)
    extinct_band = checkParam(args, 'extinct_band_list', False)
    healpix = checkParam(args,'healpix',False)
    wavg = checkParam(args,'wavg_list',False)
    wavg_oclink = checkParam(args,'wavg_oclink_list',False)
    ccdgon = checkParam(args,'ccdgon_list',False)
    molygon = checkParam(args,'molygon_list',False)
    molygon_ccdgon = checkParam(args,'molygon_ccdgon_list',False)
    coadd_object_molygon = checkParam(args,'coadd_object_molygon_list',False)
    section = checkParam(args,'section',False)
    services = checkParam(args,'des_services',False)
    alt_section = checkParam(args, 'alt_section', False)
    det_pfwid = checkParam(args, 'det_pfwid', False)
    alt_table = checkParam(args, 'alt_table', False)

    status = [" completed"," aborted"]
    dbh = desdbi.DesDbi(services, section, retry=True)
    # do some quick checking
    try:
        if alt_section is None:
            alt_section = section
        if alt_table is not None and det_pfwid is None:
            print "Getting det_pfwid from database."
            curs = dbh.cursor()
            tcoadd_file = detcat.split('/')[-1]
            if tcoadd_file.endswith('.fits'):
                temp = "'%s' and compression is null" % tcoadd_file
            else:
                parts = tcoadd_file.split('.fits')
                temp = "'%s.fits' and compression='%s'" % (parts[0], parts[1])
            sql = "select pfw_attempt_id from desfile where filename=%s" % temp

            curs.execute(sql)
            results = curs.fetchall()
            if len(results) != 1:
                raise Exception("Could not determine the pfw_attempt_id from the coadd_file")
            det_pfwid = results[0][0]

    except:
        se = sys.exc_info()
        e = se[1]
        tb = se[2]
        print "Exception raised:", e
        print "Traceback: "
        traceback.print_tb(tb)
        print " "
        exit(1)


    print "\n###################### COADD OBJECT INGESTION ########################\n"
    try:
        printinfo("Working on detection catalog " + detcat)
        detobj = CoaddCatalog(ingesttype='det', filetype=args['coadd_object_filetype'], datafile=detcat, idDict=coaddObjectIdDict, dbh=dbh)

        if alt_table is not None:
            printinfo('Getting Coadd IDs from alternate table')
            detobj.retrieveCoaddObjectIds(args['des_services'], alt_section, det_pfwid, alt_table)
        else:
            isLoaded = detobj.isLoaded()
            if isLoaded:
                printinfo("Getting Coadd IDs from database\n")
                detobj.retrieveCoaddObjectIds()
            else:
                detobj.getIDs()
                stat = detobj.executeIngest()
                retval += detobj.getstatus()
                printinfo("Ingest of detection catalog " + detcat + status[stat] + "\n")
    except:
        se = sys.exc_info()
        e = se[1]
        tb = se[2]
        print "Exception raised:", e
        print "Traceback: "
        traceback.print_tb(tb)
        print " "
        exit(1)

    # do a sanity check, as these numbers are needed for the following steps
    if len(coaddObjectIdDict) == 0:
        print "Coadd Object Dict is empty, cannot continue"
        exit(1)

    if bandcat is not None:
        bandfiles = getfilelist(bandcat)
        for bandfile in bandfiles:
            try:
                bfile = bandfile[0]
                printinfo("Working on band catalog " + bfile)
                bandobj = CoaddCatalog(ingesttype='band', filetype=args['coadd_object_filetype'], datafile=bfile, idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = bandobj.isLoaded()
                if not isLoaded:
                    stat = bandobj.executeIngest()
                    retval += bandobj.getstatus()
                    printinfo("Ingest of band catalog " + bfile + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Coadd Band catalog ingestion, none specified on command line"

    print "\n###################### HEALPIX INGESTION ########################\n"
    if healpix is not None:
        try:
            printinfo("Working on healpix catalog " + healpix)
            healobj = CoaddHealpix(filetype=args['coadd_hpix_filetype'], datafile=healpix, idDict=coaddObjectIdDict, dbh=dbh)
            isLoaded = healobj.isLoaded()
            if not isLoaded:
                stat = healobj.executeIngest()
                retval += healobj.getstatus()
                printinfo("Ingest of healpix catalog " + healpix + status[stat] + "\n")
        except:
            se = sys.exc_info()
            e = se[1]
            tb = se[2]
            print "Exception raised:", e
            print "Traceback: "
            traceback.print_tb(tb)
            print " "
            retval += 1
    else:
        print "Skipping Healpix ingestion, none specified on command line"


    print "\n###################### WEIGHTED AVERAGE INGESTION ########################\n"

    if wavg is not None:
        wavgfiles = getfilelist(wavg)
        for file in wavgfiles:
            try:
                printinfo("Working on wavg catalog " + file[0])
                wavgobj = Wavg(filetype=args['wavg_filetype'], datafile=file[0], idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = wavgobj.isLoaded()
                if not isLoaded:
                    stat = wavgobj.executeIngest()
                    retval += wavgobj.getstatus()
                    printinfo("Ingest of wavg catalog " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Weighted Average ingestion, none specified on command line"

    if wavg_oclink is not None:
        wavgfiles = getfilelist(wavg_oclink)
        for file in wavgfiles:
            try:
                printinfo("Working on wavg_oclink catalog " + file[0])
                wavgobj = Wavg(filetype=args['wavg_oclink_filetype'], datafile=file[0], idDict=coaddObjectIdDict, dbh=dbh, matchCount=False)
                isLoaded = wavgobj.isLoaded()
                if not isLoaded:
                    stat = wavgobj.executeIngest()
                    retval += wavgobj.getstatus()
                    printinfo("Ingest of wavg_oclink catalog " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Weighted Average OCLink ingestion, none specified on command line"

    print "\n###################### MANGLE INGESTION ########################\n"

    if ccdgon is not None:
        ccdfiles = getfilelist(ccdgon)
        for file in ccdfiles:
            try:
                printinfo("Working on ccdgon file " + file[0])
                ccdobj = Mangle(datafile=file[0], filetype=args['ccdgon_filetype'], idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = ccdobj.isLoaded()
                if not isLoaded:
                    stat = ccdobj.executeIngest()
                    retval += ccdobj.getstatus()
                    printinfo("Ingest of ccdgon file " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping CCDgon ingestion, none specified on command line"

    if molygon is not None:
        molyfiles = getfilelist(molygon)
        for file in molyfiles:
            try:
                printinfo("Working on molygon file " + file[0])
                molyobj = Mangle(datafile=file[0], filetype=args['molygon_filetype'], idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = molyobj.isLoaded()
                if not isLoaded:
                    stat = molyobj.executeIngest()
                    retval += molyobj.getstatus()
                    printinfo("Ingest of molygon file " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Molygon ingestion, none specified on command line"

    if molygon_ccdgon is not None:
        mcfiles = getfilelist(molygon_ccdgon)
        for file in mcfiles:
            try:
                printinfo("Working on molygon_ccdgon file " + file[0])
                mcobj = Mangle(datafile=file[0], filetype=args['molygon_ccdgon_filetype'], idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = mcobj.isLoaded()
                if not isLoaded:
                    stat = mcobj.executeIngest()
                    retval += mcobj.getstatus()
                    printinfo("Ingest of molygon_ccdgon file " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Molygon CCDgon ingestion, none specified on command line"

    if coadd_object_molygon is not None:
        cmfiles = getfilelist(coadd_object_molygon)
        for file in cmfiles:
            try:
                printinfo("Working on coadd_object_molygon file " + file[0])
                cmobj = Mangle(datafile=file[0], filetype=args['coadd_object_molygon_filetype'], idDict=coaddObjectIdDict, dbh=dbh, replacecol=3, checkcount=True, skipmissing=alt_table is not None)
                isLoaded = cmobj.isLoaded()
                if not isLoaded:
                    stat = cmobj.executeIngest()
                    retval += cmobj.getstatus()
                    printinfo("Ingest of coadd_object_molygon file " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Coadd Object Molygon ingestion, none specified on command line"

    print "\n###################### EXTINCTION INGESTION ########################\n"

    if extinct is not None:
        try:
            printinfo("Working on extinction catalog " + extinct)
            extobj = Extinction(datafile=extinct, idDict=coaddObjectIdDict, filetype=args['extinct_filetype'], dbh=dbh)
            isLoaded = extobj.isLoaded()
            if not isLoaded:
                stat = extobj.executeIngest()
                retval += extobj.getstatus()
                printinfo("Ingest of detection catalog " + extinct + status[stat] + "\n")
        except:
            se = sys.exc_info()
            e = se[1]
            tb = se[2]
            print "Exception raised:", e
            print "Traceback: "
            traceback.print_tb(tb)
            print " "
            retval += 1
    else:
        print "Skipping Excintion ingestion, none specified on command line"

    if extinct_band is not None:
        exfiles = getfilelist(extinct_band)
        for file in exfiles:
            try:
                printinfo("Working on extinction catalog " + file[0])
                extobj = Extinction(datafile=file[0], idDict=coaddObjectIdDict, filetype=args['extinct_band_filetype'], dbh=dbh)
                isLoaded = extobj.isLoaded()
                if not isLoaded:
                    stat = extobj.executeIngest()
                    retval += extobj.getstatus()
                    printinfo("Ingest of detection catalog " + file[0] + status[stat] + "\n")
            except:
                se = sys.exc_info()
                e = se[1]
                tb = se[2]
                print "Exception raised:", e
                print "Traceback: "
                traceback.print_tb(tb)
                print " "
                retval += 1
    else:
        print "Skipping Extinction Band ingestion, none specified on command line"

    print "EXITING WITH RETVAL",retval
    exit(retval)
