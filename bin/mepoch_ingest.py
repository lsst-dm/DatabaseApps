#!/usr/bin/env python

import sys
import time
from despydb import desdbi
import argparse
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

    parser = argparse.ArgumentParser(description='Ingest coadd objects from fits catalogs')
    parser.add_argument('--bandcat_list', action='store')
    parser.add_argument('--detcat', action='store')
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


    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    bandcat = checkParam(args,'bandcat_list',True)
    detcat = checkParam(args,'detcat',True)
    extinct = checkParam(args,'extinct',True)
    extinct_band = checkParam(args, 'extinct_band_list', True)
    healpix = checkParam(args,'healpix',True)
    wavg = checkParam(args,'wavg_list',True)
    wavg_oclink = checkParam(args,'wavg_oclink_list',True)
    ccdgon = checkParam(args,'ccdgon_list',True)
    molygon = checkParam(args,'molygon_list',True)
    molygon_ccdgon = checkParam(args,'molygon_ccdgon_list',True)
    coadd_object_molygon = checkParam(args,'coadd_object_molygon_list',True)
    section = checkParam(args,'section',False)
    services = checkParam(args,'des_services',False)

    dbh = desdbi.DesDbi(services, section)
    if detcat is not None:
        detobj = CoaddCatalog(ingesttype='det', datafile=detcat, idDict=coaddObjectIdDict, dbh=dbh)
        (isLoaded, code) = detobj.isLoaded()
        if isLoaded:
            if code != 0:
                exit(code)
            else:
                print "Det catalog already loaded, continuing"
        else:
            printinfo("Preparing to load detection catalog " + detcat)

            detobj.executeIngest()
        
            printinfo("Ingest of detection catalog " + detcat + " completed\n")

    if bandcat is not None:
        bandfiles = getfilelist(bandcat)
        for bandfile in bandfiles:
            bfile = bandfile[0]
            bandobj = CoaddCatalog(ingesttype='band', datafile=bfile, idDict=coaddObjectIdDict, dbh=dbh)
            (isLoaded, code) = bandobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    print "Band catalog %s already loaded, continuing" % (bfile)
            else:
                printinfo("Preparing to load band catalog " + bfile)
    
                bandobj.executeIngest()
            
                printinfo("Ingest of band catalog " + bfile + " completed\n")

    # do a sanity check, as these numbers are needed for the following steps
    if len(coaddObjectIdDict) == 0:
        raise Exception("Coadd Object Dict is Empty")

    if healpix is not None:
        healobj = CoaddHealpix(datafile=healpix, idDict=coaddObjectIdDict, dbh=dbh)
        (isLoaded, code) = healobj.isLoaded()
        if isLoaded:
            if code != 0:
                exit(code)
            else:
                print "Healpix catalog already loaded, continuing"
        else:
            printinfo("Preparing to load healpix catalog " + healpix)

            healobj.executeIngest()
        
            printinfo("Ingest of healpix catalog " + healpix + " completed\n")

    if wavg is not None:
        wavgfiles = getfilelist(wavg)
        for file, band in wavgfiles:
            wavgobj = Wavg(filetype='wavg', datafile=file, idDict=coaddObjectIdDict, band=band, dbh=dbh)
            (isLoaded, code) = wavgobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    print "Wavg catalog %s already loaded, continuing" % (file)
            else:
                printinfo("Preparing to load wavg catalog " + file)

                wavgobj.executeIngest()
        
                printinfo("Ingest of wavg catalog " + file + " completed\n")

    if wavg_oclink is not None:
        wavgfiles = getfilelist(wavg_oclink)
        for file, band in wavgfiles:
            wavgobj = Wavg(filetype='wavg_oclink', datafile=file, idDict=coaddObjectIdDict, band=band, dbh=dbh)
            (isLoaded, code) = wavgobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    print "Wavg_oclink catalog %s already loaded, continuing" % (file)
            else:
                printinfo("Preparing to load wavg_oclink catalog " + file)

                wavgobj.executeIngest()
        
                printinfo("Ingest of wavg_oclink catalog " + file + " completed\n")
    
    if ccdgon is not None:
        ccdfiles = getfilelist(ccdgon)
        for file in ccdfiles:
            ccdobj = Mangle(datafile=file[0], filetype='ccdgon', idDict=coaddObjectIdDict, dbh=dbh)
            (isLoaded, code) = ccdobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    printinfo("ccdgon catalogs already loaded, continuing")
            else:
                printinfo("Preparing to load ccdgon files")

                ccdobj.executeIngest()

                printinfo("Ingest of ccdgon file " + file[0] + " completed\n")

    if molygon is not None:
        molyfiles = getfilelist(molygon)
        for file in molyfiles:
            molyobj = Mangle(datafile=file[0], filetype='molygon', idDict=coaddObjectIdDict, dbh=dbh)
            (isLoaded, code) = molyobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    printinfo("molygon catalogs already loaded, continuing")
            else:
                printinfo("Preparing to load molygon files")

                molyobj.executeIngest()

                printinfo("Ingest of molygon file " + file[0] + " completed\n")

    if molygon_ccdgon is not None:
        mcfiles = getfilelist(molygon_ccdgon)
        for file in mcfiles:
            mcobj = Mangle(datafile=file[0], filetype='molygon_ccdgon', idDict=coaddObjectIdDict, dbh=dbh)
            (isLoaded, code) = mcobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    printinfo("molygon_ccdgon catalogs already loaded, continuing")
            else:
                printinfo("Preparing to load molygon_ccdgon files")

                mcobj.executeIngest()

                printinfo("Ingest of molygon_ccdgon file " + file[0] + " completed\n")

    if coadd_object_molygon is not None:
        cmfiles = getfilelist(coadd_object_molygon)
        for file in cmfiles:
            cmobj = Mangle(datafile=file[0], filetype='coadd_object_molygon', idDict=coaddObjectIdDict, dbh=dbh)
            (isLoaded, code) = cmobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    printinfo("coadd_object_molygon catalogs already loaded, continuing")
            else:
                printinfo("Preparing to load coadd_object_molygon files")

                cmobj.executeIngest()

                printinfo("Ingest of coadd_object_molygon file " + file[0] + " completed\n")

    if extinct is not None:
        extobj = Extinction(datafile=extinct, idDict=coaddObjectIdDict, filetype='extinct_ebv', dbh=dbh)
        (isLoaded, code) = extobj.isLoaded()
        if isLoaded:
            if code != 0:
                exit(code)
            else:
                print "Extinction catalog already loaded, continuing"
        else:
            printinfo("Preparing to load extinction catalog " + extinct)

            extobj.executeIngest()

            printinfo("Ingest of detection catalog " + extinct + " completed\n")

    if extinct_band is not None:
        exfiles = getfilelist(extinct_band)
        for file in exfiles:
            extobj = Extinction(datafile=file[0], idDict=coaddObjectIdDict, filetype='extinct_band', dbh=dbh)
            (isLoaded, code) = extobj.isLoaded()
            if isLoaded:
                if code != 0:
                    exit(code)
                else:
                    print "Extinction catalog already loaded, continuing"
            else:
                printinfo("Preparing to load extinction catalog " + file[0])
                
                extobj.executeIngest()

                printinfo("Ingest of detection catalog " + file[0] + " completed\n")

