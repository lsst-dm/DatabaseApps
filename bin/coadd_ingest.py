#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev$"

import os
import sys
import time
import argparse
from databaseapps.coaddcatalog import CoaddCatalog as CoaddCatalog
from databaseapps.coaddhealpix import CoaddHealpix as CoaddHealpix


def checkParam(args,param,required):
    if args[param]:
        return args[param]
    else:
        if required:
            sys.stderr.write("Missing required parameter: %s\n" % param)
        else:
            return None
# end checkParam

def printinfo(msg):
        print time.strftime(CoaddCatalog.debugDateFormat) + " - " + msg

if __name__ == '__main__':
    
    hduList = None
    coaddObjectIdDict = {}
    
    parser = argparse.ArgumentParser(description='Ingest coadd objects from fits catalogs')
    parser.add_argument('--bandcat',action='store')
    parser.add_argument('--detcat',action='store')
    parser.add_argument('--extinct',action='store')
    parser.add_argument('--healpix',action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)
    
    bandcat = checkParam(args,'bandcat',True)
    detcat = checkParam(args,'detcat',True)
    extinct = checkParam(args,'extinct',True)
    healpix = checkParam(args,'healpix',True)
    
    if bandcat==None or detcat==None or extinct==None or healpix==None:
        exit(1)
    
    # ingest detection catalog
    detobj = CoaddCatalog(
        ingesttype='det',
        datafile=detcat,
        idDict=coaddObjectIdDict
    )

    (isloaded,code) = detobj.isLoaded()
    if isloaded:
        exit(code)
        
    printinfo("Preparing to load detection catalog " + detcat)

    detobj.createControlFile()
    detobj.executeIngest()
        
    printinfo("Ingest of detection catalog " + detcat + " completed")

    # ingest each of the band catalogs
    bandfiles = bandcat.split(",")

    for bandfile in bandfiles:
        bandobj = CoaddCatalog(
            ingesttype='band',
            datafile=bandfile,
            idDict=coaddObjectIdDict
        )
    
        (isloaded,code) = bandobj.isLoaded()
        if isloaded:
            exit(code)
            
        printinfo("Preparing to load band catalog " + bandfile)
    
        bandobj.createControlFile()
        bandobj.executeIngest()
            
        printinfo("Ingest of band catalog " + bandfile + " completed")

    # ingest healpix
    healpixobj = CoaddHealpix(
        datafile=healpix,
        idDict=coaddObjectIdDict
    )

    (isloaded,code) = healpixobj.isLoaded()
    if isloaded:
        exit(code)
        
    printinfo("Preparing to load healpix file " + healpix)

    healpixobj.createControlFile()
    healpixobj.executeIngest()

    printinfo("Ingest of healpix file " + healpix + " completed")

# **************************************
# cpond: deal with extinct
# **************************************
