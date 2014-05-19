#!/usr/bin/env python

# $Id: catalog_ingest.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import time
import argparse
from databaseapps.catalogingest import CatalogIngest as CatalogIngest


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
        print time.strftime(CatalogIngest.debugDateFormat) + " - " + msg

if __name__ == '__main__':
    
    hduList = None
    
    parser = argparse.ArgumentParser(description='Ingest objects from a fits catalog')
    parser.add_argument('-request',action='store')
    parser.add_argument('-filename',action='store')
    parser.add_argument('-filetype',action='store')
    parser.add_argument('-temptable',action='store')
    parser.add_argument('-targettable',action='store')
    parser.add_argument('-fitsheader',action='store')
    parser.add_argument('-mode',action='store')
    parser.add_argument('-outputfile',action='store')
    
    args, unknown_args = parser.parse_known_args()
    args = vars(args)
    
    request = checkParam(args,'request',True)
    filename = checkParam(args,'filename',True)
    filetype = checkParam(args,'filetype',True)
    temptable = checkParam(args,'temptable',False)
    targettable = checkParam(args,'targettable',True)
    fitsheader = checkParam(args,'fitsheader',False)
    outputfile = checkParam(args,'outputfile',False)
    mode = checkParam(args,'mode',False)
    
    
    catingest = CatalogIngest(
                        request=request,
                        filetype=filetype,
                        datafile=filename,
                        temptable=temptable,
                        targettable=targettable,
                        fitsheader=fitsheader,
                        mode=mode,
                        outputfile=outputfile
                    )
    if mode == None or mode == 'sqlldr':
        (isloaded,code) = catingest.isLoaded()
        if isloaded:
            exit(code)
        
        printinfo("Preparing to load " + filename + " of type " + filetype +
            " into " + catingest.tempschema + '.' + catingest.temptable)
    else:
        printinfo("Preparing to dump " + filename + " of type " + filetype +
            " into file " + catingest.getOutputFilename())
    catingest.createControlFile()
    catingest.createIngestTable()
    catingest.execute()
        
    printinfo("catalogIngest load of " + str(catingest.getNumObjects()) + " objects from " + filename + " completed")
