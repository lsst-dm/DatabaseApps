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
from databaseapps.objectcatalog import ObjectCatalog as ObjectCatalog
from databaseapps.objectcatalog import Timing as Timing

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
        print time.strftime(ObjectCatalog.debugDateFormat) + " - " + msg

if __name__ == '__main__':
    
    hduList = None
    runtime = Timing('Full ingestion')
    parser = argparse.ArgumentParser(description='Ingest objects from a fits catalog')
    parser.add_argument('-request',action='store')
    parser.add_argument('-filename',action='store')
    parser.add_argument('-filetype',action='store')
    parser.add_argument('-temptable',action='store')
    parser.add_argument('-targettable',action='store')
    parser.add_argument('-fitsheader',action='store')
    parser.add_argument('-dump',action='store')
    parser.add_argument('-section', '-s', help='db section in the desservices file')
    parser.add_argument('-des_services', help='desservices file')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)
    
    request = checkParam(args,'request',True)
    filename = checkParam(args,'filename',True)
    filetype = checkParam(args,'filetype',True)
    temptable = checkParam(args,'temptable',False)
    targettable = checkParam(args,'targettable',True)
    fitsheader = checkParam(args,'fitsheader',False)
    dump = checkParam(args,'dump',False)
    services = checkParam(args,'des_services',False)
    section = checkParam(args,'section',False)

    if request==None or filename==None or filetype==None or targettable==None:
        exit(1)
    printinfo(runtime.report("INIT"))    
    objectcat = ObjectCatalog(
                        request=request,
                        filetype=filetype,
                        datafile=filename,
                        temptable=temptable,
                        targettable=targettable,
                        fitsheader=fitsheader,
                        dumponly=dump,
                        services=services,
                        section=section
                    )
    printinfo(runtime.report("READ %s" % str(objectcat.getNumObjects())))

    objectcat.createIngestTable()
    printinfo(runtime.report("CREATE"))
    objectcat.executeIngest()
    printinfo(runtime.report("LOAD %s" % str(objectcat.getNumObjects())))
    printinfo("catalogIngest load of " + str(objectcat.getNumObjects()) + " objects from " + filename + " completed")
    printinfo(runtime.end())
