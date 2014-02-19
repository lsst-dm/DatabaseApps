#!/usr/bin/env python

# $Id: catalog_ingest.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys


def checkParam(args,param,required):
    if args[param]:
        return args[param]
    else:
        if required:
            sys.stderr.write("Missing required parameter: %s" % param)
        else:
            return None
# end checkParam

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
    datafile = checkParam(args,'outputfile',False)
    mode = checkParam(args,'mode',False)

    print("Preparing to load " + filename + " of type " + filetype + " into " + temptable)

    try:
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
        
        (isloaded,code) = catingest.isLoaded()
        if isloaded:
            exit(code)                    

        catingest.execute()

        print("catalogIngest load of " + str(numCatObjects) + " objects from " + filename + " completed")

