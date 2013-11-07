#!/usr/bin/env python

from coreutils.desdbi import DesDbi
import intgutils.wclutils as wclutils
import sys, os
from collections import OrderedDict


def printNode(indict, level, filehandle):
    leveltabs = ""
    for i in range(level):
        leveltabs = leveltabs + "\t"
    else:        
        for key, value in indict.iteritems():
            if type(value) in (dict,OrderedDict):
                print >>filehandle, leveltabs + "<" + str(key) + ">"
                printNode(value,level+1,filehandle)
                print >>filehandle, leveltabs + "</" + str(key) + ">"
            else:
                print >>filehandle, leveltabs + str(key) + "=" + str(value)


def ingest_datafile_contents(sourcefile,filetype,dataDict):
    dbh = DesDbi()
    print "datafile_ingest.py: Preparing to ingest " + sourcefile
    [tablename, metadata] = dbh.get_datafile_metadata(filetype)
    print "datafile_ingest.py: destination table=" + tablename
    #printNode(metadata,0,sys.stdout)
    columnlist = []
    data = []
    indata = []
    if hasattr(dataDict,"keys"):
        indata.append(dataDict)
    else:
        indata=dataDict

    for attribute,cols in metadata.iteritems():
        for indx, colname in enumerate(cols):
            columnlist.append(colname)
    columnlist.append('filename')

    for inrow in indata:
        row = {}
        for attribute,cols in metadata.iteritems():
            for indx, colname in enumerate(cols):
                if attribute in inrow.keys():
                    if type(inrow[attribute]) is list:
                        if indx < len(inrow[attribute]):
                            row[colname] = inrow[attribute][indx]
                        else:
                            row[colname] = None
                    else:
                        if indx == 0:
                            row[colname] = inrow[attribute]
                        else:
                            row[colname] = None
                else:
                    row[colname] = None
        if(len(row) > 0):
            row["filename"] = sourcefile
            data.append(row)
    if(len(data) > 0):
        dbh.insert_many_indiv(tablename,columnlist,data)
    dbh.commit()
    dbh.close()
    print "datafile_ingest.py: ingest of " + tablename + " complete"
# end ingest_datafile_contents



if __name__ == '__main__':
    # normally this will come from file, but you can get same 
    # results from accessing DB
    """
    data = {}
    data["ASTROMCORR_REFERENCE".lower()] = [1234]
    data["ASTROMCORR_REFERENCE_HIGHSN".lower()] = [5678]
    data["ASTROMOFFSET_REFERENCE".lower()] = [1.234,2.222]
    data["ASTROMOFFSET_REFERENCE_HIGHSN".lower()] = [12.34,22.222]
    #data["ASTROMSIGMA_REFERENCE".lower()] = [5.678,7.77]
    data["ASTROMSIGMA_REFERENCE_HIGHSN".lower()] = 56.78
    data["CHI2_REFERENCE".lower()] = 89.99
    data["CHI2_REFERENCE_HIGHSN".lower()] = [899.9]
    data["NDEG_REFERENCE".lower()] = [50.99]
    data["NDEG_REFERENCE_HIGHSN".lower()] = [500.9]
    data["NDEG_REFERENCE_test".lower()] = [500.9]

    """
    data = {}
    data["nstars_loaded_total"]=343
    data["nstars_loaded_min"]=111
    data["nstars_loaded_mean"]=595.234
    data["nstars_loaded_max"]=679
    data["nstars_accepted_total"]=518
    data["nstars_accepted_min"]=148
    data["nstars_accepted_mean"]=671.32456
    data["nstars_accepted_max"]=197
    data["fwhm_fromfluxradius_min"]=437.511084
    data["fwhm_fromfluxradius_mean"]=489.970428
    data["fwhm_fromfluxradius_max"]=561.31308
    data["sampling_min"]=946.284123
    data["sampling_mean"]=261.896741
    data["sampling_max"]=587.319904
    data["chi2_min"]=430.143999
    data["chi2_mean"]=427.609334
    data["chi2_max"]=814.184781
    data["fwhm_min"]=989.25205
    data["fwhm_mean"]=835.813429
    data["fwhm_max"]=178.069524
    data["ellipticity_min"]=293.146685
    data["ellipticity_mean"]=988.015793
    data["ellipticity_max"]=757.56925
    data["moffatbeta_min"]=834.688677
    data["moffatbeta_mean"]=132.92594
    data["moffatbeta_max"]=566.018806
    data["residuals_min"]=429.49725
    data["residuals_mean"]=760.949938
    data["residuals_max"]=220.596292
    data["fwhm_pixelfree_min"]=272.397425
    data["fwhm_pixelfree_mean"]=557.478307
    data["fwhm_pixelfree_max"]=545.031259
    data["ellipticity_pixelfree_min"]=200.51762
    data["ellipticity_pixelfree_mean"]=475.222164
    data["ellipticity_pixelfree_max"]=35.550211
    data["moffatbeta_pixelfree_min"]=262.418884
    data["moffatbeta_pixelfree_mean"]=415.326315
    data["moffatbeta_pixelfree_max"]=610.010001
    data["residuals_pixelfree_min"]=524.637868
    data["residuals_pixelfree_mean"]=170.773801
    data["residuals_pixelfree_max"]=22.447657
    data["asymmetry_min"]=566.272799
    data["asymmetry_mean"]=147.518432
    data["asymmetry_max"]=285.465005

    ingest_datafile_contents("my_source_filename_7",'psf_xml',data)

