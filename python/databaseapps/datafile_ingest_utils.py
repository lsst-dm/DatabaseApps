#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""  Functions used to ingest non-metadata from a file into a database table based upon filetype """

import sys
import pyfits
import numpy

import despymisc.miscutils as miscutils
from despymisc.xmlslurp import Xmlslurper

DI_COLUMNS = 'columns'
DI_DATATYPE = 'datatype'
DI_FORMAT = 'format'


######################################################################
def ci_get(mydict, mykey):
    """ a case-insensitive dictionary getter """
    for key, val in mydict.iteritems():
        if mykey.lower() == key.lower():
            return val
    return None
# end ci_get

######################################################################
def print_node(indict, level, filehandle):
    """ print a node """
    leveltabs = "\t" * level
    #leveltabs = ""
    #for i in range(level):
    #    leveltabs = leveltabs + "\t"

    for key, value in indict.iteritems():
        if isinstance(value, dict):
            print >>filehandle, leveltabs + "<" + str(key) + ">"
            print_node(value, level+1, filehandle)
            print >>filehandle, leveltabs + "</" + str(key) + ">"
        else:
            print >>filehandle, leveltabs + str(key) + "=" + str(value)
# end print_node

######################################################################
def ingest_datafile_contents(sourcefile, filetype, tablename, metadata, datadict, dbh):
    """ ingest contents of a data file """
    # WARNING: alters dbh session's NLS_TIMESTAMP_FORMAT

#    #[tablename, metadata] = dbh.get_datafile_metadata(filetype)
#
#    if tablename == None or metadata == None:
#        sys.stderr.write("ERROR: no metadata in database for filetype=%s. Check OPS_DATAFILE_TABLE and OPS_DATAFILE_METADATA\n" % filetype)
#        exit(1)
#
#    if is_ingested(sourcefile, tablename, dbh):
#        print "INFO: file " + sourcefile + " is already ingested\n"
#        exit(0)
#
#    print "datafile_ingest.py: destination table = " + tablename
#    #print_node(metadata, 0, sys.stdout)
    columnlist = []
    data = []
    indata = []
    if hasattr(datadict, "keys"):
        indata.append(datadict)
    else:
        indata = datadict

    dateformat = None

    for hdu, attrdict in metadata.iteritems():
        for attribute, cols in attrdict.iteritems():
            for indx, colname in enumerate(cols[DI_COLUMNS]):
                columnlist.append(colname)
                # handle timestamp format; does not support multiple formats in one input file
                if cols[DI_DATATYPE] == 'date':
                    if dateformat and dateformat != cols[DI_FORMAT]:
                        sys.stderr.write("ERROR: Unsupported configuration for filetype=%s: Multiple different date formats found\n" % filetype)
                        exit(1)
                    dateformat = cols[DI_FORMAT]
                ###
        columnlist.append('filename')

    # handle timestamp format; does not support multiple formats in one input file
    if dateformat:
        cur = dbh.cursor()
        cur.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '%s'" % dateformat)

    for hdu, attrdict in datadict.iteritems():
        indata = []
        if hasattr(attrdict, "keys"):
            indata.append(attrdict)
        else:
            indata = attrdict

        rownum = 0  # counter used for rnum column
        for inrow in indata:
            row = {}
            rownum += 1
            for attribute, coldata in metadata[hdu].iteritems():
                for indx, colname in enumerate(coldata[DI_COLUMNS]):
                    attr = None
                    if isinstance(inrow, dict):
                        attr = ci_get(inrow, attribute)
                    else:
                        fitscols = indata.columns.names
                        for k in fitscols:
                            if k.lower() == attribute.lower():
                                attr = inrow.field(k)
                                break
                    if attr is not None or coldata[DI_DATATYPE] == 'rnum':
                        if isinstance(attr, numpy.ndarray):
                            attr = attr.reshape(-1).tolist()
                        if type(attr) is list:
                            if indx < len(attr):
                                row[colname] = attr[indx]
                            else:
                                row[colname] = None
                        else:
                            if indx == 0:
                                if coldata[DI_DATATYPE] == 'int':
                                    row[colname] = int(attr)
                                elif coldata[DI_DATATYPE] == 'float':
                                    row[colname] = float(attr)
                                elif coldata[DI_DATATYPE] == 'rnum':
                                    row[colname] = rownum
                                else:
                                    row[colname] = attr
                            else:
                                row[colname] = None
                    else:
                        row[colname] = None
            if len(row) > 0:
                row["filename"] = sourcefile
                data.append(row)
    if len(data) > 0:
        dbh.insert_many_indiv(tablename, columnlist, data)
    return len(data)
# end ingest_datafile_contents



######################################################################
#def get_sections_for_filetype(filetype, dbh):
#    """ Get definitions from database for what and how to save the data """
#
#    sqlstr = "select distinct hdu from OPS_DATAFILE_METADATA where filetype=%s"
#    sqlstr = sqlstr % dbh.get_named_bind_string('ftype')
#
#    result = []
#    curs = dbh.cursor()
#    curs.execute(sqlstr, {"ftype": filetype})
#    for row in curs:
#        result.append(row[0])
#    curs.close()
#    return result
# end get_sections_for_filetype


######################################################################
#def get_di_config(filetype, dbh):
#    """ Query DB to get configuration for what and how to save the data """
#
#    [didefs['tablename'], didefs['metadata] = dbh.get_datafile_metadata(filetype)


######################################################################
def is_ingested(filename, tablename, dbh):
    """ Check whether the data for a file is already ingested """

    sqlstr = "select 1 from dual where exists(select * from %s where filename=%s)"
    sqlstr = sqlstr % (tablename, dbh.get_named_bind_string('fname'))

    found = False
    curs = dbh.cursor()
    curs.execute(sqlstr, {"fname":filename})
    for row in curs:
        found = True
    curs.close()
    return found
# end is_ingested


######################################################################
def get_fits_data(fullname, whichhdu):
    """ Get data from fits file header"""

    hdu = None
    try:
        hdu = int(whichhdu)
    except ValueError:
        hdu = whichhdu

    hdulist = pyfits.open(fullname)
    hdr = hdulist[hdu].header

    mydict = {}
    if 'XTENSION' in hdr and hdr['XTENSION'] == 'BINTABLE':
        mydict[whichhdu] = hdulist[hdu].data
    else:
        mydict[whichhdu] = dict(hdr)

    hdulist.close()

    return mydict


######################################################################
def datafile_ingest_main(dbh, filetype, fullname, tablename, didatadefs):
    """ Control process for ingesting data from a file """

    #sections_wanted = get_sections_for_filetype(filetype, dbh)
    sections_wanted = didatadefs.keys()

    if 'xml' in filetype:
        datadict = Xmlslurper(fullname, sections_wanted).gettables()
    else:
        if len(sections_wanted) > 1:
            raise ValueError("Multiple hdus not yet supported\n")
        datadict = get_fits_data(fullname, sections_wanted[0])

    filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

    numrows = ingest_datafile_contents(filename, filetype,
                                       tablename, didatadefs,
                                       datadict, dbh)

    return numrows
