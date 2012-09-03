# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
control the astromatic quality control data ingest process

ingest() implements the ingest process.

Developed at: 
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2012 Board of Trustees of the University of Illinois. 
All rights reserved.
"""

__version__ = "$Rev$"

import warnings

from .compare_rounded import compare_rounded

from . import qcvotable
from . import qcdb
from . import errors

def ingest (votable_file, file_id, section=None, bool_map=None):
    """
    Ingest astromatic quality control data from a VOTable document.

    Arguments
        votable_file    a file-like object or a name of a file containing a
                        VOTable document containing astromatic quality
                        control data
        file_id         integer id of the file being ingested (this will be
                        the parent of the ingested data)
        section         name of a section in a DES service access file from
                        which database connection information will be
                        retrieved; methods described in DES-3 will be used
                        to find the section name if not provided
        bool_map        a dict or other mapping structure to use to convert
                        Boolean field values to database column values; not
                        required if no Boolean field values are ingested

    The ID attribute of the top-level RESOURCE tag in the VOTable specifies
    the filetype used to search the database metadata mapping tables for a
    mapping from VOTable table to database table and columns.

    The retrieved mapping identifies the ID of the VOTable table from which
    data is ingested.  Only the first row of data is ingested.
    """

    votable = qcvotable.QCVOTable (votable_file)

    qc_type = votable.get_qc_type ()

    with qcdb.AstromaticQCDB (section=section) as dbi:
        # Get the VOTable to database mapping from the database.  Get the
        # data from the appropriate table in the VOTable file.  Translate the
        # VOTable data to database rows.

        vo_map = dbi.get_votable_map (qc_type)

        if vo_map is None:
            raise errors.UnknownQCType (qc_type)

        data = votable.get_table_data (vo_map ['VOTable'])

        new_rows = dbi.build_rows (vo_map, file_id, data, bool_map)

        # Check whether this VOTable file has been ingested before.  If so,
        # compare the new and old data.  If they match (with rounding), just
        # report the situation and exit; otherwise, report the differences and
        # abort.  If not previously ingest, ingest the data.

        cur_rows = dbi.get_existing_rows (vo_map, file_id)

        if cur_rows:
            same, diffs = compare_rounded (cur_rows, new_rows)
            if same:
                warnings.warn (errors.ReingestWarning (file_id))
            else:
                raise errors.ReingestError (file_id, diffs)
        else:
            dbi.ingest_rows (vo_map, new_rows)
