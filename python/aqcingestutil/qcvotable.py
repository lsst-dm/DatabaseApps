# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Provide a simple vo.table wrapper for interacting with VOTable XML documents

Developed at: 
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2012 Board of Trustees of the University of Illinois. 
All rights reserved.
"""

__version__ = "$Rev$"

import vo.table

class QCVOTable (object):
    """A simple wrapper for vo.table."""

    def __init__ (self, votable_file):
        """
        Load a VOTable from votable_file

        votable_file can be a file-like object or the name of a file containing
        a VOTable XML document.
        """

        self.votable = vo.table.parse (votable_file, pedantic=False)

    def get_qc_type (self):
        """
        Return the qc type (aka filetype) for the VOTable.

        The type is the ID attribute value of the top-level RESOURCE tag.
        """
        return self.votable.resources [0].ID

    def get_table (self, tid):
        """Return a VOTable table given the ID of the TABLE tag."""
        return self.votable.get_table_by_id (tid)

    def get_table_data (self, tid, max_rows=1):
        """
        Return data from the specified VOTable table as a numpy array.

        Return a numpy array of rows containing only the specified number of
        rows from the VOTable with an ID attribute equal to the specified tid.
        Use max_rows = 0 to retrieve all rows regardless of length.

        Note that the default for max_rows is set to 1 because some of the
        currently ingested VOTable tables contain more than one row, but the
        additional rows are ignored unconditionally.
        """

        tab = self.votable.get_table_by_id (tid)
        if tab:
            return tab.array [0:max_rows] if max_rows > 0 else tab.array
        else:
            return None
