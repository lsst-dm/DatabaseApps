# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Provide astromatic quality control ingest routines

The primary interface is ingest() which controls the ingest process.

Developed at: 
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2012 Board of Trustees of the University of Illinois. 
All rights reserved.
"""

__version__ = "$Rev$"

__all__ = ['AstromaticQCDB', 'QCVOTable', 'compare_rounded', 'ingest',
           'MissingBooleanMapError', 'MissingFieldError', 'ReingestError',
           'ReingestWarning', 'UnknownQCType', 'ValueCountError',
           'DBMetadataError', 'DuplicateColumnError', 'MissingDBTableError',
           'MissingColumnNameError', 'MissingFileSegmentError',
           'MissingHeaderNameError', 'MissingFileIdColumnError',
           'MissingIdColumnError', 'MultipleIdColumnsError',
           'MultipleTableError']

from .ingest          import ingest
from .qcdb            import AstromaticQCDB
from .qcvotable       import QCVOTable
from .errors          import *
from .compare_rounded import compare_rounded
