# $Id: __init__.py 10292 2013-01-07 17:54:32Z mgower $
# $Rev:: 10292                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-01-07 11:54:32 #$:  # Date of last commit.

"""
    Provide DES database access methods

    Classes:
        ObjectCatalog - Loads fits data files into the database, or prepares
            for such a load by creating an ascii datafile and sqlldr
            control file. Uses database data to determine what data from the
            file should be loaded, and the columns that they should be loaded
            into.
        Xmlslurper - Static wrapper for parsing an XML file and presenting
            it as a set of dictionaries.
        IngestUtils - Container for ingest utility functions

    Error Classes:

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

__version__ = "$Rev: 10292 $"

# Note that pydoc includes documentation for entries in the __all__  list when
# generating documentation for this package.

__all__ = ['ObjectCatalog', 'Xmlslurper','IngestUtils']

# Make the main class and all the error classes available directly within
# the package to simplify imports for package users.

from .objectcatalog import ObjectCatalog
from .xmlslurp import Xmlslurper
from .ingestutils import IngestUtils
