# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define exception classes.
"""

__version__ = "$Rev$"

from pprint import pformat

#===============================================================================
# Define miscellaneous errors
#===============================================================================

class MissingBooleanMapError (Exception):
    """Cannot map a Boolean field without a valid mapping."""
    def __init__ (self, field):
        msg = 'Cannot map Boolean field ("%s") without valid mapping.' % field
        Exception.__init__ (self, msg)
        self.field = field

class MissingFieldError (Exception):
    """Missing field detected."""
    def __init__ (self, field):
        Exception.__init__ (self, 'Missing field: "%s".' % field)
        self.field = field

class ReingestError (Exception):
    """Attempt to re-ingest different data."""
    def __init__ (self, file_id, diffs):
        msg = ('File id %d was previously ingested with '
                                                  'different data.\n') % file_id
        msg += ('Differences between previous (A) and new (B) data:\n' + 
                pformat (diffs))
        Exception.__init__ (self, msg)
        self.file_id = file_id
        self.diffs = diffs

class ReingestWarning (UserWarning):
    """Attempt to re-ingest same data."""
    def __init__ (self, file_id):
        msg = 'This data was previously ingested for file id %d.' % file_id
        UserWarning.__init__ (self, msg)
        self.file_id = file_id

class UnknownQCType (Exception):
    """An unknown type of VOTable file was specified."""
    def __init__ (self, qc_type):
        msg = 'QC VOTable file type "%s" not found in database.' % qc_type
        Exception.__init__ (self, msg)
        self.qc_type = qc_type

class ValueCountError (Exception):
    """Insffient number of values for a multi-valued field."""
    def __init__ (self, field, expected, received):
        msg = ('For field "%s", expected %s value(s), received %s.'
               % (field, expected, received))
        Exception.__init__ (self, msg)
        self.field    = field
        self.expected = expected
        self.received = received

#===============================================================================
# Define errors related to the database metadata tables
#===============================================================================

class DBMetadataError (Exception):
    """
    Base class for problems in the database metadata table definitions.

    The database provides few constraints, so there are a lot of potential
    problems in the definition of the mapping from a VOTable file to a database
    table and corresponding columns, resulting in a number of exceptions to
    report detected problems.  All such exceptions and no others should be a
    sub-class of this one.

    The filetype attribute records the filetype for which the problem occurred.
    """

    def __init__ (self, filetype, msg):
        msg = 'Bad metadata mapping for filetype "%s": %s' % (filetype, msg)
        Exception.__init__ (self, msg)
        self.filetype = filetype

class DuplicateColumnError (DBMetadataError):
    """Two or more different fields map to the same column."""

    def __init__ (self, filetype, column):
        msg = 'Column "%s" used for multiple fields.' % column
        DBMetadataError.__init__ (self, filetype, msg)
        self.column = column

class MissingDBTableError (DBMetadataError):
    """Null database table name for a VOTable."""

    def __init__ (self, filetype):
        DBMetadataError.__init__ (self, filetype,
                                  'Missing database table name.')

class MissingColumnNameError (DBMetadataError):
    """Null column name or missing metadata row."""

    def __init__ (self, filetype, header_name):
        msg = 'Missing column name for file header "%s".' % header_name
        DBMetadataError.__init__ (self, filetype, msg)
        self.header_name = header_name

class MissingFileSegmentError (DBMetadataError):
    """Null file segment or missing required metadata rows."""

    def __init__ (self, filetype):
        DBMetadataError.__init__ (self, filetype,
                                  'Missing file segment name.')

class MissingHeaderNameError (DBMetadataError):
    """Null file header name in required_metadata table."""

    def __init__ (self, filetype):
        DBMetadataError.__init__ (self, filetype,
                                  'Missing file header name.')

class MissingFileIdColumnError (DBMetadataError):
    """Null file id column or missing required metadata_table row."""

    def __init__ (self, filetype):
        DBMetadataError.__init__ (self, filetype,
                                  'Missing file id column name.')

class MissingIdColumnError (DBMetadataError):
    """Null id column or missing required metadata_table row."""

    def __init__ (self, filetype):
        DBMetadataError.__init__ (self, filetype, 'Missing id column name.')

class MultipleIdColumnsError (DBMetadataError):
    """Two or more database id columns specified."""

    def __init__ (self, filetype, columns):
        msg = ('Multiple id columns specified: %s.' % (columns, ))
        DBMetadataError.__init__ (self, filetype, msg)
        self.columns = columns

class MultipleTableError (DBMetadataError):
    """Two or more VOTable TABLE entities referenced."""

    def __init__ (self, filetype, tables):
        msg = ('Ingestion from multiple VOTable TABLE entities %s not '
               'supported.') % (tables, )
        DBMetadataError.__init__ (self, filetype, msg)
        self.tables = tables
