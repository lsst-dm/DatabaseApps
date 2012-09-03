# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Provide database access methods related to ingest of astromatic quality
control data.

Developed at: 
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2012 Board of Trustees of the University of Illinois. 
All rights reserved.
"""

__version__ = "$Rev$"

import coreutils

import errors

class _ColumnList (list):
    "A list that prevents attempts to add duplicate items."
    def __init__ (self):
        list.__init__ (self)
    def add (self, column, filetype):
        "Add a column to the list; failing if duplicate."
        if column in self:
            raise errors.DuplicateColumnError (filetype, column)
        else:
            list.append (self, column)

class AstromaticQCDB (coreutils.DesDbi):
    """
    Provide database access methods.

    Instantiating this class opens a connection to a database identified in a
    DES service access file.  The instance may then be used to operate on the
    database and extends coreutils.DesDbi with methods useful for ingesting
    astromatic quality control data.

    Note that database table and column names retrieved from the metadata
    mapping tables are converted to lowercase and are not quoted when used in
    statements.  This disables the use of mixed case in table and column names
    in the database, but simplifies use of the metadata mapping tables.
    """

    def __init__ (self, *args, **kwargs):
        """
        Establish a connection to a database.
        
        Any arguments are passed on to coreutils.DesDbi.
        """

        coreutils.DesDbi.__init__ (self, *args, **kwargs)

        self._map_cache = {}

    def get_votable_map (self, qc_type, refresh = False):
        """
        Return a map from the specified qc file type to the database.

        A cache of maps is maintained.  Any cached map for qc_type is discarded
        and replaced if refresh is True.

        The returned map is a dictionary with the following keys:
            VOTable     ID of the VOTable table containing the data to ingest
            table       name of the target database table
            id          name of the target database table id column
            fileId      name of the target database table file id column
            fieldMap    a dictionary of column name lists indexed by VOTable
                        field name.
            columns     a list of all of the column names, providing a
                        consistent ordering.

        Note that VOTable fields that are expected to contain arrays are to be
        mapped to multiple database columns in the order listed for the field
        in fieldMap. 

        This method performs a number of integrity checks on the mapping data
        since the database seems to have very few constraints to enforce the
        requirements.  As a result, any of a number of exceptions may be
        raised.
        """

        if not hasattr (self, '_map_cache'):
            self._map_cache = {}

        if refresh:
            self._map_cache.pop (qc_type, None)

        if qc_type not in self._map_cache:
            stmt = (
                "SELECT r.file_segment, f.metadata_table, LOWER (t.id_column), "
                "       LOWER (t.parent_id_column), file_header_name, "
                "       m.position, LOWER (m.column_name) "
                "       FROM filetype f "
                "            LEFT JOIN metadata_table t "
                "                      ON t.table_name = f.metadata_table "
                "            LEFT JOIN required_metadata r USING (filetype) "
                "            LEFT JOIN metadata m USING (file_header_name) "
                "WHERE  filetype = " + self.get_positional_bind_string() +
                "ORDER BY file_header_name, m.position"
                )

            cursor = self.cursor ()
            cursor.execute (stmt, (qc_type, ))
            rows = cursor.fetchall ()
            cursor.close ()

            if len (rows) > 0:

                field_map = {}
                votable_id = rows [0][0]
                id_col     = rows [0][2]
                fid_col    = rows [0][3]
                db_table   = rows [0][1]

                # The database has few constraints, so check for lots of
                # possible problems.

                if db_table is None:
                    raise errors.MissingDBTableError (qc_type)

                if votable_id is None:
                    raise errors.MissingFileSegmentError (qc_type)

                if id_col is None:
                    raise errors.MissingIdColumnError (qc_type)

                if fid_col is None:
                    raise errors.MissingFileIdColumnError (qc_type)

                unique_cols = _ColumnList ()
                unique_cols.add (id_col, qc_type)
                unique_cols.add (fid_col, qc_type)

                for row in rows:
                    # These error conditions lead to extra rows being included
                    # in the results which lead to the appearance of multiple
                    # occurrences of a column name, so perform these checks
                    # before checking the column name for uniqueness.
                    if row [0] != votable_id:
                        raise errors.MultipleTableError (qc_type,
                                                         (votable_id, row [0]))

                    if row [2] != id_col:
                        raise errors.MultipleIdColumnsError (qc_type,
                                                             (id_col, row [2]))

                    unique_cols.add (row [6], qc_type)

                    hdr = row [4]
                    if hdr is None:
                        raise errors.MissingHeaderNameError (qc_type)

                    if row [6] is None:
                        raise errors.MissingColumnNameError (qc_type, hdr)

                    if hdr in field_map:
                        field_map [hdr].append (row [6])
                    else:
                        field_map [hdr] = [row [6]]

                self._map_cache [qc_type] = {'VOTable' : votable_id,
                                             'table'   : db_table,
                                             'id'      : id_col,
                                             'fileId'  : fid_col,
                                             'fieldMap': field_map,
                                             'columns' : list (unique_cols)}
            else:
                self._map_cache [qc_type] = None

        return self._map_cache [qc_type]

    def get_existing_rows (self, votable_map, file_id):
        """
        Return any previously-ingested rows.
        
        Arguments:
            votable_map     The map identifying the ingest table
            file_id         The id of the file of interest

        Return a list of rows for the specified file_id in order by the table's
        id column value.  Each row is in a dictionary of values indexed by
        column name.
        """


        table       = votable_map ['table']
        id_col      = votable_map ['id']
        file_id_col = votable_map ['fileId']
        columns     = votable_map ['columns']

        where = '%s = %s' % (file_id_col, self.get_positional_bind_string ())

        rows = self.query_simple (table, columns, where=where, orderby=id_col,
                                  params=(file_id, ))

        return rows

    def _from_numpy (self, item):
        "Convert an item from a numpy type."

        # numpy's tolist() method converts to python type (even for # a scalar
        # value).  If there's no tolist() method, assume the data is already a
        # standard python type.

        try:
            return item.tolist ()
        except AttributeError:
            return item

    def build_rows (self, votable_map, file_id, data, bool_map = None):
        """
        Return a list of ingestable rows

        Arguments:
            votable_map     The map identifying the field mapping
            file_id         The identifier of the file providing the data
            data            A sequence of VOTable data rows.  Each entry is a
                            mapping from VOTable field names to values.
            bool_map        A map from Boolean values to database values.
                            (Oracle does not support Boolean columns so there
                            is no standard way to implement them.)

        Rows are constructed by mapping each VOTable row in data to an
        equivalent row for the database table indicated in votable_map.
        VOTable fields not present in votable_map will be silently ignored.
        Missing fields will result in an error.  Fields mapped to multiple
        columns must provide a value for each such column.  Field values found
        to have a numpy type will be converted to a corresponding python type.
        """

        row_id = file_id
        rows   = []
        fld_map = votable_map ['fieldMap']
        id_col  = votable_map ['id']
        fid_col = votable_map ['fileId']

        for row_data in data:
            row_id += 1
            row = {id_col: row_id, fid_col: file_id}
            for fld, cols in fld_map.items ():
                # If the field maps to a single column, assign single field
                # value.  Otherwise, copy each value to associated column.

                try:
                    if hasattr (row_data [fld], '__iter__'):
                        fdata = row_data [fld]
                    else:
                        fdata = [row_data [fld]]
                except KeyError:
                    # numpy array doesn't seem to support "in" operator.
                    raise errors.MissingFieldError (fld)

                if len (cols) == len (fdata):
                    for col, val in zip (cols, fdata):
                        row [col] = self._from_numpy (val)
                        if type (row [col]) == bool:
                            try:
                                row [col] = bool_map [row [col]]
                            except TypeError:
                                raise errors.MissingBooleanMapError (fld)
                else:
                    raise errors.ValueCountError (fld, len (cols), len (fdata))

            rows.append (row)

        return rows

    def ingest_rows (self, votable_map, rows):
        """
        Ingest the provided mapped rows into the appropriate database table.

        Arguments:
            votable_map     The mapping for the VOTable of interest.
            rows            A sequence of dictionaries containing the data to
                            be ingested indexed by column name.
        """

        columns = votable_map ['columns']

        self.insert_many (votable_map ['table'], columns, rows)
