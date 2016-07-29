import time
from collections import OrderedDict
from ingestutils import IngestUtils as ingestutils
from despymisc import miscutils
import traceback
import sys

class Ingest(object):
    """ General base class for ingesting data into the database

    """
    debug = True
    debugDateFormat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, filetype, datafile, hdu=None, order=None, dbh=None):
        self.objhdu = hdu
        if dbh is None:
            dbh = desdbi.DesDbi()
        else:
            self.dbh = dbh
        self.cursor = self.dbh.cursor()
        # get the table name that is being filled, based on the input data type
        self.cursor.execute("select table_name from ops_datafile_table where filetype='%s'" % (filetype))
        self.targettable = self.cursor.fetchall()[0][0]
        self.filetype = filetype
        self.idColumn = None
        self.order = order
        self.fileColumn = None
        self.constants = {}
        self.orderedColumns = []
        self.sqldata = []
        self.fullfilename = datafile
        self.shortfilename = ingestutils.getShortFilename(datafile)
        self.status = 0

        # dictionary of table columns in db
        self.dbDict = self.getObjectColumns()

    def getstatus(self):
        return self.status

    def debug(self, msg):
        if self.debug:
            print time.strftime(self.debugDateFormat) + " - " + msg

    def info(self, msg):
        print time.strftime(self.debugDateFormat) + " - " + msg

    def getObjectColumns(self):
        """ Get the database columns that are being filled, and their data type

        """
        results = {}
        sqlstr = "select hdu, UPPER(attribute_name), position, column_name, datafile_datatype from ops_datafile_metadata where filetype = '%s'" % (self.filetype)
        if self.order is not None:
            sqlstr += " order by %s" % (self.order)
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr)
        records = cursor.fetchall()

        for rec in records:
            hdr = None
            if rec[0] == None:
                hdr = self.objhdu
            elif rec[0].upper() == 'PRIMARY':
                hdr = 0
            else:
                if ingestutils.isInteger(rec[0]):
                    hdr = int(rec[0])
                else:
                    hdr = rec[0]
            if hdr not in results:
                results[hdr] = OrderedDict()
            if rec[1] not in results[hdr]:
                results[hdr][rec[1]] = Entry(hdu=hdr, attribute_name=rec[1], position=rec[2], column_name=rec[3], dtype=rec[4])
            else:
                results[hdr][rec[1]].append(rec[3],rec[2])
        cursor.close()
        return results

    def getNumObjects(self):
        """ Get the number of items to be ingested, must be overloaded by child classes

        """
        return None

    def generateRows(self):
        """ convert the input data into a list of lists for ingestion into the database
            must be overloaded by child classes to handle individual data types

        """
        return None

    def numAlreadyIngested(self):
        """ Determine the number of entries already ingested from the data source

        """
        sqlstr = "select count(*) from %s where filename='%s'" % (self.targettable, self.shortfilename)
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr)# % self.targettable,{"fname":self.shortfilename})
        count = cursor.fetchone()[0]
        
        return count


    def isLoaded(self):
        """ determine if the data have already been loaded into the database,
            based on file name

        """
        loaded = False

        numDbObjects = self.numAlreadyIngested()
        numCatObjects = self.getNumObjects()
        if numDbObjects > 0:
            loaded = True
            if numDbObjects == numCatObjects:
                self.info("INFO: file " + self.fullfilename + 
                          " already ingested with the same number of" +
                          " objects. Skipping.")
            else:
                miscutils.fwdebug_print(("ERROR: file " + self.fullfilename +
                          " already ingested, but the number of objects is" +
                          " DIFFERENT: catalog=" + str(numCatObjects) +
                          "; DB=" + str(numDbObjects) + ".")

        return loaded

    def executeIngest(self):
        """ Generic method to insert the data into the database

        """
        if self.generateRows() == 1:
            return 1
        for k,v in self.constants.iteritems():
            if isinstance(v, str):
                self.constants[k] = "'" + v + "'"
            else:
                self.constants[k] = str(v)
        columns = []

        for att in self.orderedColumns:
            columns += self.dbDict[self.objhdu][att].column_name
        places = []
        for i in range(len(columns)):
            places.append(":%d" % (i+1))

        sqlstr = "insert into %s ( " % (self.targettable)
        sqlstr += ', '.join(self.constants.keys() + columns)
        sqlstr += ") values ("
        sqlstr +=  ', '.join(self.constants.values() + places)
        sqlstr += ")"
        cursor = self.dbh.cursor()
        cursor.prepare(sqlstr)
        #print sqlstr
        try:
            #for dt in self.sqldata:
            #    print dt
            cursor.executemany(None, self.sqldata)
            cursor.close()
            self.dbh.commit()
            self.info("Inserted %d rows into table %s" % (len(self.sqldata), self.targettable))
            return 0
        except:
            se = sys.exc_info()
            e = se[1]
            tb = se[2]
            print "Exception raised: ",e
            print "Traceback: "
            traceback.print_tb(tb)
            print " "
            self.dbh.rollback()
            return 1


class Entry(object):
    __slots__ = ["hdu", "attribute_name", "position", "column_name", "dtype"]
    def __init__(self, **kwargs):
        """ Simple light weight class to hold entries from the ops_datafile_metadata
            table

        """
        self.hdu = None
        self.attribute_name = None
        self.position = [0]
        self.column_name = []
        self.dtype = None

        for item in ["column_name", "position"]:
            if item in kwargs:
                setattr(self,item,[kwargs[item]])
                del kwargs[item]
        for kw, arg in kwargs.iteritems():
            setattr(self, kw, arg)
        if len(self.position) != len(self.column_name):
            raise Exception("BAD MATCH %d  %d" % (len(self.position),len(self.column_name)))

    def append(self, column_name, position):
        """ Method to append data to specific elements of the class

        """
        self.column_name.append(column_name)
        self.position.append(position)
