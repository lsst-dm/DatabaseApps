from Ingest import Ingest

class Mangle(Ingest):
    """ Class to ingest the outputs from a Mangle run

    """
    def __init__(self, datafile, filetype, idDict, dbh):
        Ingest.__init__(self, filetype, datafile, "CSV", '3', dbh)
        self.hdu = "CSV"
        self.idDict = idDict
        self.coadd_id = None
        self.constants = {"FILENAME" : self.shortfilename}

        if "COADD_OBJECT_ID" in self.dbDict[self.hdu].keys():
            self.coadd_id = self.dbDict[self.hdu]["COADD_OBJECT_ID"].position[0]

    def parseCSV(self, filename, types):
        """ Parse a CSV file, casting as needed into a list of lists

        """
        f = open(filename, 'r')
        lines = f.readlines()

        for line in lines:
            tdata = line.split(",")
            # cast the data appropriately
            for i, d in enumerate(tdata):
                if self.coadd_id is not None and i == self.coadd_id:
                    tdata[i] = self.idDict[types[i](d)]
                else:
                    tdata[i] = types[i](d)
            self.sqldata.append(tdata)
        f.close()

    def generateRows(self):
        """ Method to convert the input data into a list of lists

        """
        types = []
        # create a list of objects used to cast the data
        for item in self.dbDict[self.hdu].values():
            if item.dtype.upper() == "INT":
                types.append(int)
            elif item.dtype.upper() == "FLOAT":
                types.append(float)
            else:
                types.append(str)
        self.parseCSV(self.fullfilename, types)
        self.orderedColumns = self.dbDict[self.hdu].keys()

    def getNumObjects(self):
        """ Get the number of objects to ingest

        """
        count = 0
        f = open(self.fullfilename, 'r')
        lines = f.readlines()
        count += len(lines)
        f.close()
        return count
