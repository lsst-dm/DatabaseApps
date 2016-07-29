from FitsIngest import FitsIngest

class Wavg(FitsIngest):
    def __init__(self, filetype, datafile, idDict, dbh):
        """ Class to ingest wavg and wavg_oclink data

        """
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh)

        header = fitsio.read_header(datafile, self.objhdu)
        band = header['BAND'].strip()

        self.constants = {
            "BAND": band,
            "FILENAME": self.shortfilename,
        }
