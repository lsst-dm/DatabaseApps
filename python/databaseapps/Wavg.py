from FitsIngest import FitsIngest

class Wavg(FitsIngest):
    def __init__(self, filetype, datafile, idDict, band, dbh):
        """ Class to ingest wavg and wavg_oclink data

        """
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh)

        self.band = band

        self.constants = {
            "BAND": band,
            "FILENAME": self.shortfilename,
        }
