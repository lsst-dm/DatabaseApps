from FitsIngest import FitsIngest

import fitsio

class Wavg(FitsIngest):
    def __init__(self, filetype, datafile, idDict, dbh, matchCount=True):
        """ Class to ingest wavg and wavg_oclink data

        """
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh, matchCount=matchCount)

        header = fitsio.read_header(datafile, self.dbDict[self.objhdu]['BAND'].hdu)
        band = header['BAND'].strip()

        self.constants = {
            "BAND": band,
            "FILENAME": self.shortfilename,
        }
