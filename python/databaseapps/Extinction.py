import fitsio
from FitsIngest import FitsIngest


class Extinction(FitsIngest):

    def __init__(self, datafile, idDict, filetype, dbh):
        """ Class to ingest exctintion_ebv and extinction_band data

        """
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh)

        self.constants = {
            "FILENAME": self.shortfilename,
        }
        if filetype != 'coadd_extinct_ebv':
            self.header = fitsio.read_header(datafile, self.dbDict[self.objhdu]['BAND'].hdu)
            band = self.header['BAND'].strip()
            self.constants["BAND"] = band
