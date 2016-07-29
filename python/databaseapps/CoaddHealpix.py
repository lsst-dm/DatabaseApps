from FitsIngest import FitsIngest

class CoaddHealpix(FitsIngest):

    def __init__(self, filetype, datafile, idDict, dbh):
        """ Class to ingest Coadd Healpix data

        """
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh)

        self.constants = {
            "FILENAME": self.shortfilename,
            }
