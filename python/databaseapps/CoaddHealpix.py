from FitsIngest import FitsIngest

class CoaddHealpix(FitsIngest):

    def __init__(self, datafile, idDict, dbh):
        """ Class to ingest Coadd Healpix data

        """
        FitsIngest.__init__(self, 'coadd_hpix', datafile, idDict, dbh=dbh)

        self.constants = {
            "FILENAME": self.shortfilename,
            }
