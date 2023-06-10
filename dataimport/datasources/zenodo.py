from dataimport.datasource import Datasource


class Zenodo(Datasource):
    """ Fetch data via a Zenodo query """

    def fetch(self):
        self.log("Loading from Zenodo query")