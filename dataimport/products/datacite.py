import json

from dataimport.analyses.ons.extract_ons import ExtractONSData
from dataimport.lib.assemble_ons import get_invenio_record
from dataimport.product import Product


class Datacite(Product):
    FORMATS = []

    def analyse(self):
        """
        This method extracts the json objects from the json files in from
        the datasource and inserts into one file, ons.json.
        """
        self.log("Analysing data for Datacite")
        sources = self.get_sources()
        ons = []

        for source in sources:
            if source.provides_analysis(ExtractONSData):
                ons.append(source.analysis(ExtractONSData))

        with self.file_manager.output_file("ons.json") as f:
            f.write(ons[0].entries())

    def assemble(self):
        """

        """
        self.log("Preparing Datacite data")
        outfile = self.file_manager.file_path("invenio.json")

        with self.file_manager.input_file("ons.json") as f:
            data = json.loads(f.read())

        self.log(f'Writing to {outfile}')
        with self.file_manager.output_file('invenio.json') as f:
            f.write(json.dumps([get_invenio_record(self.config, d[1]) for d in data.items()]))

    def assembled(self):
        with self.file_manager.input_file("invenio.json") as f:
            data = json.loads(f.read())

        return data
