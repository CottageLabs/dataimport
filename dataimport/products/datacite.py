import json

from dataimport.analyses.acled.extract_acled import ExtractACLEDData
from dataimport.analyses.ons.extract_ons import ExtractONSData
from dataimport.lib.assemble_datacite import get_invenio_record
from dataimport.product import Product

datacite_intermediary_schema = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},  # unique id of the dataset
        "source": {"type": "string"},  # unique id of the source
        "name": {"type": "string"},  # human-readable name of dataset
        "description": {"type": "string"},  # human-readable description of dataset,
        "published": {"type": "string"},  # date dataset was published
        "creators": {"type": "array"},
        "license": {"type": "string"},  # license dataset was published under
        "locations": {"type": "array"},
        "languages": {"type": "array"},
        "subjects": {"type": "array"},
        "formats": {"type": "array"},
        "rights": {"type": "array"},
        "dates": {"type": "array"},
        "publisher": {"type": "string"},
        "datasets": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "size": {"type": "string"},
                    "title": {"type": "string"},
                },
                "additionalProperties": False
            }
        },
        "url": {"type": "string"},
        "url_doc": {"type": "string"},
        "url_api": {"type": "string"}
    },
    "required": ["id", "source", "name", "creators", "description", "datasets"],
    "additionalProperties": False
}


class Datacite(Product):
    FORMATS = []

    def analyse(self):
        """
        This method extracts the json objects from the json files in from
        the datasource and inserts into one file, ons.json.
        """
        self.log("Analysing data for Datacite")
        datacite_records = []

        with self.file_manager.output_file("datacite.json") as f:
            for source in self.get_sources():
                if source.provides_analysis(ExtractONSData):
                    datacite_records.extend(source.analysis(ExtractONSData).entries())
                if source.provides_analysis(ExtractACLEDData):
                    datacite_records.extend(source.analysis(ExtractACLEDData).entries())

            f.write(json.dumps(datacite_records))

    def assemble(self):
        """

        """
        self.log("Preparing Datacite data")
        outfile = self.file_manager.file_path("invenio.json")

        with self.file_manager.input_file("datacite.json") as f:
            data = json.loads(f.read())

        self.log(f'Writing to {outfile}')
        with self.file_manager.output_file('invenio.json') as f:
            f.write(json.dumps([get_invenio_record(d) for d in data]))

    def assembled(self):
        with self.file_manager.input_file("invenio.json") as f:
            data = json.loads(f.read())

        return data
