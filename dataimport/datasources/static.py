import json
import os

from dataimport.analyses.ons.extract_ons import ExtractONSDataFromJSON, ExtractONSData
from dataimport.datasource import Datasource
from bs4 import BeautifulSoup
from jsonschema import validate

from dataimport.products.datacite import datacite_intermediary_schema


class STATIC(Datasource):
    """
    """
    ANALYSES = [ExtractONSData]

    def fetch(self):
        """
        The static datasource processes static, local information.
        """
        print(self.config.STORE_SCOPES[self.id])

    def analyse(self):
        self.extract_ons_data()

    def analysis(self, analysis_class):
        if analysis_class == ExtractONSData:
            return self._extract_ons_data_analysis()

    def extract_ons_data(self):
        """
        This method extracts useful data from the landing page for each dataset.

        Metadata is very sparse, but handily available in the first script tag in the header as JSON-LD.

        This method extracts that JSON-LD data and saves in a distinct .json file in the same folder.

        For example:
        https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/datasets/numberofdeathsincarehomesnotifiedtothecarequalitycommissionengland
        """
        # print(os.listdir(self.config.STATIC))

        for dataset_filename in os.listdir(self.config.STATIC):
            origin = os.path.join(self.config.STATIC, dataset_filename)
            # print(self.file_manager.current_dir_created())
            #dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]
            outfile = self.file_manager.file_path(dataset_filename)

            self.log("extracting data dump {x} to {y}".format(x=origin, y=outfile))

            with self.file_manager.input_file(origin) as i:
                with self.file_manager.output_file(outfile) as o:
                    data = json.loads(i.read())
                    validate(data, datacite_intermediary_schema)

                    o.write(json.dumps(data))

    def _extract_ons_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json')]
        return ExtractONSDataFromJSON(self.id, filepaths=paths)
