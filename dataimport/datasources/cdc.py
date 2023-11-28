import json
import re
from datetime import date, datetime

from dataimport.analyses.ons.extract_ons import ExtractONSDataFromJSON, ExtractONSData
from dataimport.datasource import Datasource
from dataimport.lib.http_util import rate_limited_req
from bs4 import BeautifulSoup
from jsonschema import validate

from dataimport.products.datacite import datacite_intermediary_schema


def get_file_size(text: str) -> str:
    match = re.search(r'.*\((.*?)\)', text)

    if match:
        return match.group(1)


def get_published(url: str, h: dict):
    resp = rate_limited_req('get', url, headers=h)
    soup = BeautifulSoup(resp.content, 'html.parser')
    script = soup.find_all("script", attrs={'type': 'application/ld+json'})[0]
    json_obj = json.loads(script.text)
    published = json_obj['datePublished'][:10]

    return published


class CDC(Datasource):
    """

    """
    ANALYSES = [ExtractONSData]

    def fetch(self):
        """
        This method retrieves and saves as html the landing page for each dataset listed on the ONS_SEARCH
        page.
        """
        self.log("Loading from CDC query")

        h = {'User-Agent': self.config.USER_AGENT}

        url = self.config.CDC_URL
        resp = rate_limited_req('get', url, headers=h)
        dataset_file = "cdc" + self.config.ORIGIN_SUFFIX + '.html'

        with self.file_manager.output_file(dataset_file, mode="wb") as htmlfile:
            htmlfile.write(resp.content)

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
        for dataset_filename in self.file_manager.list_files():
            origin = self.file_manager.file_path(dataset_filename)
            # dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]

            with self.file_manager.input_file(dataset_filename) as i:
                soup = BeautifulSoup(i.read(), 'html.parser')
            datasets = soup.select(".browse2-results")[0].find_all('div', recursive=False)

            for dataset in datasets:
                title_tag = dataset.select('.browse2-result-name-link')[0]
                url = title_tag['href']

                resp = rate_limited_req('get', url, headers={'User-Agent': self.config.USER_AGENT})
                soup = BeautifulSoup(resp.content, 'html.parser')
                script = soup.find_all("script", attrs={'type': 'application/ld+json'})[0]
                json_obj = json.loads(script.text)

                published = json_obj['datePublished'][:10]
                dataset_id = url.split('/')[-1]
                title = title_tag.text.strip()
                description = dataset.select('.browse2-result-description')[0].text
                timestamp = dataset.select('.browse2-result-timestamp-value')[0].select('.dateLocalize')[0][
                    'data-rawdatetime']
                last_updated = date.fromtimestamp(int(timestamp)).isoformat()
                keywords = dataset.select('.browse2-result-topics')
                dataset_csv_url = f'https://data.cdc.gov/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD'
                dataset_json_url = f'https://data.cdc.gov/api/views/{dataset_id}/rows.json?accessType=DOWNLOAD'
                dataset_xml_url = f'https://data.cdc.gov/api/views/{dataset_id}/rows.xml?accessType=DOWNLOAD'
                outfile = self.file_manager.file_path(dataset_id + ".json")

                self.log("extracting data dump {x} to {y}".format(x=origin, y=outfile))

                dataset_json = {
                    'source': 'cdc',
                    'name': title,
                    'id': dataset_id,
                    'url': url,
                    'description': description,
                    'creators': [{
                        "person_or_org": {
                            "name": 'Centers for Disease Control and Prevention',
                            "identifiers": [
                                {
                                    "identifier": "https://www.cdc.gov/",
                                    "scheme": "url"
                                }
                            ],
                            "type": "organizational"
                        }
                    }],
                    'published': published,
                    "locations": [
                        {
                            "place": "United States",
                            "identifiers": [
                                {
                                    "scheme": "geonames",
                                    "identifier": "6252001"
                                }
                            ]
                        }],
                    'languages': [{"id": "eng"}],
                    "dates": [
                        {
                            "date": last_updated,
                            "type": {
                                "id": "updated"
                            }
                        }
                    ],
                    "datasets": [{'url': dataset_csv_url,
                                  'title': 'Dataset (CSV)'},
                                 {'url': dataset_json_url,
                                  'title': 'Dataset (JSON)'},
                                 {'url': dataset_xml_url,
                                  'title': 'Dataset (XML)'}]
                }

                if 'keywords' in json_obj:
                    dataset_json['subjects'] = [{"subject": s} for s in json_obj['keywords']]

                validate(dataset_json, datacite_intermediary_schema)

                with self.file_manager.output_file(outfile) as o:
                    o.write(json.dumps(dataset_json))

    def _extract_ons_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json')]
        return ExtractONSDataFromJSON(self.id, filepaths=paths)
