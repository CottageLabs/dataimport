import json
import re

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


class ECDC(Datasource):
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

        url = self.config.ECDC_URL
        resp = rate_limited_req('get', url, headers=h)
        dataset_file = "ecdc" + self.config.ORIGIN_SUFFIX + '.html'

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
        base_href = 'https://www.ecdc.europa.eu'
        h = {'User-Agent': self.config.USER_AGENT}

        for dataset_filename in self.file_manager.list_files():
            origin = self.file_manager.file_path(dataset_filename)
            # dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]

            with self.file_manager.input_file(dataset_filename) as i:
                soup = BeautifulSoup(i.read(), 'html.parser')

            dataset_containers = [soup.find('h2', text=re.compile('Archived data')).parent.parent]
            # This is pretty brittle
            dataset_containers.append(dataset_containers[0].find_next_sibling('div'))

            dataset_containers = [d.find('div', {'class': 'field--items'}).findChildren('div', recursive=False) for d in
                                  dataset_containers]
            datasets = [i for d in dataset_containers for i in d]

            for dataset in datasets:
                title = dataset.find('h3', {'class': 'ct__title'}).text.strip()

                if 'Archived' in title:
                    title = title.replace('Archived', '').strip()

                url = base_href + dataset.find('a', {'class': 'ct__link'})['href']
                dataset_id = url.split('/')[-1]
                dataset_page = rate_limited_req('get', url, headers=h)
                soup = BeautifulSoup(dataset_page.content, 'html.parser')
                desc = soup.find('p', {'class': 'ct__page-description'}).text

                url_doc_tag = soup.find('a', text=re.compile(r'Data dictionary'))

                if desc.startswith('\n'):
                    desc = desc[3:].lstrip()

                published = soup.find('time')['datetime'][:10]
                download_container = soup.find('div', {'class': 'wysiwyg-content'})
                datasets = []

                if download_container is None:
                    continue

                for l in download_container.findAll('a', {'class': 'btn'}):
                    if 'Download' in l.text:
                        format = l.text.split(' ')[-1]
                        datasets.append({'url': l['href'], 'title': f'Dataset ({format})'})

                outfile = self.file_manager.file_path(dataset_id + ".json")

                dataset_json = {
                    'source': 'ecdc',
                    'name': title,
                    'id': dataset_id,
                    'url': url,
                    'description': desc,
                    'creators': [{
                        "person_or_org": {
                            "name": 'European Centre for Disease Prevention and Control',
                            "identifiers": [
                                {
                                    "identifier": "https://www.ecdc.europa.eu/",
                                    "scheme": "url"
                                }
                            ],
                            "type": "organizational"
                        }
                    }],
                    'published': published,
                    "locations": [
                        {
                            "place": "Europe",
                            "identifiers": [
                                {
                                    "scheme": "geonames",
                                    "identifier": "6255148"
                                }
                            ]
                        }],
                    'languages': [{"id": "eng"}],
                    # "dates": [
                    #    {
                    #        "date": last_updated,
                    #        "type": {
                    #            "id": "updated"
                    #        }
                    #    }
                    # ],
                    "datasets": datasets
                }

                if url_doc_tag:
                    dataset_json['url_doc'] = url_doc_tag['href']

                validate(dataset_json, datacite_intermediary_schema)

                with self.file_manager.output_file(outfile) as o:
                    o.write(json.dumps(dataset_json))

    def _extract_ons_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json')]
        return ExtractONSDataFromJSON(self.id, filepaths=paths)
