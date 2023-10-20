import json
import re

from dataimport.analyses.ons.extract_ons import ExtractONSDataFromJSON, ExtractONSData
from dataimport.datasource import Datasource
from dataimport.lib.http_util import rate_limited_req
from bs4 import BeautifulSoup, Tag
from jsonschema import validate

from dataimport.products.datacite import datacite_intermediary_schema


def get_file_size(text: str) -> str:
    match = re.search(r'.*\((.*?)\)', text)

    if match:
        return match.group(1)


class ONS(Datasource):
    """
    ONS provides a public API that unfortunately only seems to have a subset of datasets available.
    General documentation at https://developer.ons.gov.uk/.

    In a blog post from 2021 the question is asked "Why isn’t the data I want available?"
    See: https://digitalblog.ons.gov.uk/2021/02/15/how-to-access-data-from-the-ons-beta-api/

    The data we need is therefore scraped by accessing a page at ONS that lists all C19 related datasets
    See the constant ONS_SEARCH in settings.
    """
    ANALYSES = [ExtractONSData]

    def fetch(self):
        """
        This method retrieves and saves as html the landing page for each dataset listed on the ONS_SEARCH
        page.
        """
        self.log("Loading from ONS query")

        h = {'User-Agent': self.config.USER_AGENT}

        url = self.config.ONS_URL + self.config.ONS_SEARCH
        resp = rate_limited_req('get', url, headers=h)
        soup = BeautifulSoup(str(resp.content), 'html.parser')
        datasets = [d for d in soup.select("#results div ul")[0] if isinstance(d, Tag)]

        for i, dataset in enumerate(datasets):
            dataset_url = dataset.find('a').get('data-gtm-uri')
            # dataset_name = dataset.find('a').get('aria-label').strip()

            self.log(f'Fetching dataset {i}/{len(datasets)}', update=i - 1 < len(datasets))

            nxt = self.config.ONS_URL + dataset_url
            resp = rate_limited_req('get', url=nxt, headers=h)
            dataset_file = dataset_url.split('/')[-1] + self.config.ORIGIN_SUFFIX + '.html'

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
            dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]
            outfile = self.file_manager.file_path(dataset + ".json")

            self.log("extracting data dump {x} to {y}".format(x=origin, y=outfile))

            with self.file_manager.input_file(dataset_filename) as i:
                with self.file_manager.output_file(dataset + ".json") as o:
                    soup = BeautifulSoup(i.read(), 'html.parser')
                    script = json.loads(soup.find('script').text)
                    downloads = soup.findAll('a', {'class': 'btn btn--primary btn--thick'})
                    download_descriptions = soup.findAll('h3', {'class': 'margin-top--0 margin-bottom--0'})

                    # contact = soup.find('address')
                    # print(contact.text.strip().split('\n')[0].replace(' and', ',').replace('&', ',').replace(',,', ','))
                    script['id'] = dataset
                    script['source'] = self.id
                    script['publisher'] = {
                            'name': 'Office for National Statistics',
                            'type': 'url',
                            'id': 'https://www.ons.gov.uk/'}
                    script['locations'] = {
                        "place": "United Kingdom",
                        "identifiers": [
                            {
                                "scheme": "geonames",
                                "identifier": "2635167"
                            }
                        ]
                    }
                    script["languages"] = [{"id": "eng"}]
                    del script['@context']
                    del script['@type']
                    script['published'] = script.pop('datePublished')[:10]
                    del script['distribution']
                    script['datasets'] = []  # script.pop('distribution')
                    # template = script['datasets'].pop()
                    # del template['@type']
                    # self.log(template)

                    # Four datasets offer more than one format of the data, they are incorrectly
                    # formatted
                    if len(downloads) != len(download_descriptions):
                        script['datasets'] = []
                        # They will have 5 divs
                        divs = soup.findAll('div', {'class': 'show-hide'})[0].findChildren('div')

                        desc = divs[0].find('h3').text.strip()

                        links = []

                        for div in divs[1:]:
                            for download in [a for a in div.findAll('a') if a['href'] not in links]:
                                template = {}
                                template['url'] = self.config.ONS_URL + download['href']
                                template['size'] = get_file_size(download.text.strip())
                                template['title'] = desc
                                # template['encodingFormat'] = download['href'][download['href'].rfind('.') + 1:]
                                script['datasets'].append(template)

                                # template = copy.deepcopy(template)
                                links.append(download['href'])

                    else:
                        for download, desc in zip(downloads, download_descriptions):
                            template = {}
                            template['url'] = self.config.ONS_URL + download['href']
                            template['size'] = get_file_size(download.text.strip())
                            template['title'] = desc.text.strip()
                            # template['encodingFormat'] = download['href'][download['href'].rfind('.') + 1:]
                            script['datasets'].append(template)

                            # template = copy.deepcopy(template)

                    validate(script, datacite_intermediary_schema)

                    o.write(json.dumps(script))

    def _extract_ons_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json')]
        return ExtractONSDataFromJSON(self.id, filepaths=paths)
