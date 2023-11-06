import json
from datetime import datetime
from typing import List

from jsonschema.validators import validate

from dataimport.analyses.acled.extract_acled import ExtractACLEDData, ExtractACLEDDataFromJSON
from dataimport.datasource import Datasource
from dataimport.lib.http_util import rate_limited_req

from dataimport.products.datacite import datacite_intermediary_schema


def get_if_english(descriptions: dict) -> str:
    if 'en' in descriptions:
        return str(descriptions['en']).strip()
    else:
        return str(descriptions[list(descriptions.keys())[0]]).strip()


def get_datasets(distributions: List) -> List:
    return [{'url': d['access_url'][0] if 'access_url' in d else '',
             'title': get_if_english(d['title']) if 'title' in d else 'Untitled dataset'} for d in distributions]


class EUROPA(Datasource):
    """
    ONS provides a public API that unfortunately only seems to have a subset of datasets available.
    General documentation at https://developer.ons.gov.uk/.

    In a blog post from 2021 the question is asked "Why isn’t the data I want available?"
    See: https://digitalblog.ons.gov.uk/2021/02/15/how-to-access-data-from-the-ons-beta-api/

    The data we need is therefore scraped by accessing a page at ONS that lists all C19 related datasets
    See the constant ONS_SEARCH in settings.
    """
    ANALYSES = [ExtractACLEDData]

    def fetch(self):
        """
        This method retrieves and saves the json for each dataset paginated on the Europa API.
        """
        self.log("Loading from EUROPA query")

        h = {'User-Agent': self.config.USER_AGENT}

        page = 0
        url = self.config.EUROPA_URL + f'&page={page}'
        resp = rate_limited_req('get', url, headers=h)
        data = resp.json()['result']

        while len(data['results']):
            self.log(f'Fetching dataset {page}', update=True)

            with self.file_manager.output_file(f'europa-{page}{self.config.ORIGIN_SUFFIX}.json', mode="w") as jsonfile:
                jsonfile.write(json.dumps(data))

            page += 1
            url = self.config.EUROPA_URL + f'&page={page}'
            resp = rate_limited_req('get', url, headers=h)
            data = resp.json()['result']

    def analyse(self):
        self.extract_europa_data()

    def analysis(self, analysis_class):
        if analysis_class == ExtractACLEDData:
            return self._extract_acled_data_analysis()

    def extract_europa_data(self):
        """
        This method extracts useful data from the API.
        """

        for dataset_filename in self.file_manager.list_files():
            origin = self.file_manager.file_path(dataset_filename)
            # dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]

            with self.file_manager.input_file(dataset_filename) as i:
                europa_batch = json.loads(i.read())

                for result in europa_batch['results']:
                    if not result['distributions']:
                        self.log(get_if_english(result['title']) + ' contains no datasets.')
                        continue

                    europa_data = {'name': get_if_english(result['title']),
                                   'publisher': {'name': result['catalog']['publisher']['name'],
                                                 'type': 'url', 'id': result['catalog']['publisher']['homepage']},
                                   'id': result['id'],
                                   'source': self.id,
                                   'url': result['resource'],
                                   # 'publicationDate': "2020",
                                   # 'keywords': ["politics", "society", "democracy"],
                                   "datasets": get_datasets(result['distributions'])}

                    if 'issued' in result and result['issued']:
                        europa_data['published'] = result['issued'][:10]
                    else:
                        europa_data['published'] = datetime.now().date().isoformat()

                    if 'description' in result and result['description']:
                        europa_data['description'] = get_if_english(result['description'])
                    else:
                        europa_data['description'] = 'No description available'

                    validate(europa_data, datacite_intermediary_schema)
                    outfile = self.file_manager.file_path(result['id'] + ".json")

                    self.log("extracting data dump {x} to {y}".format(x=origin, y=outfile))
                    with self.file_manager.output_file(result['id'] + ".json") as o:
                        o.write(json.dumps(europa_data))

    def _extract_acled_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json') if self.config.ORIGIN_SUFFIX not in p]
        return ExtractACLEDDataFromJSON(self.id, filepaths=paths)
