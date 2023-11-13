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


def get_creator(result: dict) -> dict:
    print(json.dumps(result))
    if result['publisher'] and 'name' in result['publisher'] and 'resource' in result['publisher']:
        return {
            "person_or_org": {
                "name": result['publisher']['name'],
                "identifiers": [{
                    "identifier": result['publisher']['resource'],
                    "scheme": "url"
                }],
                "type": "organizational"
            }
        }
    '''else:
        return {
            "person_or_org": {
                "name": result['catalog']['publisher']['name'],
                "identifiers": [{
                    "identifier": "https://acleddata.com",
                    "scheme": "url"
                }],
                "type": "organizational"
            }
        }'''


def get_publisher(result: dict) -> str:
    if 'name' in result['publisher']:
        return result['publisher']['name']
    elif 'name' in result['catalog']['publisher']:
        return result['catalog']['publisher']['name']


class EUROPA(Datasource):
    """

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

                    if not get_creator(result):
                        self.log(get_if_english(result['title']) + ' has no author.')
                        continue

                    europa_data = {'name': get_if_english(result['title']),
                                   'publisher': get_publisher(result),
                                   'creators': [get_creator(result)],
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
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json') if
                 self.config.ORIGIN_SUFFIX not in p]
        return ExtractACLEDDataFromJSON(self.id, filepaths=paths)
