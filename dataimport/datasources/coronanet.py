import json
import re
from datetime import datetime

from jsonschema.validators import validate

from dataimport.analyses.acled.extract_acled import ExtractACLEDData, ExtractACLEDDataFromJSON
from dataimport.datasource import Datasource
from dataimport.lib.http_util import rate_limited_req
from bs4 import BeautifulSoup

from dataimport.products.datacite import datacite_intermediary_schema


class CORONANET(Datasource):
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
        This method retrieves and saves as html the landing page for each dataset listed on the ONS_SEARCH
        page.
        """
        self.log("Loading from Coronanet query")

        h = {'User-Agent': self.config.USER_AGENT}

        url = self.config.CORONANET_URL
        resp = rate_limited_req('get', url, headers=h)

        self.log('Fetching dataset')

        with self.file_manager.output_file(f'coronanet{self.config.ORIGIN_SUFFIX}.html', mode="wb") as htmlfile:
            htmlfile.write(resp.content)

    def analyse(self):
        self.extract_coronanet_data()

    def analysis(self, analysis_class):
        if analysis_class == ExtractACLEDData:
            return self._extract_acled_data_analysis()

    def extract_coronanet_data(self):
        """
        This method extracts useful data from the landing page the dataset.

        This method scrapes data from the landing page and saves in a distinct .json file in the same folder.
        """
        h = {'User-Agent': self.config.USER_AGENT,
             'Authorization': self.config.GITHUB_TOKEN}

        for dataset_filename in self.file_manager.list_files():
            origin = self.file_manager.file_path(dataset_filename)
            dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]
            outfile = self.file_manager.file_path(dataset + ".json")

            self.log("extracting data dump {x} to {y}".format(x=origin, y=outfile))

            with self.file_manager.input_file(dataset_filename) as i:
                soup = BeautifulSoup(i.read(), 'html.parser')
                scripts = None

                for script in soup.findAll('script'):
                    if script.find(text=re.compile('getLastDataUpdate')):
                        re_pattern = r'getLastDataUpdate\(\'(.*)\', \'(.*)\'\)'
                        scripts = re.findall(re_pattern, script.text, re.MULTILINE)

                buttons = soup.findAll('button', {'class': 'languageButton'})

                for ind, script in enumerate(scripts):
                    _id = f'coronanet_{script[1][10:].lower()}'
                    with self.file_manager.output_file(_id + ".json") as o:
                        dataset_panel = soup.find('time', {'id': script[1]}).parent.parent
                        last_git_commit = rate_limited_req('get', script[0], headers=h)
                        last_updated = last_git_commit.json()[0]['commit']['committer']['date']
                        desc = dataset_panel.text.split('\n')[2].strip()
                        title = buttons[ind].text
                        dataset_url = dataset_panel.find_all('button', text='Download')[0]['formaction']

                        cnet_data = {'name': title,
                                     # 'publisher': 'The Armed Conflict Location & Event Data Project (ACLED)',
                                     'id': _id,
                                     'source': self.id,
                                     'url': self.config.CORONANET_URL + f'#{_id}',
                                     'description': desc,
                                     'published': last_updated[:10],
                                     'subjects': [{'subject': 'politics'},
                                                  {'subject': 'governments'},
                                                  {'subject': 'politics'}],
                                     # 'publicationDate': "2020",
                                     "creators": [{
                                         "person_or_org": {
                                             "name": "CoronaNet",
                                             "identifiers": [{
                                                 "identifier": "https://www.coronanet-project.org/index.html",
                                                 "scheme": "url"
                                             }],
                                             "type": "organizational"
                                         }
                                     }],
                                     # 'datasetDocumentation': "https://www.coronanet-project.org/assets/CoronaNet_Codebook_Panel.pdf",
                                     "datasets": [
                                         {
                                             # "@type": "DataDownload",
                                             # "encodingFormat": "xlsx",
                                             "url": dataset_url,
                                             "size": '',
                                             "title": 'Dataset'
                                         }]}

                        validate(cnet_data, datacite_intermediary_schema)
                        o.write(json.dumps(cnet_data))

    def _extract_acled_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json')]
        return ExtractACLEDDataFromJSON(self.id, filepaths=paths)
