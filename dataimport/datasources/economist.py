import json
import re
from datetime import datetime

from jsonschema.validators import validate

from dataimport.analyses.acled.extract_acled import ExtractACLEDData, ExtractACLEDDataFromJSON
from dataimport.datasource import Datasource
from dataimport.lib.http_util import rate_limited_req
from bs4 import BeautifulSoup

from dataimport.products.datacite import datacite_intermediary_schema


# https://api.github.com/repos/TheEconomist/covid-19-excess-deaths-tracker/commits?path=output-data/excess-deaths/all_monthly_excess_deaths.csv&page=1&per_page=1
# https://github.com/TheEconomist/covid-19-excess-deaths-tracker/raw/master/output-data/excess-deaths/all_weekly_excess_deaths.csv

class ECONOMIST(Datasource):
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
        pass
        '''self.log("Loading from ECONOMIST query")

        h = {'User-Agent': self.config.USER_AGENT}

        url = self.config.ACLED_URL
        resp = rate_limited_req('get', url, headers=h)

        self.log('Fetching dataset')

        with self.file_manager.output_file(f'acled{self.config.ORIGIN_SUFFIX}.html', mode="wb") as htmlfile:
            htmlfile.write(resp.content)'''

    def analyse(self):
        self.extract_acled_data()

    def analysis(self, analysis_class):
        if analysis_class == ExtractACLEDData:
            return self._extract_acled_data_analysis()

    def extract_acled_data(self):
        """
        The metadata for this record will be hardcoded here. The only reason a harvester is needed is because
        Github is scraped for latest update to the dataset.
        """

        economist_data = {
            "name": "The Economist's tracker for covid-19 excess deaths",
            "publisher": "The Economist",
            "creators": [
                {
                    "person_or_org": {
                        "given_name": "James",
                        "family_name": "Tozer",
                        "type": "personal"
                    }
                },
                {
                    "person_or_org": {
                        "given_name": "Martín",
                        "family_name": "González",
                        "type": "personal"
                    }
                },
            ],
            "id": "economist_excess",
            "source": "economist",
            "url": "https://github.com/TheEconomist/covid-19-excess-deaths-tracker",
            "description": "This repository contains the data behind The Economist’s tracker for covid-19 excess deaths, which updates daily, and the code that we have used to clean, analyse and present the numbers.",
            "published": "2020-05-15",
            "languages": [
                {
                    "id": "eng"
                }
            ],
            "rights": [
                {
                    "id": "cc-by-4.0"
                }
            ],
            "subjects": [
                {
                    "subject": "mortality"
                },
                {
                    "subject": "epidemiology"
                }
            ],
            #"dates": [
            #    {
            #        "date": "2020-12-10",
            #        "type": {
            #            "id": "updated"
            #       }
            #    }
            #],
            "datasets": [
                {
                    "url": "https://github.com/TheEconomist/covid-19-excess-deaths-tracker/raw/master/output-data/excess-deaths/all_monthly_excess_deaths.csv",
                    "title": "Monthly Global Excess Deaths (CSV)"
                },
                {
                    "url": "https://github.com/TheEconomist/covid-19-excess-deaths-tracker/raw/master/output-data/excess-deaths/all_weekly_excess_deaths.csv",
                    "title": "Weekly Global Excess Deaths (CSV)"
                }
            ]
        }

        validate(economist_data, datacite_intermediary_schema)

        with self.file_manager.output_file("economist.json") as o:
            o.write(json.dumps(economist_data))

        '''for dataset_filename in self.file_manager.list_files():
            origin = self.file_manager.file_path(dataset_filename)
            dataset = dataset_filename[:dataset_filename.find(self.config.ORIGIN_SUFFIX)]
            outfile = self.file_manager.file_path(dataset + ".json")

            self.log("extracting data dump {x} to {y}".format(x=origin, y=outfile))

            with self.file_manager.input_file(dataset_filename) as i:
                with self.file_manager.output_file(dataset + ".json") as o:
                    soup = BeautifulSoup(i.read(), 'html.parser')

                    dataset_parent = soup.findAll('div', {'class': 'download-box-content'})[0]
                    dataset_title = dataset_parent.find('h1')
                    dataset_link = dataset_parent.findAll('a')[0]
                    dataset_size = dataset_link.findAll('small')[0].text.split(' – ')[1]
                    m = re.match(r".*\((.*)\)", dataset_title.text)

                    dataset_publication_date = datetime.strptime(m.groups()[0], '%d %B %Y').date().isoformat()

                    acled_data = {'name': "COVID-19 Disorder Tracker",
                                  # 'publisher': 'The Armed Conflict Location & Event Data Project (ACLED)',
                                  'id': 'acled',
                                  'source': self.id,
                                  'url': "https://acleddata.com/analysis/covid-19-disorder-tracker/",
                                  'description': "The COVID-19 Disorder Tracker (CDT) provides special coverage of the pandemics impact on political violence and protest around the world, monitoring changes in demonstration activity, state repression, mob attacks, overall rates of armed conflict, and more.",
                                  'published': dataset_publication_date,
                                  'subjects': [{'subject': 'politics'},
                                               {'subject': 'society'},
                                               {'subject': 'democracy'}],
                                  # 'publicationDate': "2020",
                                  "creators": [{
                                      "person_or_org": {
                                          "name": "The Armed Conflict Location & Event Data Project (ACLED)",
                                          "identifiers": [{
                                              "identifier": "https://acleddata.com",
                                              "scheme": "url"
                                          }],
                                          "type": "organizational"
                                      }
                                  }],
                                  # 'datasetDocumentation': "https://acleddata.com/acleddatanew/wp-content/uploads/dlm_uploads/2020/04/ACLED_Direct-COVID19-Disorder_Methodology-Brief_4.2020.pdf",
                                  "datasets": [
                                      {
                                          # "@type": "DataDownload",
                                          # "encodingFormat": "xlsx",
                                          "url": dataset_link['href'].split('?')[0],
                                          "size": dataset_size,
                                          "title": dataset_title.text
                                      }]}'''

    def _extract_acled_data_analysis(self):
        paths = [self.file_manager.file_path(p) for p in self.file_manager.list_files('.json')]
        return ExtractACLEDDataFromJSON(self.id, filepaths=paths)
