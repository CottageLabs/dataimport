import json
from dataimport.datasource import Datasource
from dataimport.lib.http_util import rate_limited_req


class Zenodo(Datasource):
    """ Fetch data via a Zenodo query """

    def fetch(self):
        self.log("Loading from Zenodo query")

        url = self.config.ZENODO_URL + self.config.ZENODO_SEARCH
        headers = {'Content-Type': 'application/json'}

        retrieved_records = []
        resp = rate_limited_req('get', url=url, headers=headers)

        while True:
            retrieved_records += resp.json().get('hits', {}).get('hits', [])
            nxt = resp.json().get('links', {}).get('next')
            if nxt is None:
                break
            resp = rate_limited_req('get', url=nxt, headers=headers)

        with self.file_manager.output_file("origin.tar.gz", mode="wb") as tarball:
            tarball.write(json.dumps(retrieved_records))
