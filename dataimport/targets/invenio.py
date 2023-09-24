import json

import requests
from urllib3.exceptions import InsecureRequestWarning

from dataimport.lib.http_util import rate_limited_req
from dataimport.settings import INVENIO_TOKEN, INVENIO_API
from dataimport.target import Target

h = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {INVENIO_TOKEN}"
}

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def get_record(identifier: str) -> dict | None:
    r = rate_limited_req('get', f"{INVENIO_API}/api/records?q=metadata.identifiers.identifier:%22{identifier}%22",
                         headers=h, verify=False)
    resp = r.json()

    if len(resp['hits']['hits']) == 1:
        return resp['hits']['hits'][0]
    else:
        #  There shouldn't be more than one record with a URL identifier
        pass

    return None


def create_or_update_draft_record(data: dict, invenio_id: str = None):
    invenio_url = f"{INVENIO_API}/api/records"

    if invenio_id is not None:
        invenio_url += f"/{invenio_id}/draft"

        r = rate_limited_req('post', invenio_url, headers=h, verify=False)
        assert r.status_code == 201, \
            f"Failed to create draft record (code: {r.status_code}) \n {r.content}"

        data['pids'] = r.json().get('pids', {})

        r = rate_limited_req('put', r.json().get('links', {}).get('self'), data=json.dumps(data), headers=h,
                             verify=False)
        assert r.status_code == 200, \
            f"Failed to create record (code: {r.status_code}) \n {r.content}"

    else:
        r = rate_limited_req('post', invenio_url, data=json.dumps(data), headers=h, verify=False)
        assert r.status_code == 201, \
            f"Failed to create record (code: {r.status_code}) \n {r.content}"

    # Publish the record
    r = rate_limited_req('post', r.json()['links']['publish'], headers=h, verify=False)

    assert r.status_code == 202, \
        f"Failed to publish record (code: {r.status_code})"


class Invenio(Target):
    """ Upload to an Invenio API endpoint """

    def prepare(self):
        self.log('Preparing')
        product = self.get_products()[0]

        with self.file_manager.output_file("invenio.json") as f:
            f.write(json.dumps(product.assembled()))

    def load(self):
        self.log('Loading')

        with self.file_manager.input_file("invenio.json") as f:
            data = json.loads(f.read())

        for record in data:
            identifier = record['metadata']['identifiers'][0]['identifier']
            server_record = get_record(identifier)

            if server_record is None:
                self.log('Creating a new Invenio record')
                create_or_update_draft_record(record)
            else:
                invenio_id = server_record['id']
                record_datasets = record['custom_fields']['datasets']
                server_datasets = server_record['custom_fields']['datasets']
                server_dataset_identifiers = [d['url'] for d in server_datasets]
                new_datasets = [d for d in record_datasets if d['url'] not in server_dataset_identifiers]

                if new_datasets:
                    # sorted(server_record['metadata']) != sorted(record['metadata']) or
                    self.log(f'Updating Invenio record {invenio_id} with {len(new_datasets)} new datasets')

                    record['metadata'] = server_record['metadata']
                    # Prepending new datasets to the list
                    record['custom_fields']['datasets'] = new_datasets + server_datasets

                    create_or_update_draft_record(record, invenio_id)
                else:
                    self.log(f'No new datasets for {invenio_id}')

    def cleanup(self):
        super(Invenio, self).cleanup()
