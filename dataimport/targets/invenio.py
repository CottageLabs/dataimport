import json

import requests

from dataimport.settings import INVENIO_TOKEN, INVENIO_API
from dataimport.target import Target

h = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {INVENIO_TOKEN}"
}


def send_to_invenio(data: dict):
    r = requests.post(
        f"{INVENIO_API}/api/records", data=json.dumps(data), headers=h, verify=False)
    assert r.status_code == 201, \
        f"Failed to create record (code: {r.status_code})"
    links = r.json()['links']

    # Publish the record
    r = requests.post(links["publish"], headers=h, verify=False)

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
            send_to_invenio(record)

    def cleanup(self):
        super(Invenio, self).cleanup()
