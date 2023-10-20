import json

import requests
from slugify import slugify
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

community_template = {
    "access": {
        "visibility": "public",
        "member_policy": "open",
        "record_policy": "open"
    },
    "slug": "my_community_identifier",
    "metadata": {
        "title": "My Community",
        # "description": "This is an example Community.",
        "type": {
            "id": "organization"
        },
        # "curation_policy": "This is the kind of records we accept.",
        # "page": "Information for my community.",
        # "website": "https://inveniosoftware.org/",
        # "organizations": [{
        #        "name": "My Org"
        # }]
    }
}

cache_communities = []


def get_community(title: str) -> dict:
    print(cache_communities)
    # If we have already fetched the community, then just return it
    if len([c for c in cache_communities if c['metadata']['title'] == title]):
        return [c for c in cache_communities if c['metadata']['title'] == title][0]

    # Check if community already exists
    r = rate_limited_req('get', headers=h, verify=False,
                         url=f"{INVENIO_API}/api/communities?q=metadata.title:%22{title}%22")
    resp = r.json()

    if len(resp['hits']['hits']) == 1:
        if resp['hits']['hits'][0] not in cache_communities:
            cache_communities.append(resp['hits']['hits'][0])

        return resp['hits']['hits'][0]
    else:
        # Create new community
        community_template['slug'] = slugify(title)
        community_template['metadata']['title'] = title

        r = rate_limited_req('post', headers=h, verify=False, data=json.dumps(community_template),
                             url=f"{INVENIO_API}/api/communities")
        cache_communities.append(r.json())
        print('unique')
        print(r.json())
        print('-----')
        return r.json()


def get_record(identifier: str) -> dict | None:
    r = rate_limited_req('get', headers=h, verify=False,
                         url=f"{INVENIO_API}/api/records?q=metadata.identifiers.identifier:%22{identifier}%22")
    resp = r.json()

    if len(resp['hits']['hits']) == 1:
        return resp['hits']['hits'][0]
    else:
        #  There shouldn't be more than one record with a URL identifier
        pass

    return None


def create_or_update_draft_record(data: dict, invenio_id: str = None) -> dict:
    invenio_url = f"{INVENIO_API}/api/records"

    if invenio_id is not None:
        # Creating a record
        invenio_url += f"/{invenio_id}/draft"

        r = rate_limited_req('post', invenio_url, headers=h, verify=False)
        assert r.status_code == 201, \
            f"Failed to create draft record (code: {r.status_code}) \n {r.content}"

        data['pids'] = r.json().get('pids', {})

        r = rate_limited_req('put', r.json().get('links').get('self'), data=json.dumps(data), headers=h,
                             verify=False)
        assert r.status_code == 200, \
            f"Failed to create record (code: {r.status_code}) \n {r.content}"

        # Publish the record
        r = rate_limited_req('post', r.json()['links']['publish'], headers=h, verify=False)

        assert r.status_code == 202, \
            f"Failed to publish record (code: {r.status_code})"

    else:
        r = rate_limited_req('post', invenio_url, data=json.dumps(data), headers=h, verify=False)
        assert r.status_code == 201, \
            f"Failed to create record (code: {r.status_code}) \n {r.content}"

        record = r.json()

        community = get_community(record['metadata']['publisher'])
        # Adding a draft record to a community publishes the record
        add_record_to_community(record['id'], community['id'])

    return r.json()


def add_record_to_community(record_id: str, community_id: str):
    request_obj = {
        "receiver": {
            "community": community_id
        },
        "type": "community-submission"
    }
    r = rate_limited_req('put', url=f'{INVENIO_API}/api/records/{record_id}/draft/review',
                         data=json.dumps(request_obj), headers=h, verify=False)
    assert r.status_code == 200, \
        f"Failed to create record (code: {r.status_code}) \n {r.content}"

    links = r.json()['links']
    r = rate_limited_req('post', links["actions"]["submit"], headers=h, verify=False)

    assert r.status_code == 200, \
        f"Failed to create record (code: {r.status_code}) \n {r.content}"

    links = r.json()['links']
    r = rate_limited_req('post', links["actions"]["accept"], headers=h, verify=False)

    assert r.status_code == 200, \
        f"Failed to create record (code: {r.status_code}) \n {r.content}"


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
                server_datasets = server_record['custom_fields'].get('datasets', [])
                server_dataset_identifiers = [d['url'] for d in server_datasets]
                new_datasets = [d for d in record_datasets if d['url'] not in server_dataset_identifiers]

                if new_datasets:
                    # sorted(server_record['metadata']) != sorted(record['metadata']) or
                    self.log(f'Updating Invenio record {invenio_id} with {len(new_datasets)} new dataset/s')

                    record['metadata'] = server_record['metadata']
                    # Prepending new datasets to the list
                    record['custom_fields']['datasets'] = new_datasets + server_datasets

                    create_or_update_draft_record(record, invenio_id)
                else:
                    self.log(f'No new datasets for {invenio_id}')

    def cleanup(self):
        super(Invenio, self).cleanup()
