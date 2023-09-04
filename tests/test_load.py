import json
import re
import sys

import requests

from dataimport.cli import entry_point
from dataimport.formats.json_feed import JSONFeed
from dataimport.target import Target
from tests.fixtures import DSPACE_ITEM_TEMPLATE
from tests.mock_settings import DSPACE_API, DSPACE_USER, DSPACE_PASSWORD
from tests.util import TestDataimport

session = None


def dspace_item_exists(output: str) -> bool:
    """
    Utility method to verify item has been created in the DSpace server.
    """
    match = re.search(r'DSPACE ITEM CREATED:\s(.*)$', output)

    if not match:
        session.close()
        return False

    resp = session.get(DSPACE_API + '/core/items/' + match.groups()[0])
    session.close()

    return resp.status_code == 200


class MockTarget(Target):
    def prepare(self):
        """
        This method gets the data from the product and writes to a json file.
        """
        self.log("Preparing products for '{x}'".format(x=self.id))
        products = self.get_products()

        data_feeds = []
        for product in products:
            if product.provides_format(JSONFeed):
                data_feeds.append(product.get_format(JSONFeed))

        with self.file_manager.output_file('load.json') as o:
            for d in data_feeds[0].entries():
                o.write(json.dumps(d))

    def load(self):
        """
        This method prepars a record for upload to a DSpace => 7 instance containing
        a dc.description value taken from the original loaded data.
        """
        self.log("Loading products for '{x}'".format(x=self.id))
        with self.file_manager.input_file('load.json') as o:
            data = json.loads(o.read())

        data = ", ".join([str(i) for i in data['numbers'][0]])

        # Add the even numbers to a metadata property
        DSPACE_ITEM_TEMPLATE['metadata']['dc.description'] = [
            {
                "value": data,
                "language": "en",
                "authority": None,
                "confidence": -1
            }
        ]

        with self.file_manager.output_file('dspace.json') as o:
            o.write(json.dumps(DSPACE_ITEM_TEMPLATE))


class MockDSpaceTarget(Target):
    def prepare(self):
        """
        This method gets the data from the product and writes to a json file.
        """
        self.log("Preparing products for '{x}'".format(x=self.id))
        products = self.get_products()

        data_feeds = []
        for product in products:
            if product.provides_format(JSONFeed):
                data_feeds.append(product.get_format(JSONFeed))

        with self.file_manager.output_file('load.json') as o:
            for d in data_feeds[0].entries():
                o.write(json.dumps(d))

    def load(self):
        """
        This method creates a record in the public DSpace 7.6 test instance containing
        a dc.description value taken from the original loaded data.

        It selects arbitrarily collection number #5 on the list of collections to be the
        owning collection of the uploaded record.
        """
        self.log("Loading products for '{x}'".format(x=self.id))
        with self.file_manager.input_file('load.json') as o:
            data = json.loads(o.read())

        data = ", ".join([str(i) for i in data['numbers'][0]])

        # Add the even numbers to a metadata property
        DSPACE_ITEM_TEMPLATE['metadata']['dc.description'] = [
            {
                "value": data,
                "language": "en",
                "authority": None,
                "confidence": -1
            }
        ]

        # We need a session to authenticate properly with the REST API
        global session
        session = requests.Session()

        # In our first request we get a list of collections and establish the CSRF
        # token we need in order to do POSTs or updates
        # see https://github.com/DSpace/RestContract/blob/main/csrf-tokens.md
        resp = session.get(DSPACE_API + '/core/collections')

        # The first 20 collections currently on the test server
        collection = json.loads(resp.text)['_embedded']['collections']

        # We get and set the CSRF token
        csrf_token = resp.headers.get('DSPACE-XSRF-TOKEN')
        session.headers.update({'X-XSRF-Token': csrf_token})
        session.cookies.update({'DSPACE-XSRF-Token': csrf_token})

        # Authenticate
        resp = session.post(DSPACE_API + '/authn/login',
                            data={'user': DSPACE_USER, 'password': DSPACE_PASSWORD})

        authorization = resp.headers.get('Authorization')

        # As we've done a post we need to update our CSRF token
        csrf_token = resp.headers.get('DSPACE-XSRF-TOKEN')

        session.cookies.update({'DSPACE-XSRF-Token': csrf_token})
        session.headers.update({'X-XSRF-Token': csrf_token,
                                'Authorization': authorization,
                                'Content-type': 'application/json'})

        # Upload record
        resp = session.post(DSPACE_API + '/core/items', json=DSPACE_ITEM_TEMPLATE,
                            params={'owningCollection': collection[4]['id']})

        if 'id' in resp.json():
            csrf_token = resp.headers.get('DSPACE-XSRF-TOKEN')
            session.headers.update({'X-XSRF-Token': csrf_token})
            session.cookies.update({'DSPACE-XSRF-Token': csrf_token})

            print('DSPACE ITEM CREATED: ' + resp.json()['id'])
        else:
            sys.exit(1)


class TestTarget(TestDataimport):
    mode = 'mocktarget'

    def test_prepare(self):
        """
        Test only the prepare stage
        """
        args = ['load', 'mocktarget', '-s', 'prepare', '-o', '-c', 'tests.mock_settings']
        result = self.runner.invoke(entry_point, args)

        self.assertEqual(result.exit_code, 0)
        self.assertIsFile('load.json')
        self.assertRegex(result.output, r"Preparing products for 'mocktarget'")

    def test_all(self):
        """
        Test the whole pipeline
        """
        result = self.runner.invoke(entry_point, ['load', 'mocktarget', '-c', 'tests.mock_settings'])

        self.assertEqual(result.exit_code, 0)
        self.assertIsFile('dspace.json')
        self.assertRegex(result.output, r"Loading products for 'mocktarget'")


'''class TestDSpaceTarget(TestDataimport):
    mode = 'mockdspacetarget'

    def test_all_dspace(self):
        """
        Test the whole pipeline and upload to DSpace.
        """
        result = self.runner.invoke(entry_point, ['load', 'mockdspacetarget', '-c', 'tests.mock_settings'])

        self.assertEqual(result.exit_code, 0)
        self.assertTrue(dspace_item_exists(result.output))
        self.assertRegex(result.output, r"Loading products for 'mockdspacetarget'")'''
