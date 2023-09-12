import json
import re
from urllib.parse import urlparse

from dataimport.target import Target


def _get_new_creator(author: str, url: str):
    return {
        "person_or_org": {
            "name": author,
            "type": "organizational",
            "identifiers": [
                {
                    "scheme": "url",
                    "identifier": url
                }
            ]
        }
    }


def _list_or_str(field):
    """
    In the concatenated json, some fields are wrapped in lists, but in the individual records, they're not.
    This function gets the first string from a list, or returns the whole string if it's not a list
     """
    return field.pop() if type(field) is list else field


def _get_identifiers(full_record: dict):
    ids = [
        {
            "identifier": _list_or_str(full_record['url']),
            "scheme": "url"
        }
    ]

    # Pull out the download URLs into their own list so that we can enumerate through them
    dl_urls = []
    for distrib in full_record.get('distribution', []):
        if distrib.get('@type') and distrib.get('@type') == 'DataDownload':
            dl_urls.append(distrib)
            continue

        if distrib.get('distributionType').lower() == 'restful api' and distrib.get('contentUrl'):
            ids.append({
                "identifier": distrib['contentUrl'],
                "scheme": "url_api"
            })
        if distrib.get('distributionType').lower() == 'file download' and distrib.get('contentUrl'):
            dl_urls.append(distrib)

    # support multiple download URLs, url_file0..url_file5 (cap at 6)
    for ix, distrib in enumerate(dl_urls[:6]):
        ids.append({
            "identifier": distrib['contentUrl'],
            "scheme": f"url_file{ix}"
        })

    if len(full_record.get('datasetDocumentation', [])):
        ids.append({
            "identifier": _list_or_str(full_record['datasetDocumentation']),
            "scheme": "url_doc"
        })

    return ids


def _get_license(license_url: str):
    parsed_url = urlparse(license_url)

    # If this is a CC URL, we transform it to the license ID that Invenio recognises
    if parsed_url.hostname == 'creativecommons.org':
        try:
            # Extract the properties and version from the URL path
            extr = re.search(r'(?<=\/)([a-z-]+)\/([0-9.]+)', parsed_url.path)
            return {'id': f'cc-{extr.groups()[0]}-{extr.groups()[1]}'}
        except IndexError:
            print(f'CC license parse failed: {license_url}')
    elif license_url == "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/":
        return {"id": "OGL-UK-3.0"}

    # Fallback license declaration - label and URL
    return {
        "link": license_url,
        "title": {"en": "Terms and Conditions"},
        "description": {
            "en": "The data provider adopts the following terms and conditions to access and reuse datasets."}
    }


class ONS(Target):
    """ Upload to an Invenio API endpoint """

    def prepare(self):
        self.log('Preparing')
        # self.log(json.dumps(eui2invenio(old_json)))
        #self.log(self.file_manager.list_files('.json'))
        self.log(self.file_manager.list_ons_files('.json'))

        for f in self.file_manager.list_ons_files('.json'):
            with open('/home/jabbi/PycharmProjects/dataimport/databases/datasources/ons/2023-08-30_1321/' + f) as i:
                new_json = json.loads(i.read())

                invenio_record = {
                    "access": {
                        "record": "public",
                        "files": "restricted"
                    },
                    "files": {
                        "enabled": False
                    },
                    "metadata": {
                        "creators": [_get_new_creator(new_json['publisher']['name'], self.config.ONS_URL)],
                        "resource_type": {
                            "id": _list_or_str(new_json['@type']).lower()
                        },
                        "title": _list_or_str(new_json['name']),
                        "identifiers": _get_identifiers(new_json),
                        "description": _list_or_str(new_json['description']),
                        "publication_date": _list_or_str(new_json.get('datePublished', '0001')),
                        # "subjects": [{'subject': kwd} for kwd in list(set(incoming_json.get('keywords', [])))],
                        "languages": [{"id": "eng"}],
                        "subjects": [],
                        'publisher': _list_or_str(new_json.get('publisher').get('name')),
                        'locations': [{"features": [{"place": 'United Kingdom',
                                                     "identifiers": [{'scheme': 'iso3166', 'identifier': 'UK'}]
                                                     }]
                                       }]
                    }
                }
                self.log(invenio_record)

        return

        new_json = {}


        fmts = [d.get('encodingFormat') for d in new_json.get('distribution', [])]
        if fmts and any(fmts):
            invenio_record['metadata']['formats'] = fmts

        # Add license if present
        if new_json.get('license'):
            invenio_record['metadata']['rights'] = [_get_license(_list_or_str(new_json.get('license')))]
            #invenio_record['metadata']['rights'] = [{"id": "OGL-UK-3.0"}]

        self.log(json.dumps(invenio_record))

        '''with self.file_manager.output_file(bulkfile) as o:
            for d in feed.entries():
                if "id" not in d:
                    d["id"] = uuid.uuid4().hex
                bulklines = esprit.raw.to_bulk_single_rec(d)
                o.write(bulklines)'''

    def load(self):
        self.log('Loading')

    def cleanup(self):
        super(ONS, self).cleanup()
