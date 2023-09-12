from typing import List


def _get_new_creator(author: str, url: str) -> dict:
    return {
        "person_or_org": {
            "name": author,
            "type": "organizational",
            "identifiers": [
                #{
                #    "scheme": "url",
                #    "identifier": url
                #}
                {
                    "scheme": "ror",
                    "identifier": "https://ror.org/021fhft25"
                }
            ]
        }
    }


def _get_identifiers(full_record: dict) -> List[dict]:
    ids = [
        {
            "identifier": full_record['url'],
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
            "identifier": full_record['datasetDocumentation'],
            "scheme": "url_doc"
        })

    return ids


def get_alt_ids(full_record: dict):
    dl_urls = []
    for distrib in full_record.get('distribution', []):
        if distrib.get('@type') and distrib.get('@type') == 'DataDownload':

            dl_urls.append({
                "identifier": distrib['contentUrl'],
                "scheme": "url",
                "relation_type": {
                    "id": "issupplementedby",
                },
                "resource_type": {
                    "id": "dataset"
                }
            })

    return dl_urls


def get_invenio_record(config, ons_json: dict) -> dict:
    invenio_record = {
        "access": {
            "record": "public",
            "files": "restricted"
        }, "files": {
            "enabled": False
        },
        "metadata": {
            # "subjects": [{'subject': kwd} for kwd in list(set(incoming_json.get('keywords', [])))],
            "languages": [{"id": "eng"}],
            "subjects": [],
            'locations': [{"features": [{"place": 'United Kingdom',
                                         "identifiers": [{'scheme': 'iso3166', 'identifier': 'UK'}]
                                         }]
                           }]
        }
    }

    metadata = invenio_record['metadata']

    # Set creators
    metadata['creators'] = [_get_new_creator(ons_json['publisher']['name'], config.ONS_URL)]

    # Set resource type
    metadata['resource_type'] = {"id": ons_json['@type'].lower()}

    # Set name, description and publication date
    metadata['title'] = ons_json['name']
    metadata['description'] = ons_json['description']
    metadata['publisher'] = ons_json.get('publisher').get('name')
    metadata['publication_date'] = ons_json.get('datePublished', '0001')[:10]

    # Set identifiers
    metadata['identifiers'] = [
        {
            "identifier": ons_json['url'],
            "scheme": "url"
        }
    ]

    metadata['related_identifiers'] = get_alt_ids(ons_json)

    return invenio_record
