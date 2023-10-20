from datetime import date


def _get_new_creator(publisher: dict) -> dict:
    return {
        "person_or_org": {
            "name": publisher['name'],
            "type": "organizational",
            "identifiers": [
                {
                    "scheme": publisher['type'],
                    "identifier": publisher['id']
                }
            ]
        }
    }

'''
def _get_identifiers(full_record: dict) -> List[dict]:
    ids = [
        {
            "identifier": full_record['url'],
            "scheme": "url"
        }
    ]
    print('here')
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
    print(dl_urls)
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
'''


def get_alt_ids(full_record: dict):
    dl_urls = []
    for distrib in full_record.get('datasets', []):
        distrib['date_added'] = date.today().isoformat()
        dl_urls.append(distrib)

    return dl_urls


def get_invenio_record(json_obj: dict) -> dict:
    invenio_record = {
        "access": {
            "record": "public",
            "files": "restricted"
        }, "files": {
            "enabled": False
        },
        "metadata": {
            "resource_type": {"id": "dataset"}
        }
    }

    metadata = invenio_record['metadata']

    # Set creators
    # TODO safe to assume author is publisher?
    metadata['creators'] = [_get_new_creator(json_obj['publisher'])]

    # Set name, description and publication date
    metadata['title'] = json_obj['name']

    if json_obj['description']:
        metadata['description'] = json_obj['description']

    if 'locations' in json_obj:
        metadata['locations'] = {'features': [json_obj['locations']]}

    if 'languages' in json_obj:
        metadata['languages'] = json_obj['languages']

    metadata['publisher'] = json_obj.get('publisher').get('name')

    if 'published' in json_obj:
        metadata['publication_date'] = json_obj.get('published')

    # Set identifiers
    metadata['identifiers'] = [
        {
            "identifier": json_obj['url'],
            "scheme": "url"
        }
    ]

    invenio_record['custom_fields'] = {'datasets': get_alt_ids(json_obj)}

    return invenio_record