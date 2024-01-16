from dataimport.lib.paths import rel2abs
import os

DATABASES = rel2abs(__file__, "..", "databases")
RESOURCES = rel2abs(__file__, "..", "resources")
STATIC = rel2abs(__file__, "..", "static")
TARGET_DIRS = os.path.join(DATABASES, "targets")

DATASOURCES = {
    "doaj": "dataimport.datasources.doaj.DOAJ",
    "zenodo": "dataimport.datasources.zenodo.Zenodo",
    "ons": "dataimport.datasources.ons.ONS",
    "acled": "dataimport.datasources.acled.ACLED",
    "static": "dataimport.datasources.static.STATIC",
    "europa": "dataimport.datasources.europa.EUROPA",
    "cdc": "dataimport.datasources.cdc.CDC",
    "coronanet": "dataimport.datasources.coronanet.CORONANET"
}

PRODUCTS = {
    "jac": "dataimport.assemblers.jac.JAC",
    "datacite": "dataimport.products.datacite.Datacite"
}

TARGETS = {
    "es17": "dataimport.targets.es17.ES17",
    "invenio": "dataimport.targets.invenio.Invenio"
}

PRODUCT_SOURCES = {
    "jac": ["doaj"],
    "eui": ["ons"],
    "datacite": ["coronanet"]
}

PRODUCT_TARGETS = {
    "jac": [{"id": "es17", "dir": os.path.join(TARGET_DIRS, "jac__es17")}]
}


TARGET_PRODUCTS = {
    "invenio": ["datacite"]
}

DIR_DATE_FORMAT = "%Y-%m-%d_%H%M"
ORIGIN_SUFFIX = '-origin'

RESOLVER_MAX_AGE = {
    "doaj": 60 * 60 * 24 * 7
}

STORE_SCOPES = {
    "doaj": os.path.join(DATABASES, "datasources", "doaj"),
    "jac": os.path.join(DATABASES, "products", "jac"),
    "ons": os.path.join(DATABASES, "datasources", "ons"),
    "invenio": os.path.join(DATABASES, "targets", "invenio"),
    "datacite": os.path.join(DATABASES, "products", "datacite"),
    "acled": os.path.join(DATABASES, "datasources", "acled"),
    "static": os.path.join(DATABASES, "datasources", "static"),
    "europa": os.path.join(DATABASES, "datasources", "europa"),
    "cdc": os.path.join(DATABASES, "datasources", "cdc"),
    "coronanet": os.path.join(DATABASES, "datasources", "coronanet"),
}

STORE_KEEP_HISTORIC = {
    "doaj": 3,
    "jac": 5,
    "es17": 5,
    "ons": 1,
    "invenio": 1,
    "datacite": 1,
    "acled": 1,
    "europa": 1,
    "static": 1,
    "cdc": 1,
    "coronanet": 1
}


DOAJ_PUBLIC_DATA_DUMP = "https://doaj.org/public-data-dump/journal"
DOAJ_PUBLIC_DATA_DUMP_KEYFILE = "/home/richard/Code/External/journalcheckertool/Importer/keyfiles/doaj_public_data_dump.txt"

JAC_PREF_ORDER = ["doaj"]


ES17_HOST = "http://localhost:9200"
ES17_INDEX_PREFIX = 'jct'
ES17_INDEX_SUFFIX = 'dev'
ES17_INDEX_SUFFIX_DATE_FORMAT = "%Y%m%d%H%M%S"
ES17_KEEP_OLD_INDICES = 2
ES17_DEFAULT_MAPPING = {
    "dynamic_templates": [
        {
            "default": {
                "match": "*",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "multi_field",
                    "fields": {
                        "{name}": {"type": "{dynamic_type}", "index": "analyzed", "store": "no"},
                        "exact": {"type": "{dynamic_type}", "index": "not_analyzed", "store": "yes"}
                    }
                }
            }
        }
    ]
}

# ZENODO
ZENODO_URL = "https://zenodo.org/api/"
default_search = "records?size=20&communities=covid-19&type=dataset&all_versions=false&status=published&q=access_right:open"
excl_terms = [
    "SARS-CoV-2", "inhibitor", "cytokine", "vitro", "cell", "pneumonia", "scans", "genes", "cough", "Diabetes",
    "antibodies", "ARDS", "Lung", "Anthropometric", "proteins", "sequencing", "biopsies", "genome", "pulmonary"
]
ZENODO_SEARCH = default_search + " -" + " -".join(excl_terms)


# ONS

ONS_URL = 'https://www.ons.gov.uk'
ONS_SEARCH = '/peoplepopulationandcommunity/healthandsocialcare/conditionsanddiseases/datalist?sortBy=release_date&filter=datasets&size=1000'

INVENIO_API = "https://127.0.0.1:5000/"
INVENIO_TOKEN = "7kBuUAW9XMlsiCrMVAqaREHTUX4J8ZT7ZLibnFdTBGmuF1WlqKawBR3ENmIC"
INVENIO_COMMUNITY = '8222517f-947f-47df-8714-5dd3896f2c7c'

USER_AGENT = 'Covid Data Resource Aggregate Scraper v. 0.5'
GITHUB_TOKEN = 'token'

# ACLED

ACLED_URL = 'https://acleddata.com/analysis/covid-19-disorder-tracker/'


# EUROPA
# Going with the default limit of 10 because some records contains massive information
# leading to mbs for json per result file
EUROPA_URL = 'https://data.europa.eu/api/hub/search/search?filter=dataset&facets={%22keywords%22:[%22covid-19%22]}&limit=10'

# CDC
CDC_URL = 'https://data.cdc.gov/browse?limitTo=datasets&tags=covid-19&limit=500'

# Coronanet
CORONANET_URL = 'https://www.coronanet-project.org/index.html'
