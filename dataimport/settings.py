from dataimport.lib.paths import rel2abs
import os

DATABASES = rel2abs(__file__, "..", "databases")
RESOURCES = rel2abs(__file__, "..", "resources")
TARGET_DIRS = os.path.join(DATABASES, "targets")

DATASOURCES = {
    "doaj": "dataimport.datasources.doaj.DOAJ",
    "zenodo": "dataimport.datasources.zenodo.Zenodo"
}

PRODUCTS = {
    "jac": "dataimport.assemblers.jac.JAC"
}

TARGETS = {
    "es17": "dataimport.targets.es17.ES17",
    "zenodo": "dataimport.targets.zenodo.Zenodo"
}

PRODUCT_SOURCES = {
    "jac": ["doaj"]
}

PRODUCT_TARGETS = {
    "jac": [{"id": "es17", "dir": os.path.join(TARGET_DIRS, "jac__es17")}]
}

DIR_DATE_FORMAT = "%Y-%m-%d_%H%M"

RESOLVER_MAX_AGE = {
    "doaj": 60 * 60 * 24 * 7
}

STORE_SCOPES = {
    "doaj": os.path.join(DATABASES, "datasources", "doaj"),
    "jac": os.path.join(DATABASES, "products", "jac")
}

STORE_KEEP_HISORIC = {
    "doaj": 3,
    "jac": 5,
    "es17": 5
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
