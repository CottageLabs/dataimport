from dataimport.lib.paths import rel2abs
import os

DATASOURCES = {
    "doaj": "dataimport.datasources.doaj.DOAJ"
}

PRODUCTS = {
    "jac": "dataimport.assemblers.jac.JAC"
}

TARGETS = {
    "es17": "dataimport.targets.es17.ES17"
}

PRODUCT_SOURCES = {
    "jac": ["doaj"]
}

DATABASES = rel2abs(__file__, "..", "databases")
RESOURCES = rel2abs(__file__, "..", "resources")

DIR_DATE_FORMAT = "%Y-%m-%d_%H%M"

RESOLVER_MAX_AGE = {
    "doaj": 60 * 60 * 24 * 7
}

STORE_SCOPES = {
    "doaj": os.path.join(DATABASES, "datasources", "doaj"),
    "jac": os.path.join(DATABASES, "products", "jac"),
    "es17": os.path.join(DATABASES, "targets")
}

STORE_KEEP_HISORIC = {
    "doaj": 3,
    "jac": 5
}




DOAJ_PUBLIC_DATA_DUMP = "https://doaj.org/public-data-dump/journal"
DOAJ_PUBLIC_DATA_DUMP_KEYFILE = "/home/richard/Code/External/journalcheckertool/Importer/keyfiles/doaj_public_data_dump.txt"


JAC_PREF_ORDER = ["doaj"]