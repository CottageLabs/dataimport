from dataimport.lib.paths import rel2abs
import os

DATASOURCES = {
    "doaj": "dataimport.datasources.doaj.DOAJ"
}

ASSEMBLERS = {
    "jac": "dataimport.datasources.jac.JAC"
}

ASSEMBLER_SOURCES = {
    "jac": ["doaj"]
}

DATABASES = rel2abs(__file__, "..", "databases")
RESOURCES = rel2abs(__file__, "..", "resources")

DIR_DATE_FORMAT = "%Y-%m-%d_%H%M"

RESOLVER_MAX_AGE = {
    "doaj": 60 * 60 * 24 * 7
}

STORE_SCOPES = {
    "doaj": os.path.join(DATABASES, "doaj")
}

STORE_KEEP_HISORIC = {
    "doaj": 3
}




DOAJ_PUBLIC_DATA_DUMP = "https://doaj.org/public-data-dump/journal"
DOAJ_PUBLIC_DATA_DUMP_KEYFILE = "/home/richard/Code/External/journalcheckertool/Importer/keyfiles/doaj_public_data_dump.txt"

