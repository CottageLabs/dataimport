from dataimport.lib.paths import rel2abs
import os

DATABASES = rel2abs(__file__, "..", "databases")
RESOURCES = rel2abs(__file__, "..", "resources")
TARGET_DIRS = os.path.join(DATABASES, "targets")

DATASOURCES = {
    "mockdatasource": "tests.test_resolve.MockDatasource",
}

PRODUCTS = {
    "mockproduct": "tests.test_assemble.MockProduct"
}

TARGETS = {
    "mocktarget": "tests.test_load.MockTarget",
    "mockdspacetarget": "tests.test_load.MockDSpaceTarget"
}

PRODUCT_SOURCES = {
    "mockproduct": ["mockdatasource"]
}

TARGET_PRODUCTS = {
    "mocktarget": ["mockproduct"],
    "mockdspacetarget": ['mockproduct']
}

DIR_DATE_FORMAT = "%Y-%m-%d_%H%M"

RESOLVER_MAX_AGE = {
    "mockdatasource": 60,  # seconds, nominal to avoid regular repeated copies in a short period
    "mockproduct": 60,
    "mocktarget": 60,
}

STORE_SCOPES = {
    "mockdatasource": os.path.join(DATABASES, "datasources", "mockdatasource"),
    "mockproduct": os.path.join(DATABASES, "products", "mockproduct"),
    "mocktarget": os.path.join(DATABASES, "targets", "mocktarget"),
    "mockdspacetarget": os.path.join(DATABASES, "targets", "mocktarget")
}

STORE_KEEP_HISTORIC = {
    "mockdatasource": 3,
    "mockproduct": 3,
    "mocktarget": 3,
    "mockdspacetarget": 3,
}

##########################################

# MONGO_SOURCE_FILE = os.path.join(RESOURCES, "test_package.zip")

DSPACE_API = 'https://sandbox.dspace.org/server/api'
DSPACE_USER = 'dspacedemo+admin@gmail.com'
DSPACE_PASSWORD = 'dspace'
