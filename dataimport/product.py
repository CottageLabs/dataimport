from dataimport.file_manager import FileManager
from dataimport import logger
from dataimport.resolver import Resolver
from dataimport.datasource_factory import DatasourceFactory


class Product(object):
    def __init__(self, config, id):
        self.id = id
        self.config = config
        self.file_manager = FileManager(config, self.id)

    def log(self, msg):
        logger.log(msg, self.id.upper())

    def gather(self, force_update=False):
        sources = self.config.PRODUCT_SOURCES.get(self.id, [])
        self.log('Gathering data for from sources: {x}'.format(x=",".join(sources)))
        resolver = Resolver(self.config)
        dsf = DatasourceFactory(self.config)
        datasources = [dsf.get_datasource(s) for s in sources]
        resolver.resolve(datasources, force_update=force_update)

    def analyse(self):
        raise NotImplementedError()

    def assemble(self):
        raise NotImplementedError()