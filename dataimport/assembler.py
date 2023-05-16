from dataimport.file_manager import FileManager
from dataimport import logger
from dataimport.resolver import Resolver

class Assembler(object):
    def __init__(self, config, id):
        self.id = id
        self.config = config
        self.file_manager = FileManager(config, self.id)

    def log(self, msg):
        logger.log(msg, self.id.upper())

    def gather(self):
        sources = self.config.ASSEMBLER_SOURCES.get(self.id, [])
        self.log('Gathering data for from sources: {x}'.format(x=",".join(sources)))
        resolver = Resolver(self.config)
        resolver.resolve(sources)

    def analyse(self):
        raise NotImplementedError()

    def assemble(self):
        raise NotImplementedError()