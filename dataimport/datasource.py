from dataimport.file_manager import FileManager
from dataimport import logger


class Datasource:

    def __init__(self, config, id):
        self.id = id
        self.config = config
        self.file_manager = FileManager(config, self.id)

    def log(self, msg):
        logger.log(msg, self.id.upper())

    def fetch(self):
        raise NotImplementedError()

    def analyse(self):
        raise NotImplementedError()

    def provides_analysis(self, analysis_class):
        return False

    def analysis(self, analysis_class):
        raise NotImplementedError()

