from dataimport.file_manager import FileManager
from dataimport import logger


class Target(object):
    def __init__(self, config, id, product):
        self.id = product.id + "__" + id
        self.config = config
        self.file_manager = FileManager(config, self.id)

    def log(self, msg):
        logger.log(msg, self.id.upper())

    def prepare(self):
        raise NotImplementedError()

    def load(self):
        raise NotImplementedError()