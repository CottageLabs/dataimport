from dataimport.file_manager import FileManager
from dataimport import logger


class Target(object):
    def __init__(self, config, id, product, target_product_dir):
        self.id = id
        self.product = product
        self.config = config
        self.file_manager = FileManager(config, self.id, base_dir=target_product_dir)

    def log(self, msg):
        logger.log(msg, self.id.upper())

    def prepare(self):
        raise NotImplementedError()

    def load(self):
        raise NotImplementedError()

    def cleanup(self):
        self.file_manager.cleanup()