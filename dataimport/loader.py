from dataimport.file_manager import FileManager
from dataimport import logger

from datetime import datetime, timedelta
from dataimport.assembler import Assembler

class Loader(object):
    LOAD_PIPELINE = ["assemble", "prepare", "load"]

    def __init__(self, config):
        self.config = config

    def log(self, msg):
        logger.log(msg, "LOADER")

    def load(self, products, force_update=False, stages=False):
        if not stages:
            stages = self.LOAD_PIPELINE

        for stage in stages:
            if stage == "assemble":
                self.assemble(products, force_update)
            elif stage == "prepare":
                self.prepare(products)
            elif stage == "load":
                self.loads(products)

    def assemble(self, products, force_update=False):
        assembler = Assembler(self.config)
        assembler.assemble(products, force_update=force_update)

    def prepare(self, product):
        product.file_manager.fresh()
        product.analyse()
        product.file_manager.cleanup()

    def loads(self, product):
        product.file_manager.current()
        product.assemble()
        product.file_manager.cleanup()