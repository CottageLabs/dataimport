from dataimport.file_manager import FileManager
from dataimport import logger

from datetime import datetime, timedelta


class Assembler(object):
    ASSEMBLE_PIPELINE = ["gather", "analyse", "assemble"]

    def __init__(self, config):
        self.config = config

    def log(self, msg):
        logger.log(msg, "ASSEMBLER")

    def assemble(self, products, force_update=False, stages=False):
        if not stages:
            stages = self.ASSEMBLE_PIPELINE

        for product in products:
            for stage in stages:
                if stage == "gather":
                    self.gather(product, force_update)
                elif stage == "analyse":
                    self.analyse(product)
                elif stage == "assemble":
                    self.assembly(product)

    def gather(self, product, force_update=False):
        product.gather(force_update)

    def analyse(self, product):
        product.file_manager.fresh()
        product.analyse()
        product.cleanup()

    def assembly(self, product):
        product.file_manager.current()
        product.assemble()
        product.cleanup()