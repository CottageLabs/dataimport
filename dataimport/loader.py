from dataimport.file_manager import FileManager
from dataimport import logger

from datetime import datetime, timedelta
from dataimport.assembler import Assembler
from dataimport.target_factory import TargetFactory


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
        self.log("Assembling data for products: {x}".format(x=",".join([p.id for p in products])))
        assembler = Assembler(self.config)
        assembler.assemble(products, force_update=force_update)

    def prepare(self, products):
        tf = TargetFactory(self.config)
        for product in products:
            targets = tf.get_targets(product)

            for target in targets:
                self.log("Preparing target '{x}' for product '{y}'".format(x=target.id, y=product.id))
                target.file_manager.fresh()
                target.prepare()
                target.cleanup()

    def loads(self, products):
        tf = TargetFactory(self.config)
        for product in products:
            targets = tf.get_targets(product)

            for target in targets:
                self.log("Loading target '{x}' for product '{y}'".format(x=target.id, y=product.id))
                target.file_manager.current(make_fresh=True)
                target.load()
                target.cleanup()
