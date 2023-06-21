from dataimport.file_manager import FileManager
from dataimport import logger
from dataimport.product_factory import ProductFactory
from dataimport.assembler import Assembler


class Target(object):
    def __init__(self, config, id):
        self.id = id
        self.config = config
        self.file_manager = FileManager(config, self.id)

    def log(self, msg):
        logger.log(msg, self.id.upper())

    def get_products(self):
        product_ids = self.config.TARGET_PRODUCTS.get(self.id, [])
        pf = ProductFactory(self.config)
        return [pf.get_product(p) for p in product_ids]

    def assemble(self, force_update=False):
        products = self.get_products()
        self.log('Assembling products: {x}'.format(x=",".join([p.id for p in products])))
        assembler = Assembler(self.config)
        assembler.assemble(products, force_update=force_update)

    def prepare(self):
        raise NotImplementedError()

    def load(self):
        raise NotImplementedError()

    def cleanup(self):
        self.file_manager.cleanup()