from dataimport.lib import plugin


class ProductFactory(object):

    def __init__(self, config):
        self.config = config

    def get_product(self, name):
        ref = self.config.PRODUCTS.get(name)
        klazz = plugin.load_class(ref)
        return klazz(self.config, name)

    def get_all_products(self):
        all = []
        for name, ref in self.config.PRODUCTS.items():
            all.append(self.get_product(name))
        return all

    def get_all_product_names(self):
        return list(self.config.PRODUCTS.keys())