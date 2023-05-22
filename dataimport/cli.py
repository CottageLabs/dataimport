import click

from dataimport.datasource_factory import DatasourceFactory
from dataimport.product_factory import ProductFactory
from dataimport.target_factory import TargetFactory

# FIXME: this needs to be replaced with proper config management
from dataimport import settings

from dataimport.resolver import Resolver
from dataimport.assembler import Assembler
from dataimport.loader import Loader


@click.command()
@click.argument("mode")
@click.argument("targets", nargs=-1)
@click.option("-s", "--stage")
@click.option("-o", "--only", "full_pipeline", flag_value=False)
@click.option("-a", "--all", "full_pipeline", flag_value=True, default=True)
def entry_point(mode, targets, stage=None, full_pipeline=True):
    run(mode, targets, stage, full_pipeline)


def run(mode:str, targets:tuple, stage:str=None, full_pipeline:bool=True):
    processor = MODE_MAP.get(mode)
    if not processor:
        return

    processor(targets, stage, full_pipeline)


def resolve(datasource_names, stage=None, full_pipeline=True):
    if stage is None:
        stage = Resolver.RESOLVE_PIPELINE[-1]

    stages = [stage]

    if full_pipeline:
        final = Resolver.RESOLVE_PIPELINE.index(stage)
        stages = Resolver.RESOLVE_PIPELINE[0:final + 1]

    dsf = DatasourceFactory(settings)

    if datasource_names[0] == "_all":
        datasources = dsf.get_all_datasources()
    else:
        datasources = [dsf.get_datasource(t) for t in datasource_names]

    resolver = Resolver(settings)
    resolver.resolve(datasources, force_update=True, stages=stages)


def assemble(product_names, stage=None, full_pipeline=True):
    if stage is None:
        stage = Assembler.ASSEMBLE_PIPELINE[-1]

    stages = [stage]

    if full_pipeline:
        final = Assembler.ASSEMBLE_PIPELINE.index(stage)
        stages = Assembler.ASSEMBLE_PIPELINE[0:final + 1]

    pf = ProductFactory(settings)

    if product_names[0] == "_all":
        products = pf.get_all_products()
    else:
        products = [pf.get_product(pn) for pn in product_names]

    assembler = Assembler(settings)
    assembler.assemble(products, force_update=False, stages=stages)


def load(product_names, stage=None, full_pipeline=True):
    if stage is None:
        stage = Loader.LOAD_PIPELINE[-1]

    stages = [stage]

    if full_pipeline:
        final = Loader.LOAD_PIPELINE.index(stage)
        stages = Loader.LOAD_PIPELINE[0:final + 1]

    pf = ProductFactory(settings)

    if product_names[0] == "_all":
        products = pf.get_all_products()
    else:
        products = [pf.get_product(pn) for pn in product_names]

    loader = Loader(settings)
    loader.load(products, force_update=False, stages=stages)


MODE_MAP = {
    "resolve": resolve,
    "assemble": assemble,
    "load": load
}


if __name__ == "__main__":
    entry_point()