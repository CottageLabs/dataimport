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


def resolve(targets, stage=None, full_pipeline=True):
    if stage is None:
        stage = Resolver.RESOLVE_PIPELINE[-1]

    stages = [stage]

    if full_pipeline:
        final = Resolver.RESOLVE_PIPELINE.index(stage)
        stages = Resolver.RESOLVE_PIPELINE[0:final + 1]

    dsf = DatasourceFactory(settings)

    if targets[0] == "_all":
        datasources = dsf.get_all_datasources()
    else:
        datasources = [dsf.get_datasource(t) for t in targets]

    resolver = Resolver(settings)
    resolver.resolve(datasources, force_update=True, stages=stages)


def assemble(targets, stage=None, full_pipeline=True):
    if stage is None:
        stage = Assembler.ASSEMBLE_PIPELINE[-1]

    stages = [stage]

    if full_pipeline:
        final = Assembler.ASSEMBLE_PIPELINE.index(stage)
        stages = Assembler.ASSEMBLE_PIPELINE[0:final + 1]

    pf = ProductFactory(settings)

    if targets[0] == "_all":
        products = pf.get_all_products()
    else:
        products = [pf.get_product(target) for target in targets]

    assembler = Assembler(settings)
    assembler.assemble(products, force_update=False, stages=stages)


def load(targets, stage=None, full_pipeline=True):
    if stage is None:
        stage = Loader.LOAD_PIPELINE[-1]

    stages = [stage]

    if full_pipeline:
        final = Loader.LOAD_PIPELINE.index(stage)
        stages = Loader.LOAD_PIPELINE[0:final + 1]

    pf = ProductFactory(settings)

    if targets[0] == "_all":
        products = pf.get_all_products()
    else:
        products = [pf.get_product(target) for target in targets]


    if full_pipeline:
        index(targets)

    for t in targets:
        load_type = settings.INDEX_LOADERS[t]
        if load_type == "es":
            loader.index_latest_with_alias(t, settings.ES_INDEX_SUFFIX)
        elif load_type == "file":
            loader.load_to_file(t)



MODE_MAP = {
    "resolve": resolve,
    "assemble": assemble,
    "load": load
}


if __name__ == "__main__":
    entry_point()