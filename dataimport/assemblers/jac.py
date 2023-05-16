import csv, json, itertools, os, re, shutil
from datetime import datetime

from dataimport.product import Product
from dataimport.datasource_factory import DatasourceFactory
from dataimport.analyses.coincident_issns import CoincidentISSNs
from dataimport.analyses.titles import Titles
from dataimport.analyses.publishers import Publishers

from dataimport.lib import manipulators, indexing


class JAC(Product):

    def analyse(self):
        self.log("Analysing data for journal autocomplete")

        sources = self.config.PRODUCT_SOURCES.get(self.id, [])
        dsf = DatasourceFactory(self.config)

        issns = []
        titles = []
        pubs = []
        for s in sources:
            ds = dsf.get_datasource(s)

            if ds.provides_analysis(CoincidentISSNs):
                issns.append(ds.analysis(CoincidentISSNs))

            if ds.provides_analysis(Titles):
                titles.append(ds.analysis(Titles))

            if ds.provides_analysis(Publishers):
                pubs.append(ds.analysis(Publishers))

        with self.file_manager.output_file("issn_clusters.csv") as handle:
            manipulators.issn_clusters(issns, handle)

        with self.file_manager.output_file("titles.csv") as handle:
            titlerows = manipulators.cat_and_dedupe(titles)
            writer = csv.writer(handle)
            writer.writerows(titlerows)

        with self.file_manager.output_file("pubs.csv") as handle:
            pubrows = manipulators.cat_and_dedupe(pubs)
            writer = csv.writer(handle)
            writer.writerows(pubrows)

        self.log("analysed data written")

    def assemble(self):
        self.log("Preparing journal autocomplete data")

        preference_order = self.config.JAC_PREF_ORDER

        with self.file_manager.input_file("titles.csv") as f:
            reader = csv.reader(f)
            titlerows = [row for row in reader]

        with self.file_manager.input_file("pubs.csv") as f:
            reader = csv.reader(f)
            pubrows = [row for row in reader]

        titles = manipulators.cluster_to_dict(titlerows, 3)
        publishers = manipulators.cluster_to_dict(pubrows, 2)

        with self.file_manager.input_file("issn_clusters.csv") as f, self.file_manager.output_file("jac.json") as o:
            reader = csv.reader(f)
            for vissns in reader:
                record = {"issns": vissns}
                main, alts = self._get_titles(vissns, titles, preference_order)
                if main is not None:
                    record["title"] = main
                else:
                    record["title"] = ""
                if len(alts) > 0:
                    record["alts"] = alts

                publisher = self._get_publisher(vissns, publishers, preference_order)
                if publisher is not None:
                    record["publisher"] = publisher

                self._index(record)

                o.write(json.dumps(record) + "\n")

        self.log("Journal Autocomplete data assembled")

    def _get_titles(self, issns, titles, preference_order):
        mains = []
        alts = []

        for issn in issns:
            candidates = titles.get(issn, [])
            for c in candidates:
                if c[1] == "main":
                    mains.append((c[0].strip(), c[2].strip()))
                elif c[1] == "alt":
                    alts.append((c[0].strip(), c[2].strip()))

        if len(mains) == 0:
            # if there are no titles, return an empty state
            if len(alts) == 0:
                return None, alts
            # otherwise return the best title from the alternates
            main = manipulators.extract_preferred(alts, preference_order)
            return main, [x for x in list(set([a[0] for a in alts])) if x != main]

        if len(mains) == 1:
            return mains[0][0], [x for x in list(set([a[0] for a in alts])) if x != mains[0][0]]

        main = manipulators.extract_preferred(mains, preference_order)
        return main, [x for x in list(set([m[0] for m in mains] + [a[0] for a in alts])) if x != main]

    def _get_publisher(self, issns, publishers, preference_order):
        pubs = []

        for issn in issns:
            candidates = publishers.get(issn, [])
            for c in candidates:
                pubs.append((c[0].strip(), c[1].strip()))

        if len(pubs) == 0:
            return None

        pub = manipulators.extract_preferred(pubs, preference_order)
        return pub

    def _index(self, record):
        idx = {}

        idx["issns"] = [issn.lower() for issn in record["issns"]]
        idx["issns"] += [issn.replace("-", "") for issn in idx["issns"]]

        idx["title"] = indexing.title_variants(record.get("title", ""))
        idx["alts"] = idx["title"]  # This helps with getting better index scores because alts also contains the main titles

        if "alts" in record:
            for alt in record["alts"]:
                idx["alts"] += indexing.title_variants(alt)
            idx["alts"] = list(set(idx["alts"]))

        record["index"] = idx

