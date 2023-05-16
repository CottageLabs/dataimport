from dataimport.datasource import Datasource
from dataimport.lib.secrets import get_secret

from dataimport.analyses.coincident_issns import CoincidentISSNs, CoincidentISSNsFromCSV
from dataimport.analyses.titles import Titles
from dataimport.analyses.publishers import Publishers

import requests
import tarfile, json, csv
from copy import deepcopy


class DOAJ(Datasource):
    def fetch(self):
        self.log("downloading latest data dump")

        url = self.config.DOAJ_PUBLIC_DATA_DUMP
        url += "?api_key=" + get_secret(self.config.DOAJ_PUBLIC_DATA_DUMP_KEYFILE)

        resp = requests.get(url)

        with self.file_manager.output_file("origin.tar.gz", mode="wb") as tarball:
            tarball.write(resp.content)

    def analyse(self):
        self._extract_doaj_data()
        self._coincident_issns()
        self._title_map()
        self._publisher_map()
        self._licence_map()

    def provides_analysis(self, analysis_class):
        return isinstance(analysis_class, CoincidentISSNs) or \
                isinstance(analysis_class, Titles) or \
                isinstance(analysis_class, Publishers)

    def analysis(self, analysis_class):
        if isinstance(analysis_class, CoincidentISSNs):
            return self._coincident_issns_analysis()
        elif isinstance(analysis_class, Titles):
            return self._titles_analysis()
        elif isinstance(analysis_class, Publishers):
            return self._publisher_analysis()

    def _extract_doaj_data(self):
        tarball = self.file_manager.file_path("origin.tar.gz")
        outfile = self.file_manager.file_path("origin.csv")

        self.log("extracting data dump {x} to {y}".format(x=tarball, y=outfile))

        tf = tarfile.open(tarball, "r:gz")
        with self.file_manager.output_file("origin.csv") as o:
            writer = csv.writer(o)

            while True:
                entry = tf.next()
                if entry is None:
                    break
                f = tf.extractfile(entry)
                j = json.loads(f.read())

                for journal in j:
                    eissn = journal.get("bibjson", {}).get("eissn", "")
                    pissn = journal.get("bibjson", {}).get("pissn", "")
                    title = journal.get("bibjson", {}).get("title", "")
                    alt = journal.get("bibjson", {}).get("alternative_title", "")
                    publisher = journal.get("bibjson", {}).get("publisher", {}).get("name", "")
                    licences = json.dumps(journal.get("bibjson", {}).get("license", []))
                    row = [eissn, pissn, title, alt, publisher, licences]

                    licences = journal.get("bibjson", {}).get("license", [])
                    if len(licences) == 0:
                        writer.writerow(row)

                    for i, l in enumerate(licences):
                        lrow = deepcopy(row)
                        lrow += [
                            str(i + 1) + "/" + str(len(licences)),
                            l.get("type")
                        ]
                        writer.writerow(lrow)

    def _coincident_issns(self):
        issn_pairs = []

        with self.file_manager.input_file("origin.csv") as doaj_file:
            reader = csv.reader(doaj_file)

            for row in reader:
                if row[0] and row[1]:
                    issn_pairs.append([row[0], row[1]])
                    issn_pairs.append([row[1], row[0]])
                elif row[0] and not row[1]:
                    issn_pairs.append([row[0], ""])
                elif not row[0] and row[1]:
                    issn_pairs.append([row[1], ""])

        issn_pairs.sort(key=lambda x: x[0])

        with self.file_manager.output_file("coincident_issns.csv") as outfile:
            writer = csv.writer(outfile)
            writer.writerows(issn_pairs)

    def _coincident_issns_analysis(self):
        path = self.file_manager.file_path("coincident_issns.csv")
        return CoincidentISSNsFromCSV(self.id, filepath=path)

    def _title_map(self):
        with self.file_manager.output_file("titles.csv") as outfile, \
                self.file_manager.input_file("origin.csv") as doaj_file:
            writer = csv.writer(outfile)
            reader = csv.reader(doaj_file)

            for row in reader:
                if row[0]:
                    if row[2]:
                        writer.writerow([row[0], row[2], "main"])
                    if row[3]:
                        writer.writerow([row[0], row[3], "alt"])
                if row[1]:
                    if row[2]:
                        writer.writerow([row[1], row[2], "main"])
                    if row[3]:
                        writer.writerow([row[1], row[3], "alt"])

    def _titles_analysis(self):
        pass

    def _publisher_map(self):
        with self.file_manager.output_file("publishers.csv") as outfile, \
                self.file_manager.input_file("origin.csv") as doaj_file:

            writer = csv.writer(outfile)
            reader = csv.reader(doaj_file)

            for row in reader:
                if row[0]:
                    if row[4]:
                        writer.writerow([row[0], row[4]])
                if row[1]:
                    if row[4]:
                        writer.writerow([row[1], row[4]])

    def _publisher_analysis(self):
        pass

    def _licence_map(self):
        with self.file_manager.output_file("licences.csv") as outfile, \
                self.file_manager.input_file("origin.csv") as doaj_file:

            writer = csv.writer(outfile)
            reader = csv.reader(doaj_file)

            for row in reader:
                if row[0]:
                    if row[5]:
                        writer.writerow([row[0], row[5]])
                if row[1]:
                    if row[5]:
                        writer.writerow([row[1], row[5]])