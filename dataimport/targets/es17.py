from dataimport.target import Target


class ES17(Target):
    def prepare(self):
        with open(infile, "r") as f, open(bulkfile, "w") as o:
            line = f.readline()
            while line:
                d = json.loads(line)
                if "id" not in d:
                    d["id"] = uuid.uuid4().hex
                bulklines = esprit.raw.to_bulk_single_rec(d)
                o.write(bulklines)
                line = f.readline()

    def load(self):
        pass