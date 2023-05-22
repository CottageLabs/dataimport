from dataimport.target import Target
from dataimport.formats.json_feed import JSONFeed
from dataimport.format import FormatNotSupported

import uuid
import esprit
from datetime import datetime
import requests
import json
import re

class ES17(Target):

    def prepare(self):
        if not self.product.provides_format(JSONFeed):
            raise FormatNotSupported(JSONFeed)

        feed = self.product.get_format(JSONFeed)
        bulkfile = self._bulkfile_name()

        with self.file_manager.output_file(bulkfile) as o:
            for d in feed.entries():
                if "id" not in d:
                    d["id"] = uuid.uuid4().hex
                bulklines = esprit.raw.to_bulk_single_rec(d)
                o.write(bulklines)

    def load(self):
        alias = self._get_alias_name()
        timestamped_index_name = alias + datetime.strftime(datetime.utcnow(), self.config.ES17_INDEX_SUFFIX_DATE_FORMAT)

        conn = esprit.raw.Connection(self.config.ES17_HOST, timestamped_index_name)
        mapping = {self.product.id: self.config.ES17_DEFAULT_MAPPING}

        self.log("loading into index")

        if not esprit.raw.type_exists(conn, self.product.id, es_version="1.7.5"):
            r = esprit.raw.put_mapping(conn, self.product.id, mapping, es_version="1.7.5")
            self.log("Creating ES Type + Mapping in index {0}; status: {1}".format(conn.index, r.status_code))
        else:
            self.log("ES Type + Mapping already exists in index {0}".format(conn.index))

        bulkfile_name = self._bulkfile_name()
        bulkfile = self.file_manager.file_path(bulkfile_name)

        self.log("bulk loading from {x}".format(x=bulkfile))
        esprit.tasks.bulk_load(conn, self.product.id, bulkfile)

        old_idx = None
        aliases = self._list_aliases(conn)
        for idx, a in aliases.items():
            if alias in a.get("aliases", {}):
                old_idx = idx
                break

        if old_idx is None:
            self.log("creating new alias {x} for {y}".format(x=alias, y=conn.index))
            esprit.tasks.create_alias(conn, alias)
        else:
            old_conn = esprit.raw.Connection(conn.host, old_idx, port=conn.port)
            self.log("repointing existing alias {x} from {y} to {z}".format(x=alias, y=old_conn.index, z=conn.index))
            esprit.tasks.repoint_alias(old_conn, conn, alias)

    def cleanup(self):
        super(ES17, self).cleanup()

        conn = esprit.raw.Connection(self.config.ES17_HOST, "")
        aliases = self._list_aliases(conn)
        alias = self._get_alias_name()

        removal_candidates = [idx for idx in aliases.keys() if re.match(rf'^{alias}\d+$', idx)]
        if len(removal_candidates) < self.config.ES17_KEEP_OLD_INDICES:
            self.log("less than {x} old indices, none removed".format(x=self.config.ES17_KEEP_OLD_INDICES))
            return

        removal_candidates.sort(reverse=True)
        removes = removal_candidates[self.config.ES17_KEEP_OLD_INDICES:]
        for r in removes:
            self.log("removing old index {x}".format(x=r))
            conn = esprit.raw.Connection(conn.host, r, port=conn.port)
            esprit.raw.delete(conn)

    def _bulkfile_name(self):
        return self.product.id + "__" + self.id + ".bulk"

    def _list_aliases(self, conn=None):
        if conn is None:
            conn = esprit.raw.Connection(self.config.ES17_HOST, "")
        resp = requests.get(conn.host + ":" + conn.port + "/_aliases")
        aliases = json.loads(resp.text)
        return aliases

    def _get_alias_name(self):
        index_suffix = self.config.ES17_INDEX_SUFFIX
        if index_suffix and not index_suffix.startswith('_'):
            index_suffix = "_" + index_suffix

        index_prefix = self.config.ES17_INDEX_PREFIX
        if index_prefix and not index_prefix.endswith('_'):
            index_prefix = index_prefix + "_"

        alias = index_prefix + self.product.id + index_suffix
        return alias