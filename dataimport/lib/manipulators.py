import csv
import itertools
import re

ISSN_RX = "^\d{4}-\d{3}[\dxX]$"


def cat_and_dedupe(analyses):
    inputs = []
    for analysis in analyses:
        reader = analysis.entries()
        inputs += [row + [analysis.source] for row in reader]

    inputs.sort()
    inputs = list(k for k, _ in itertools.groupby(inputs))
    return inputs


def issn_clusters(coincident_issn_analyses, clusters_file_handle):
    issn_clusters = []

    inputs = cat_and_dedupe(coincident_issn_analyses)
    inputs = remove_invalid_issns(inputs)
    inputs.sort(key=lambda x: x[0])

    current_issn_root = None
    current_cluster = []
    for row in inputs:
        if not current_issn_root:
            current_issn_root = row[0]
            current_cluster.append(row[0])
        if current_issn_root != row[0]:
            # the issn has changed, so we can write the current cluster
            current_cluster = list(set(current_cluster))
            current_cluster.sort()
            issn_clusters.append(current_cluster)

            current_cluster = [row[0]]
            current_issn_root = row[0]

        if len(row) > 1 and row[1]:
            current_cluster.append(row[1])

    issn_clusters.sort()
    issn_clusters = list(k for k,_ in itertools.groupby(issn_clusters))

    d = {}
    for row in issn_clusters:
        if row[0] not in d:
            d[row[0]] = []
        d[row[0]] += [row[x + 1] for x in range(len(row) - 1)]

    # go through every key, and for each value in the array associated with it, look
    # for the key of that value.  This will lead to another set of values.  For each
    # of those transitive values that are not the original key and are not in the
    # original list of values, add it to a list of additional values to add.  Then remove
    # the key of the value, so we don't count it again.
    #
    # In this way, we "consume" all the values into their unique super-clusters by
    # expanding the first list of values with values from all transitive lists.
    # In theory we end up with a unique list of coincident identifiers, with all
    # smaller subsets of the larger cluster subsumed into one.
    keys = list(d.keys())
    for k in keys:
        if k not in d:
            continue
        additions = []
        for e in d[k]:
            if e in d:
                additions += [x for x in d[e] if x != k and x not in d[k]]
                del d[e]
        d[k] = list(set(d[k] + additions))

    rows = []
    for k, v in d.items():
        rows.append([k] + v)

    writer = csv.writer(clusters_file_handle)
    writer.writerows(rows)


def remove_invalid_issns(input):
    valid = []
    for row in input:
        newrow = valid_issns(row)
        if len(newrow) > 0:
            valid.append(newrow)
    return valid


def valid_issns(issns):
    return [issn.upper() for issn in issns if re.match(ISSN_RX, issn)]

