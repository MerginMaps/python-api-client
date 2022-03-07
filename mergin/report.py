import csv
import json
import os
import tempfile
from collections import defaultdict
from itertools import groupby

from . import ClientError
from .merginproject import MerginProject, pygeodiff
from .utils import int_version


# inspired by C++ implementation https://github.com/lutraconsulting/geodiff/blob/master/geodiff/src/drivers/sqliteutils.cpp
# in geodiff lib (MIT licence)
# ideally there should be pygeodiff api for it
def parse_gpkgb_header_size(gpkg_wkb):
    """ Parse header of geopackage wkb and return its size """
    # some constants
    no_envelope_header_size = 8
    flag_byte_pos = 3
    envelope_size_mask = 14

    try:
        flag_byte = gpkg_wkb[flag_byte_pos]
    except IndexError:
        return -1  # probably some invalid input
    envelope_byte = (flag_byte & envelope_size_mask) >> 1
    envelope_size = 0

    if envelope_byte == 1:
        envelope_size = 32
    elif envelope_byte == 2:
        envelope_size = 48
    elif envelope_byte == 3:
        envelope_size = 48
    elif envelope_byte == 4:
        envelope_size = 64

    return no_envelope_header_size + envelope_size


class ChangesetReportEntry:
    """ Derivative of geodiff ChangesetEntry suitable for further processing/reporting """
    def __init__(self, changeset_entry, geom_idx, geom):
        self.table = changeset_entry.table.name
        self.geom_type = geom["type"]
        self.crs = geom["srs_id"]

        if changeset_entry.operation == changeset_entry.OP_DELETE:
            self.operation = "delete"
            self.old_geom = changeset_entry.old_values[geom_idx]
            self.new_geom = None
        elif changeset_entry.operation == changeset_entry.OP_UPDATE:
            self.operation = "update"
            self.old_geom = changeset_entry.old_values[geom_idx]
            self.new_geom = changeset_entry.new_values[geom_idx]
        elif changeset_entry.operation == changeset_entry.OP_INSERT:
            self.operation = "insert"
            self.old_geom = None
            self.new_geom = changeset_entry.new_values[geom_idx]

        self.count = None
        self.length = None
        self.area = None

        if self.geom_type == "LINESTRING":
            # we calculate change in length, for attributes changes only we set to 0
            self.metric = "length"
            if self.operation == "delete":
                self.length = self.measure(self.old_geom)
            elif self.operation == "update":
                self.length = self.measure(self.new_geom) - self.measure(self.old_geom)
            elif self.operation == "insert":
                self.length = self.measure(self.new_geom)
        elif self.geom_type == "POLYGON":
            # we calculate change in area, for attributes changes only we set to 0
            self.metric = "area"
            if self.operation == "delete":
                self.area = self.measure(self.old_geom)
            elif self.operation == "update":
                self.area = self.measure(self.new_geom) - self.measure(self.old_geom)
            elif self.operation == "insert":
                self.area = self.measure(self.new_geom)
        else:
            # regardless of geometry change count as 1
            self.metric = "count"
            self.count = 1

    def measure(self, geom):
        """ Return length or area of geometry based on type """
        # calculate geom length/area only if QGIS API is available
        try:
            from qgis.core import QgsGeometry, QgsDistanceArea, QgsCoordinateReferenceSystem, QgsCoordinateTransformContext
        except ImportError:
            return -1

        d = QgsDistanceArea()
        d.setEllipsoid('WGS84')
        crs = QgsCoordinateReferenceSystem()
        crs.createFromString(self.crs)
        d.setSourceCrs(crs, QgsCoordinateTransformContext())
        g = QgsGeometry()
        wkb_header_length = parse_gpkgb_header_size(geom)
        wkb_geom = geom[wkb_header_length:]
        g.fromWkb(wkb_geom)
        if self.metric == "length":
            return d.measureLength(g)
        elif self.metric == "area":
            return d.measureArea(g)
        else:
            return 1


class ChangesetReport:
    """ Report (aggregate) from geopackage changeset (diff file) """
    def __init__(self, changeset_reader, schema):
        self.cr = changeset_reader
        self.schema = schema
        self.entries = []
        # let's iterate through reader and populate entries
        for entry in self.cr:
            schema_table = next((t for t in schema if t["table"] == entry.table.name), None)
            # get geometry index in both gpkg schema and diffs values
            geom_idx = next((index for (index, col) in enumerate(schema_table["columns"]) if col["type"] == "geometry"),
                            None)
            if not geom_idx:
                continue

            geom_col = schema_table["columns"][geom_idx]["geometry"]
            report_entry = ChangesetReportEntry(entry, geom_idx, geom_col)
            self.entries.append(report_entry)

    def report(self):
        """ Aggregate entries by table and operation type and calculate quantity """
        records = []
        tables = defaultdict(list)

        for obj in self.entries:
            tables[obj.table].append(obj)

        for table, entries in tables.items():
            items = groupby(entries, lambda i: (i.operation, i.metric))
            for k, v in items:
                quantity_type = "_".join(k)
                values = list(v)
                quantity = sum([getattr(entry, k[1]) for entry in values])
                records.append({
                    "table": table,
                    "quantity_type": quantity_type,
                    "quantity": quantity
                })
        return records


def create_report(mc, directory, project, since, to, out_dir=tempfile.gettempdir()):
    """ Creates report from geodiff changesets for a range of project versions in CSV format.

    Args:
        mc (MerginClient): MerginClient instance.
        directory (str): local project directory (must already exist).
        project (str): full project name (<namespace/project>).
        since (str): starting project version tag, for example 'v3'.
        to (str): ending project version tag, for example 'v6'.
        out_dir (str): output directory to save report.csv in, defaults to temp dir
    """
    mp = MerginProject(directory)
    mp.log.info(f"--- Creating changesets report for {project} from {since} to {to} versions ----")
    versions_map = {v["name"]: v for v in mc.project_versions(project, since, to)["versions"]}
    headers = ["file", "table", "author", "timestamp", "version", "quantity_type", "quantity"]
    records = []
    info = mc.project_info(project, since=since)
    # filter only .gpkg files
    files = [f for f in info["files"] if mp.is_versioned_file(f["path"])]
    for f in files:
        mp.log.debug(f"analyzing {f['path']} ...")
        diff_file = os.path.join(mp.meta_dir, f["path"] + ".diff")
        try:
            if "history" not in f:
                continue

            # download diffs in desired range, in case versions are not within history,
            # pass unknown, and it will be determined in download function
            # it can be different for each file
            since_ = since if since in f["history"] else None
            to_ = to if to in f["history"] else None
            if not (since_ and to_):
                continue  # no change at all for particular file in desired range

            mc.get_file_diff(directory, f["path"], diff_file, since_, to_, True)

            # download full gpkg in "to" version to analyze its schema to determine which col is geometry
            full_gpkg = os.path.join(mp.meta_dir, ".cache", f["path"])
            if not os.path.exists(full_gpkg):
                mc.download_file(directory, f["path"], full_gpkg, to)

            # get gpkg schema
            schema_file = full_gpkg + '-schema'  # geodiff writes schema into a file
            if not os.path.exists(schema_file):
                mp.geodiff.schema("sqlite", "", full_gpkg, schema_file)
            with open(schema_file, 'r') as sf:
                schema = json.load(sf).get("geodiff_schema")

            # add records for every version (diff) and all tables within geopackage
            for idx in range(int_version(since) + 1, int_version(to) + 1):
                version = "v" + str(idx)
                # skip version if there was no diff file
                if version not in f['history']:
                    continue
                v_diff_file = os.path.join(mp.meta_dir, '.cache',
                                           version + "-" + os.path.basename(f['history'][version]['diff']['path']))

                version_data = versions_map[version]
                cr = mp.geodiff.read_changeset(v_diff_file)
                rep = ChangesetReport(cr, schema)
                report = rep.report()
                # append version info to changeset info
                version_fields = {
                    "file": f["path"],
                    "author": version_data["author"],
                    "timestamp": version_data["created"],
                    "version": version_data["name"]
                }
                for row in report:
                    records.append({**row, **version_fields})
            mp.log.debug(f"done")
        except (ClientError, pygeodiff.GeoDiffLibError) as e:
            mp.log.warning(f"Skipping from report {f['path']}, issue found: {str(e)}")
            raise ClientError("Reporting failed, please check log for details")

    # export report to csv file
    proj_name = project.replace(os.path.sep, "-")
    report_file = os.path.join(out_dir, f'report-{proj_name}-{since}-{to}.csv')
    with open(report_file, 'w', newline='') as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records)
    mp.log.info(f"--- Report saved to {report_file} ----")
