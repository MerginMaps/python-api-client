import csv
import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from itertools import groupby

from mergin import ClientError
from mergin.merginproject import MerginProject, pygeodiff
from mergin.utils import int_version

try:
    from qgis.core import QgsGeometry, QgsDistanceArea, QgsCoordinateReferenceSystem, QgsCoordinateTransformContext, QgsWkbTypes
    has_qgis = True
except ImportError:
    has_qgis = False


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


def qgs_geom_from_wkb(geom):
    if not has_qgis:
        raise NotImplementedError
    g = QgsGeometry()
    wkb_header_length = parse_gpkgb_header_size(geom)
    wkb_geom = geom[wkb_header_length:]
    g.fromWkb(wkb_geom)
    return g


class ChangesetReportEntry:
    """ Derivative of geodiff ChangesetEntry suitable for further processing/reporting """
    def __init__(self, changeset_entry, geom_idx, geom):
        self.table = changeset_entry.table.name
        self.geom_type = geom["type"]
        self.crs = "EPSG:" + geom["srs_id"]
        self.length = None
        self.area = None

        if changeset_entry.operation == changeset_entry.OP_DELETE:
            self.operation = "delete"
        elif changeset_entry.operation == changeset_entry.OP_UPDATE:
            self.operation = "update"
        elif changeset_entry.operation == changeset_entry.OP_INSERT:
            self.operation = "insert"
        else:
            self.operation = "unknown"

        # only calculate geom properties when qgis api is available
        if not has_qgis:
            return

        d = QgsDistanceArea()
        d.setEllipsoid('WGS84')
        crs = QgsCoordinateReferenceSystem()
        crs.createFromString(self.crs)
        d.setSourceCrs(crs, QgsCoordinateTransformContext())

        if hasattr(changeset_entry, "old_values"):
            old_wkb = changeset_entry.old_values[geom_idx]
        else:
            old_wkb = None
        if hasattr(changeset_entry, "new_values"):
            new_wkb = changeset_entry.new_values[geom_idx]
        else:
            new_wkb = None

        # no geometry at all
        if old_wkb is None and new_wkb is None:
            return

        updated_qgs_geom = None
        if self.operation == "delete":
            qgs_geom = qgs_geom_from_wkb(old_wkb)
        elif self.operation == "update":
            qgs_geom = qgs_geom_from_wkb(old_wkb)
            # get new geom if it was updated, there can be updates also without change of geom
            updated_qgs_geom = qgs_geom_from_wkb(new_wkb) if new_wkb else qgs_geom
        elif self.operation == "insert":
            qgs_geom = qgs_geom_from_wkb(new_wkb)

        dim = QgsWkbTypes.wkbDimensions(qgs_geom.wkbType())
        if dim == 1:
            self.length = d.measureLength(qgs_geom)
            if updated_qgs_geom:
                self.length = d.measureLength(updated_qgs_geom) - self.length
        elif dim == 2:
            self.length = d.measurePerimeter(qgs_geom)
            self.area = d.measureArea(qgs_geom)
            if updated_qgs_geom:
                self.length = d.measurePerimeter(updated_qgs_geom) - self.length
                self.area = d.measureArea(updated_qgs_geom) - self.area


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
            if geom_idx is None:
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
            items = groupby(entries, lambda i: (i.operation, i.geom_type))
            for k, v in items:
                values = list(v)
                area = sum([entry.area for entry in values if entry.area]) if has_qgis else None
                length = sum([entry.length for entry in values if entry.length]) if has_qgis else None
                records.append({
                    "table": table,
                    "operation": k[0],
                    "length": length,
                    "area": area,
                    "count": len(values)
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
    versions_map = {v["name"]: v for v in mc.project_versions(project, since, to)}
    headers = ["file", "table", "author", "date", "time", "version", "operation", "length", "area", "count"]
    records = []
    info = mc.project_info(project, since=since)
    num_since = int_version(since)
    num_to = int_version(to)
    # filter only .gpkg files
    files = [f for f in info["files"] if mp.is_versioned_file(f["path"])]
    for f in files:
        mp.log.debug(f"analyzing {f['path']} ...")
        try:
            if "history" not in f:
                mp.log.debug(f"no history field, skip")
                continue

            # get version list (keys) within range
            history_keys = list(filter(lambda v: (num_since <= int_version(v) <= num_to), f["history"].keys()))
            if not history_keys:
                mp.log.debug(f"no file history within range, skip")
                continue

            # download diffs
            mc.download_file_diffs(directory, f["path"], history_keys)

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
            for version in history_keys:
                if "diff" not in f['history'][version]:
                    continue

                v_diff_file = os.path.join(mp.meta_dir, '.cache',
                                           version + "-" + f['history'][version]['diff']['path'])

                version_data = versions_map[version]
                cr = mp.geodiff.read_changeset(v_diff_file)
                rep = ChangesetReport(cr, schema)
                report = rep.report()
                # append version info to changeset info
                dt = datetime.fromisoformat(version_data["created"].rstrip("Z"))
                version_fields = {
                    "file": f["path"],
                    "author": version_data["author"],
                    "date": dt.date().isoformat(),
                    "time": dt.time().isoformat(),
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
