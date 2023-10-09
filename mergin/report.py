import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from itertools import groupby

from . import ClientError
from .merginproject import MerginProject, pygeodiff
from .utils import int_version

try:
    from qgis.core import (
        QgsGeometry,
        QgsDistanceArea,
        QgsCoordinateReferenceSystem,
        QgsCoordinateTransformContext,
        QgsWkbTypes,
    )

    has_qgis = True
except ImportError:
    has_qgis = False


# inspired by C++ implementation https://github.com/lutraconsulting/geodiff/blob/master/geodiff/src/drivers/sqliteutils.cpp
# in geodiff lib (MIT licence)
# ideally there should be pygeodiff api for it
def parse_gpkgb_header_size(gpkg_wkb):
    """Parse header of geopackage wkb and return its size"""
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
    """Derivative of geodiff ChangesetEntry suitable for further processing/reporting"""

    def __init__(self, changeset_entry, geom_idx, geom, qgs_distance_area=None):
        self.table = changeset_entry.table.name
        self.geom_type = geom["type"]
        self.crs = "EPSG:" + geom["srs_id"]
        self.length = None
        self.area = None
        self.dim = 0

        if changeset_entry.operation == changeset_entry.OP_DELETE:
            self.operation = "delete"
        elif changeset_entry.operation == changeset_entry.OP_UPDATE:
            self.operation = "update"
        elif changeset_entry.operation == changeset_entry.OP_INSERT:
            self.operation = "insert"
        else:
            self.operation = "unknown"

        # only calculate geom properties when qgis api is available
        if not qgs_distance_area:
            return

        crs = QgsCoordinateReferenceSystem()
        crs.createFromString(self.crs)
        qgs_distance_area.setSourceCrs(crs, QgsCoordinateTransformContext())

        if hasattr(changeset_entry, "old_values"):
            old_wkb = changeset_entry.old_values[geom_idx]
            if isinstance(old_wkb, pygeodiff.UndefinedValue):
                old_wkb = None
        else:
            old_wkb = None
        if hasattr(changeset_entry, "new_values"):
            new_wkb = changeset_entry.new_values[geom_idx]
            if isinstance(new_wkb, pygeodiff.UndefinedValue):
                new_wkb = None
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

        self.dim = QgsWkbTypes.wkbDimensions(qgs_geom.wkbType())
        if self.dim == 1:
            self.length = qgs_distance_area.measureLength(qgs_geom)
            if updated_qgs_geom:
                self.length = qgs_distance_area.measureLength(updated_qgs_geom) - self.length
        elif self.dim == 2:
            self.length = qgs_distance_area.measurePerimeter(qgs_geom)
            self.area = qgs_distance_area.measureArea(qgs_geom)
            if updated_qgs_geom:
                self.length = qgs_distance_area.measurePerimeter(updated_qgs_geom) - self.length
                self.area = qgs_distance_area.measureArea(updated_qgs_geom) - self.area


def changeset_report(changeset_reader, schema, mp):
    """Parse Geodiff changeset reader and create report from it.
    Aggregate individual entries based on common table, operation and geom type.
    If QGIS API is available, then lengths and areas of individual entries are summed.

    :Example:

        >>> geodiff.schema("sqlite", "", "/tmp/base.gpkg", "/tmp/base-schema.json")
        >>> with open("/tmp/base-schema.json", 'r') as sf:
                schema = json.load(sf).get("geodiff_schema")
        >>> cr = geodiff.read_changeset("/tmp/base.gpkg-diff")
        >>> changeset_report(cr, schema)
        [{"table": "Lines", "operation": "insert", "length": 1.234, "area": 0.0, "count": 3}]

    Args:
        changeset_reader (pygeodiff.ChangesetReader): changeset reader from geodiff changeset (diff file)
        schema: geopackage schema with list of tables (from full .gpkg file)
    Returns:
        list of dict of aggregated records
    """
    entries = []
    records = []

    if has_qgis:
        distance_area = QgsDistanceArea()
        distance_area.setEllipsoid("WGS84")
    else:
        distance_area = None
    # let's iterate through reader and populate entries
    for entry in changeset_reader:
        schema_table = next((t for t in schema if t["table"] == entry.table.name), None)
        if schema_table:
            # get geometry index in both gpkg schema and diffs values
            geom_idx = next(
                (index for (index, col) in enumerate(schema_table["columns"]) if col["type"] == "geometry"), None
            )
            if geom_idx is None:
                continue

            geom_col = schema_table["columns"][geom_idx]["geometry"]
            report_entry = ChangesetReportEntry(entry, geom_idx, geom_col, distance_area)
            entries.append(report_entry)
        else:
            mp.log.warning(f"Table {entry.table.name} is not present in the changeset.")

    # create a map of entries grouped by tables within single .gpkg file
    tables = defaultdict(list)
    for obj in entries:
        tables[obj.table].append(obj)

    # iterate through all tables and aggregate changes by operation type (e.g. insert) and geometry type (e.g point)
    # for example 3 point features inserted in 'Points' table would be single row with count 3
    for table, entries in tables.items():
        items = groupby(entries, lambda i: (i.operation, i.geom_type))
        for k, v in items:
            values = list(v)
            # sum lenghts and areas only if it makes sense (valid dimension)
            area = sum([entry.area for entry in values if entry.area]) if values[0].dim == 2 else None
            length = sum([entry.length for entry in values if entry.length]) if values[0].dim > 0 else None
            records.append({"table": table, "operation": k[0], "length": length, "area": area, "count": len(values)})
    return records


def create_report(mc, directory, since, to, out_file):
    """Creates report from geodiff changesets for a range of project versions in CSV format.

    Report is created for all .gpkg files and all tables within where updates were done using Geodiff lib.
    Changeset contains operation (insert/update/delete) and geometry properties like length/perimeter and area.
    Each row is an aggregate of the features modified by the same operation and of the same geometry type and contains
    these values: "file", "table", "author", "date", "time", "version", "operation", "length", "area", "count"

    No filtering and grouping is done here, this is left for third-party office software to use pivot table functionality.

    Args:
        mc (MerginClient): MerginClient instance.
        directory (str): local project directory (must already exist).
        since (str): starting project version tag, for example 'v3'.
        to (str): ending project version tag, for example 'v6'. If empty string is passed the latest version will be used.
        out_file (str): output file to save csv in

    Returns:
          List of warnings/issues for versions which could not be processed (e.g. broken history with missing diff)
    """
    mp = MerginProject(directory)
    project = mp.project_full_name()
    mp.log.info(f"--- Creating changesets report for {project} from {since} to {to if to else 'latest'} versions ----")
    versions = mc.project_versions(project, since, to if to else None)
    versions_map = {v["name"]: v for v in versions}
    headers = ["file", "table", "author", "date", "time", "version", "operation", "length", "area", "count"]
    records = []
    warnings = []
    info = mc.project_info(project, since=since)
    num_since = int_version(since)
    num_to = int_version(to) if to else int_version(versions[-1]["name"])
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
            full_gpkg = mp.fpath_cache(f["path"], version=to)
            if not os.path.exists(full_gpkg):
                mc.download_file(directory, f["path"], full_gpkg, to)

            # get gpkg schema
            schema_file = full_gpkg + "-schema.json"  # geodiff writes schema into a file
            if not os.path.exists(schema_file):
                mp.geodiff.schema("sqlite", "", full_gpkg, schema_file)
            with open(schema_file, "r") as sf:
                schema = json.load(sf).get("geodiff_schema")

            # add records for every version (diff) and all tables within geopackage
            for version in history_keys:
                if "diff" not in f["history"][version]:
                    if f["history"][version]["change"] == "updated":
                        warnings.append(f"Missing diff: {f['path']} was overwritten in {version} - broken diff history")
                    else:
                        warnings.append(f"Missing diff: {f['path']} was {f['history'][version]['change']} in {version}")
                    continue

                v_diff_file = mp.fpath_cache(f["history"][version]["diff"]["path"], version=version)
                version_data = versions_map[version]
                cr = mp.geodiff.read_changeset(v_diff_file)
                report = changeset_report(cr, schema, mp)
                # append version info to changeset info
                dt = datetime.fromisoformat(version_data["created"].rstrip("Z"))
                version_fields = {
                    "file": f["path"],
                    "author": version_data["author"],
                    "date": dt.date().isoformat(),
                    "time": dt.time().isoformat(),
                    "version": version_data["name"],
                }
                for row in report:
                    records.append({**row, **version_fields})
            mp.log.debug(f"done")
        except (ClientError, pygeodiff.GeoDiffLibError) as e:
            mp.log.warning(f"Skipping from report {f['path']}, issue found: {str(e)}")
            raise ClientError("Reporting failed, please check log for details")

    # export report to csv file
    out_dir = os.path.dirname(out_file)
    os.makedirs(out_dir, exist_ok=True)
    with open(out_file, "w", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records)
    mp.log.info(f"--- Report saved to {out_file} ----")
    return warnings
