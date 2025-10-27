import csv
import io
from collections.abc import Generator
from typing import Any

import duckdb
import orjson
import pyarrow.parquet as pq
from pyproj import CRS

from app.models import Link

WGS84_CRS_JSON = CRS.from_epsg(4326).to_json_dict()


def dump_feat(feat: dict[str, Any]) -> bytes:
    return orjson.dumps(
        feat,
        option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
    )


def stream_feature_collection(
    features: Generator[dict[str, Any]],
    number_matched: int,
    number_returned: int,
    limit: int,
    offset: int,
    links: list[Link],
) -> Generator[bytes]:
    yield b'{"type":"FeatureCollection","features":['

    for i, feat in enumerate(features):
        if i > 0:
            yield b"," + dump_feat(feat)
        else:
            yield dump_feat(feat)

    metadata = (
        orjson.dumps(
            {
                "numberMatched": number_matched,
                "numberReturned": number_returned,
                "limit": limit,
                "offset": offset,
                "links": [link.model_dump() for link in links],
            }
        )
        .decode()
        .strip("{")
    )
    yield f"], {metadata}".encode()


def stream_geojsonseq(features: Generator[dict[str, Any]]) -> Generator[bytes]:
    for feat in features:
        yield dump_feat(feat) + b"\n"


def stream_csv(features: Generator[dict[str, Any]]) -> Generator[bytes]:
    """Cribbed from TiPG:
    https://github.com/developmentseed/tipg/blob/b9aff728e857b9d40b56f315d91aa8b6ab397f8f/tipg/factory.py#L100
    """

    class DummyWriter:
        """Dummy writer that implements write for use with csv.writer."""

        def write(self, line: str):
            """Return line."""
            return line

    row = next(features)
    columns = row.keys()

    writer = csv.DictWriter(DummyWriter(), fieldnames=columns)

    yield writer.writerow(dict(zip(columns, columns)))

    yield writer.writerow(row)

    for row in features:
        yield writer.writerow(row)


def stream_parquet(
    rel: duckdb.DuckDBPyRelation, geom_column: str, bbox_column: str
) -> io.BytesIO:
    # DuckDB → Arrow Table
    reader = rel.arrow()

    # Optionally add GeoParquet metadata
    column_meta = {
        "encoding": "WKB",
        "geometry_types": [],
        "crs": WGS84_CRS_JSON,
        "edges": "planar",
        "covering": {
            "bbox": {
                "xmin": [f"{bbox_column}", "xmin"],
                "ymin": [f"{bbox_column}", "ymin"],
                "xmax": [f"{bbox_column}", "xmax"],
                "ymax": [f"{bbox_column}", "ymax"],
            }
        },
    }
    geo_meta = {
        "columns": {
            geom_column: column_meta,
        },
        "primary_column": geom_column,
        "version": "1.1.0",
    }

    schema = reader.schema.with_metadata(
        {
            **(reader.schema.metadata or {}),
            b"geo": orjson.dumps(geo_meta),
        }
    )

    buf = io.BytesIO()
    with pq.ParquetWriter(buf, schema, compression="zstd") as writer:
        for batch in reader:
            writer.write_batch(batch)

    buf.seek(0)
    return buf
