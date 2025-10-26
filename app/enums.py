from enum import Enum


class MediaType(str, Enum):
    """Responses Media types formerly known as MIME types."""

    XML = "application/xml"
    JSON = "application/json"
    NDJSON = "application/ndjson"
    GEOJSON = "application/geo+json"
    GEOJSONSEQ = "application/geo+json-seq"
    GEOPARQUET = "application/x-parquet"
    PARQUET = "application/x-parquet"
    SCHEMAJSON = "application/schema+json"
    HTML = "text/html"
    TEXT = "text/plain"
    CSV = "text/csv"
    OPENAPI30_JSON = "application/vnd.oai.openapi+json;version=3.0"
    OPENAPI30_YAML = "application/vnd.oai.openapi;version=3.0"
    PBF = "application/x-protobuf"
    MVT = "application/vnd.mapbox-vector-tile"


class OutputFormat(str, Enum):
    GEOJSON = "geojson"
    GEOJSONSEQ = "geojsonseq"
    NDJSON = "ndjson"
    CSV = "csv"
    GEOPARQUET = "geoparquet"
    PARQUET = "parquet"


MEDIA_TYPE_MAP = {
    OutputFormat.GEOJSON: MediaType.GEOJSON,
    OutputFormat.GEOJSONSEQ: MediaType.GEOJSONSEQ,
    OutputFormat.NDJSON: MediaType.NDJSON,
    OutputFormat.CSV: MediaType.CSV,
}
