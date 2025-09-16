import csv
import logging
import re
from collections.abc import Generator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal
from urllib.parse import urlencode

import cql2
import duckdb
import orjson
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from enums import MediaType, OutputFormat
from models import BBox, Link

logger = logging.getLogger("uvicorn")

FilterLang = Literal["cql2-text", "cql2-json"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Set application lifespan variables including:
    * A reusable DuckDB connection
    """
    con = duckdb.connect()
    extensions = ["httpfs", "azure", "aws", "spatial"]
    con.execute("\n".join(f"INSTALL {ext}; LOAD {ext};" for ext in extensions))

    app.state.db = con
    yield
    app.state.db.close()


app = FastAPI(
    title="FastFeatures",
    lifespan=lifespan,
)


def feature_generator(
    con: duckdb.DuckDBPyConnection | duckdb.DuckDBPyRelation,
    geom_column: str,
) -> Generator[dict[str, Any]]:
    """Yield GeoJSON like Features from an Arrow Table.

    Attempts to parse geometry column as JSON. If an error
    occurs, it is left as string (e.g., WKT for CSV output).
    """
    for batch in con.arrow(batch_size=100).to_batches():  # type: ignore
        for record in batch.to_pylist():
            if (geometry := record.pop(geom_column, None)) is not None:
                try:
                    geometry = orjson.loads(geometry)
                except orjson.JSONDecodeError:
                    pass
            else:
                continue

            yield {
                "type": "Feature",
                "geometry": geometry,
                "properties": record,
            }


def dump_feat(feat: dict[str, Any]) -> bytes:
    return orjson.dumps(
        feat,
        option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
    )


def build_links(
    request: Request,
    number_matched: int,
    limit: int,
    offset: int,
) -> list[Link]:
    params: dict[str, Any] = request.query_params._dict.copy()
    links = [
        Link(
            title="Features",
            rel="self",
            href=request.url._url,
            type=MediaType.GEOJSON,
        )
    ]

    base_url = request.url_for("get_features")._url

    if (next_offset := (offset + limit)) < number_matched:
        params["offset"] = next_offset
        links.append(
            Link(
                title="Next page",
                rel="next",
                href=f"{base_url}?{urlencode(params)}",
                type=MediaType.GEOJSON,
            )
        )

    if offset > 0:
        params["offset"] = max(offset - limit, 0)
        links.append(
            Link(
                title="Previous page",
                rel="prev",
                href=f"{base_url}?{urlencode(params)}",
                type=MediaType.GEOJSON,
            )
        )

    return links


def stream_feature_collection(
    features: Generator[dict[str, Any]],
    number_matched: int,
    limit: int,
    offset: int,
    request: Request,
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
                "numberReturned": i + 1,
                "limit": limit,
                "offset": offset,
                "links": [
                    link.model_dump()
                    for link in build_links(request, number_matched, limit, offset)
                ],
            }
        )
        .decode()
        .strip("{}")
    )
    yield f"], {metadata}}}".encode()


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


def base_rel(
    *,
    con: duckdb.DuckDBPyConnection,
    url: str,
    bbox: BBox | None,
    filter: str | None,
    filter_lang: FilterLang,
) -> duckdb.DuckDBPyRelation:
    filters = list()

    if bbox is not None:
        filters.append(bbox.to_sql())

    cql_filter = None
    cql_params = None
    if filter:
        parsed_filter = (
            cql2.parse_text(filter)
            if filter_lang == "cql2-text"
            else cql2.parse_json(filter)
        )
        cql_filter = parsed_filter.to_sql()
        filters.append(cql_filter.query)
        cql_params = cql_filter.params

    filter_stmt = f"WHERE {' AND '.join(filters)}"

    # HACK: rewrite scheme for Azure URLs (https:// -> az://)
    if url.startswith("https") and "blob.core.windows.net" in url:
        url = re.sub("^https", "az", url)

    rel = con.sql(
        f"""SELECT *
FROM read_parquet('{url}')
{filter_stmt if filters else ""}""",
        params=cql_params if filter else None,
    )
    return rel


def get_count(rel: duckdb.DuckDBPyRelation) -> int:
    return (rel.aggregate("COUNT(*) AS total").fetchone() or [0])[0]


async def stream_features(
    con: duckdb.DuckDBPyConnection,
    url: str,
    limit: int,
    offset: int,
    geom_column: str,
    request: Request,
    bbox: BBox | None = None,
    filter: str | None = None,
    filter_lang: FilterLang = "cql2-text",
    output_format: OutputFormat | None = None,
):
    """Stream features from GeoParquet."""
    rel = base_rel(
        con=con,
        url=url,
        bbox=bbox,
        filter=filter,
        filter_lang=filter_lang,
    )
    total = get_count(rel)

    offset = min(offset, max(total - limit, 0))

    geom_conversion_func = (
        "ST_AsText" if output_format in [OutputFormat.CSV] else "ST_AsGeoJSON"
    )

    filtered = rel.project(
        (
            f"{geom_conversion_func}({geom_column}) {geom_column}, "
            f"* EXCLUDE ({geom_column})"
        )
    ).limit(limit, offset=offset)

    features = feature_generator(filtered, geom_column)
    if output_format == OutputFormat.GEOJSON or output_format is None:
        stream = stream_feature_collection(
            features=features,
            number_matched=total,
            limit=limit,
            offset=offset,
            request=request,
        )
    elif output_format in [OutputFormat.GEOJSONSEQ, OutputFormat.NDJSON]:
        stream = stream_geojsonseq(features)
    elif output_format == OutputFormat.CSV:
        stream = stream_csv(features)

    for chunk in stream:
        yield chunk


def duckdb_cursor(request: Request) -> duckdb.DuckDBPyConnection:
    """Returns a threadsafe cursor from the connection stored in app state."""
    return request.app.state.db.cursor()


def parse_bbox(bbox: str | None = None) -> BBox | None:
    if bbox is None:
        return None

    try:
        return BBox.from_str(bbox)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )


@app.get(
    "/features",
    responses={
        status.HTTP_200_OK: {
            "content": {
                MediaType.GEOJSON: {},
                MediaType.GEOJSONSEQ: {},
                MediaType.CSV: {},
            }
        }
    },
)
async def get_features(
    request: Request,
    con: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
    url: str = Query(),
    limit: int = Query(
        default=10,
        gte=1,
        lte=10_000,
    ),
    offset: int = Query(default=0, ge=0),
    geom_column: str = Query(default="geometry"),
    filter: str | None = Query(None, description="A CQL2 filter statement"),
    filter_lang: FilterLang = Query(default="cql2-text"),
    bbox: Annotated[BBox, str] | None = Depends(parse_bbox),
    f: OutputFormat = OutputFormat.GEOJSON,
):
    """Get Features"""
    return StreamingResponse(
        stream_features(
            con=con,
            url=url,
            limit=limit,
            offset=offset,
            geom_column=geom_column,
            bbox=bbox,
            filter=filter,
            filter_lang=filter_lang,
            output_format=f,
            request=request,
        ),
        media_type=MediaType[f.name],
    )


@app.get("/features/count")
def get_feature_count(
    con: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
    url: str = Query(),
    filter: str | None = Query(None),
    bbox: Annotated[BBox, str] | None = Depends(parse_bbox),
):
    rel = base_rel(con=con, url=url, bbox=bbox, filter=filter)
    total = get_count(rel)
    return {"numberMatched": total}
