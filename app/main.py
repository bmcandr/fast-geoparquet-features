import logging
import re
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlencode

import cql2
import duckdb
import orjson
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, StreamingResponse
from jinja2 import Environment, FileSystemLoader
from starlette.templating import Jinja2Templates

from app.enums import MediaType, OutputFormat
from app.models import BBox, Link
from app.serializers import (
    stream_csv,
    stream_feature_collection,
    stream_geojsonseq,
    stream_parquet,
)

logger = logging.getLogger("uvicorn")


jinja2_env = Environment(
    loader=FileSystemLoader(f"{Path(__file__).resolve().parent}/templates")
)
templates = Jinja2Templates(env=jinja2_env)

FilterLang = Literal["cql2-text", "cql2-json"]

BBoxQuery = Query(default="bbox")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Set application lifespan variables including:
    * A reusable DuckDB connection
    """
    con = duckdb.connect()
    extensions = ["httpfs", "azure", "aws", "s3", "spatial"]
    con.execute("\n".join(f"INSTALL {ext}; LOAD {ext};" for ext in extensions))
    con.execute("SET http_keep_alive=false;")

    # TODO: figure out better pattern for this?
    con.execute("""CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    REGION 'us-west-2'
);""")

    app.state.db = con
    yield
    app.state.db.close()


app = FastAPI(
    title="fast-geoparquet-features",
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
    for batch in con.arrow(batch_size=100):  # type: ignore
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
    return (rel.count("*").fetchone() or [0])[0]


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
) -> AsyncGenerator[bytes]:
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
        f"{geom_conversion_func}({geom_column}) {geom_column}, "
        f"* EXCLUDE ({geom_column})"
    ).limit(limit, offset=offset)

    features = feature_generator(filtered, geom_column)
    if output_format == OutputFormat.GEOJSON or output_format is None:
        num_returned = get_count(filtered)
        links = build_links(request, number_matched=total, limit=limit, offset=offset)
        stream = stream_feature_collection(
            features=features,
            number_matched=total,
            number_returned=num_returned,
            limit=limit,
            offset=offset,
            links=links,
        )
    elif output_format in [OutputFormat.GEOJSONSEQ, OutputFormat.NDJSON]:
        stream = stream_geojsonseq(features)
    elif output_format == OutputFormat.CSV:
        stream = stream_csv(features)

    for chunk in stream:
        yield chunk


async def stream_features_to_parquet(
    con: duckdb.DuckDBPyConnection,
    url: str,
    geom_column: str | None = None,
    bbox_column: str | None = None,
    bbox: BBox | None = None,
    filter: str | None = None,
    filter_lang: FilterLang = "cql2-text",
) -> AsyncGenerator[bytes]:
    """Stream features from GeoParquet."""
    rel = base_rel(
        con=con,
        url=url,
        bbox=bbox,
        filter=filter,
        filter_lang=filter_lang,
    )

    for chunk in stream_parquet(rel, geom_column=geom_column, bbox_column=bbox_column):
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
    bbox_column: str = BBoxQuery,
    filter: str | None = Query(None, description="A CQL2 filter statement"),
    filter_lang: FilterLang = Query(default="cql2-text"),
    bbox: Annotated[BBox, str] | None = Depends(parse_bbox),
    f: OutputFormat = OutputFormat.GEOJSON,
):
    """Get Features"""
    if f in [OutputFormat.GEOPARQUET, OutputFormat.PARQUET]:
        return StreamingResponse(
            content=stream_features_to_parquet(
                con=con,
                url=url,
                geom_column=geom_column,
                bbox_column=bbox_column,
                bbox=bbox,
                filter=filter,
                filter_lang=filter_lang,
            ),
            headers={"Content-Disposition": "attachment; filename=features.parquet"},
        )
    else:
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
    filter_lang: FilterLang = Query(default="cql2-text"),
    bbox: Annotated[BBox, str] | None = Depends(parse_bbox),
):
    rel = base_rel(
        con=con,
        url=url,
        bbox=bbox,
        filter=filter,
        filter_lang=filter_lang,
    )
    total = get_count(rel)
    return {"numberMatched": total}


@app.get(
    "/tiles/{z}/{x}/{y}",
    responses={
        status.HTTP_200_OK: {
            "content": {
                MediaType.PBF: {},
            }
        }
    },
)
async def get_tile(
    z: int,
    x: int,
    y: int,
    url: str,
    geom_column: str | None = Query(None),
    bbox_column: str | None = Query(None),
    filter: str | None = Query(None),
    filter_lang: FilterLang = Query(default="cql2-text"),
    con: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
):
    geom_column = geom_column or "geometry"

    tile_bbox = next(
        iter(
            con.sql(
                """SELECT ST_Extent(
    ST_Transform(
        ST_TileEnvelope($1, $2, $3),
        'EPSG:3857',
        'EPSG:4326',
        always_xy := true
    )
)""",
                params=[z, x, y],
            ).fetchone()
            or []
        ),
        None,
    )

    if tile_bbox is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
        )

    rel = base_rel(
        con=con,
        url=url,
        bbox=BBox(
            bbox_column=bbox_column,
            xmin=tile_bbox["min_x"],
            ymin=tile_bbox["min_y"],
            xmax=tile_bbox["max_x"],
            ymax=tile_bbox["max_y"],
        ),
        filter=filter,
        filter_lang=filter_lang,
    )

    # NOTE: ST_Intersects unnecessary after bbox filter?
    #     rel.filter(f"""ST_Intersects(
    #         {geom_column},
    #         ST_Transform(
    #             ST_TileEnvelope({z}, {x}, {y}),
    #             'EPSG:3857',
    #             'EPSG:4326',
    #             always_xy := true
    #         )
    # )""")

    tile_blob = rel.aggregate(f"""ST_AsMVT(
        {{
            "geometry": ST_AsMVTGeom(
                ST_Transform(
                    {geom_column},
                    'EPSG:4326',
                    'EPSG:3857',
                    always_xy := true
                ),
                ST_Extent(ST_TileEnvelope({z}, {x}, {y}))
            )
        }}
    )""").fetchone()

    tile = tile_blob[0] if tile_blob and tile_blob[0] else b""

    return Response(
        tile,
        media_type=MediaType.PBF,
        headers={"Cache-Control": "max-age=3600, public"},
    )


@app.get(
    "/viewer",
    responses={
        status.HTTP_200_OK: {
            "content": {
                MediaType.HTML: {},
            }
        }
    },
)
def viewer(
    request: Request,
    url: str = Query(),
    geom_column: str | None = Query(None),
    bbox_column: str = BBoxQuery,
    filter: str | None = Query(None),
    filter_lang: FilterLang | None = Query(default=None),
):
    params = {
        k: v
        for k, v in {
            "url": url,
            "geom_column": geom_column,
            "bbox_column": bbox_column,
            "filter": filter,
            "filter_lang": filter_lang,
        }.items()
        if v is not None
    }
    tiles_url = (
        f"{request.base_url._url.rstrip('/')}/tiles/{{z}}/{{x}}/{{y}}?"
        + urlencode(params)
    )

    return HTMLResponse(
        templates.get_template("viewer.html").render(tiles_url=tiles_url),
        media_type=MediaType.HTML,
    )
