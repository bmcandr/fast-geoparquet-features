from typing import Literal, Self

import cql2
from fastapi import Query
from pydantic import BaseModel, model_validator

from app.enums import FilterLang, MediaType


class BBox(BaseModel):
    bbox_column: str = "bbox"
    xmin: float
    ymin: float
    xmax: float
    ymax: float

    @classmethod
    def from_str(cls, bbox: str) -> Self:
        if len(coords := bbox.split(",")) != 4:
            raise ValueError("bbox must be 4 comma-separated floats")
        else:
            try:
                xmin, ymin, xmax, ymax = tuple(float(c.strip()) for c in coords)
            except ValueError:
                raise ValueError("all bbox values must be floats")

        return cls(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)

    def to_sql(self) -> str:
        return " AND ".join(
            [
                f"{self.bbox_column}.xmax >= {self.xmin}",
                f"{self.bbox_column}.xmin <= {self.xmax}",
                f"{self.bbox_column}.ymax >= {self.ymin}",
                f"{self.bbox_column}.ymin <= {self.ymax}",
            ]
        )


class CQL2FilterParams(BaseModel):
    filter: str | None = Query(default=None, description="CQL2 Filter")
    filter_lang: FilterLang = Query(
        default="cql2-text", description="CQL2 Filter Language"
    )

    @property
    def cql_filter(self) -> cql2.Expr | None:
        if self.filter:
            cql_filter = (
                cql2.parse_text(self.filter)
                if self.filter_lang == "cql2-text"
                else cql2.parse_json(self.filter)
            )
            return cql_filter

    @model_validator(mode="after")
    def validate_filter(self) -> Self:
        if self.cql_filter:
            self.cql_filter.validate()
        return self


class Link(BaseModel):
    title: str | None = None
    rel: Literal["self", "next", "prev"]
    href: str
    type: MediaType
