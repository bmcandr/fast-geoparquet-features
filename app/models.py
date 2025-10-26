from typing import Literal, Self

from pydantic import BaseModel

from app.enums import MediaType


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


class Link(BaseModel):
    title: str | None = None
    rel: Literal["self", "next", "prev"]
    href: str
    type: MediaType
