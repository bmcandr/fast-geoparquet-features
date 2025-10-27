import cql2
import pytest

from app.models import CQL2FilterParams


@pytest.mark.parametrize(
    "filter,filter_lang",
    [
        [
            "landsat:scene_id = 'LC82030282019133LGN00'",
            "cql2-text",
        ],
        [
            '{"op":"=","args":[{"property":"landsat:scene_id"},"LC82030282019133LGN00"]}',  # noqa: E501
            "cql2-json",
        ],
    ],
)
def test_cql2_filter_validate_passes(filter, filter_lang):
    CQL2FilterParams(filter=filter, filter_lang=filter_lang)


@pytest.mark.parametrize(
    "filter,filter_lang",
    [
        [
            # missing eq
            "landsat:scene_id 'LC82030282019133LGN00'",
            "cql2-text",
        ],
        [
            # cribbed from: https://github.com/developmentseed/cql2-rs/blob/ef69a664f567e5de102c7dbfbfe6c25bee30546e/python/tests/test_expr.py#L49 # noqa: E501
            '{"op": "t_before", "args": [{"property": "updated_at"}, {"timestamp": "invalid-timestamp"}]}',  # noqa: E501
            "cql2-json",
        ],
    ],
)
def test_cql2_filter_validation_fails(filter, filter_lang):
    with pytest.raises(cql2.ValidationError):
        CQL2FilterParams(filter=filter, filter_lang=filter_lang)
