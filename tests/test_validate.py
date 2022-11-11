
import pytest
from ftl_diff.validate import validate, get_errors, ValidationError


def test_validate_empty():
    validate({}, {})


def test_get_errors_empty():
    errors = get_errors({}, {})
    assert len(list(errors)) == 0


def test_validate_simple():
    schema = {
        "type": "object",
        "properties": {
            "price": {"type": "number"},
            "name": {"type": "string"},
        },
    }
    validate({"name" : "Eggs", "price" : 34.99}, schema)
    with pytest.raises(ValidationError):
        assert validate({"name" : "Eggs", "price" : "Invalid"}, schema)


def test_get_errors_simple():
    schema = {
        "type": "object",
        "properties": {
            "price": {"type": "number"},
            "name": {"type": "string"},
        },
    }
    errors = get_errors({"name" : "Eggs", "price" : "Invalid"}, schema)
    assert len(list(errors)) == 1
