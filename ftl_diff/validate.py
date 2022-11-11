

import jsonschema


def get_errors(state, schema):
    validator = jsonschema.Draft7Validator(schema)
    for error in validator.iter_errors(state):
        yield error


def validate(state, schema):
    validator = jsonschema.Draft7Validator(schema)
    validator.validate(state)


ValidationError = jsonschema.ValidationError
