import pytest

from sharkadm.validators import Validator


def validation_factory(name, display_name: str | None = None):
    new_validator = type(
        name,
        (Validator,),
        {
            "_display_name": display_name,
            "get_validator_description": lambda: None,
            "_validate": lambda: None,
        },
    )
    return new_validator


@pytest.mark.parametrize(
    "given_class_name", ("ValidateSomething", "ValidateSomethingElse")
)
def test_display_name_falls_back_to_class_name(given_class_name):
    # Given a validation class
    GivenValidationClass = validation_factory(given_class_name)

    # And no display is specified
    assert GivenValidationClass._display_name is None

    # When instancing the class
    validator = GivenValidationClass()

    # Then the class name will be used
    assert validator.display_name == given_class_name


@pytest.mark.parametrize(
    "given_class_name, given_display_name",
    (
        ("ValidateSomething", "Validate something"),
        ("ValidateSomethingElse", "Validate something else"),
    ),
)
def test_display_name_when_specified(given_class_name, given_display_name):
    # Given a validation class
    GivenValidationClass = validation_factory(given_class_name, given_display_name)

    # And a display is specified that is different from the class name
    assert GivenValidationClass._display_name is given_display_name
    assert GivenValidationClass._display_name != GivenValidationClass.__name__

    # When instancing the class
    validator = GivenValidationClass()

    # Then the display name will be used
    assert validator.display_name != given_class_name
    assert validator.display_name == given_display_name
