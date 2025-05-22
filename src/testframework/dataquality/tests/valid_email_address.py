# this file is called regex_tst instead of regex_test due to pytest confusion otherwise
from .regex_tst import RegexTest


class ValidEmailAddress(RegexTest):
    """
    Returns True when a string is a valid email address.

    This regex validates email addresses according to most common rules:
    - Local part can contain: letters, numbers, dots, underscores, percent signs, plus signs, and hyphens
    - Domain part must be valid with proper TLD
    - No consecutive dots allowed
    - Local and domain parts are separated by @
    """

    def __init__(self, *, name: str = "ValidEmailAddress") -> None:
        super().__init__(
            name=name,
            pattern=r"^(?!.*\.\.)[a-zA-Z0-9._%+-]+@(?!.*\.\.)[a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,}$",
        )
