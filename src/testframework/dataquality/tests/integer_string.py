from .regex_tst import RegexTest


class IntegerString(RegexTest):
    """
    Returns True when a string can be converted to an integer without losing information.
    01 -> False
    1 -> True
    -1 -> True
    1.1 -> False
    1.0 -> True
    """

    def __init__(self, *, name: str = "IntegerString") -> None:
        super().__init__(name=name, pattern=r"^-?(0|[1-9]\d*)(\.0+)?$")
