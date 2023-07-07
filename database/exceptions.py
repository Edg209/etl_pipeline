class MissingPrimaryKeyException(Exception):
    """An operation that requires a primary key to exist was attempted on a table without a primary key."""
    def __init__(self, msg: str):
        self.msg = msg
