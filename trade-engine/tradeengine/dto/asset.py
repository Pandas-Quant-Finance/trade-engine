import hashlib
from dataclasses import dataclass
from typing import Any

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(frozen=True, eq=True)
class Asset:
    symbol: Any

    def __lt__(self, other):
        return self.symbol < other.symbol

    def __str__(self):
        return self.to_json()

    def __hash__(self):
        return int(hashlib.md5(str(self.symbol).encode("utf-8")).hexdigest(), 16)


# SOME CONSTANTS
CASH = Asset("$$$")
