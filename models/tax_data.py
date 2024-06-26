from dataclasses import dataclass


@dataclass
class TaxData:
    token: str
    tax: float
    timestamp: int
