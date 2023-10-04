from typing import Optional
from pydantic import BaseModel


class UpdateInventory(BaseModel):
    id: Optional[str]
    sku: Optional[str]
    name: Optional[str]
    operation: Optional[str]
    parent_id: Optional[str]
    quantity: int

    class Stream:
        name = "UpdateInventory"
