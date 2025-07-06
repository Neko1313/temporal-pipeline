from pydantic import BaseModel
from typing import Literal


class Info(BaseModel):
    name: str
    version: str | None = None
    description: str | None = None
    type_class: object
    type_module: Literal["extract", "transform", "load", "core"]
    config_class: type[BaseModel]
