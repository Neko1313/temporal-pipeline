from typing import Literal

from pydantic import BaseModel

from core.component.interfaces.componentconfig import ComponentConfig


class Info(BaseModel):
    name: str
    version: str | None = None
    description: str | None = None
    type_class: object
    type_module: Literal["extract", "transform", "load", "core"]
    config_class: type[ComponentConfig]
