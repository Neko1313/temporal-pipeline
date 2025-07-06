from pydantic import BaseModel, ConfigDict


class ComponentConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
