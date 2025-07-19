"""Const base component."""

from core.component.interfaces import ComponentConfig, Info

ERROR_MESSAGE = "Method must be implemented in derived class"

BASE_INFO = Info(
    name="base",
    version="0.1.0",
    description=None,
    type_class=type(Info),
    type_module="core",
    config_class=ComponentConfig,
)
