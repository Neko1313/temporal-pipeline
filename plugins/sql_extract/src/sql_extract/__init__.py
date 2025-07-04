from core.component import BaseProcessClass, ComponentConfig, Result, Info


class SQLExtract(BaseProcessClass):
    config = ComponentConfig()

    def process(self) -> Result:
        return Result(
            status="error",
            response=None,
        )

    @property
    def info(self) -> Info:
        return Info(
            name="SQLExtract",
            version="0.1.0",
            description="SqlExtract",
            type_class=self.__class__,
            type_module="extract",
            config_class=self.config,
        )