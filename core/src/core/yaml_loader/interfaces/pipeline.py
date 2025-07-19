"""Type config yml activities."""

from pydantic import BaseModel, Field, field_validator

from core.yaml_loader.interfaces.resilience import RetryConfig
from core.yaml_loader.interfaces.schedule import ScheduleConfig
from core.yaml_loader.interfaces.stage import StageConfig
from core.yaml_loader.interfaces.utils import has_circular_dependencies


class PipelineConfig(BaseModel):
    """Pipeline configuration schema."""

    name: str = Field(..., min_length=1, max_length=100)
    description: str = ""
    version: str = Field(default="1.0.0", pattern=r"^\d+\.\d+\.\d+$")
    stages: dict[str, StageConfig] = Field(min_length=1)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)  # type: ignore
    execution_metadata: dict[str, str | bool] = Field(default_factory=dict)

    # Global settings
    max_parallel_stages: int = Field(default=3, ge=1, le=10)
    default_timeout: int = Field(default=300, gt=0)

    # Global default retry_policy (can be overridden per stage)
    default_resilience: RetryConfig = Field(default_factory=RetryConfig)

    # Environment variables
    required_env_vars: list[str] = Field(default_factory=list)

    @field_validator("stages")
    @classmethod
    def validate_stage_dependencies(
        cls, v: dict[str, StageConfig]
    ) -> dict[str, StageConfig]:
        """Check the dependencies between stages."""
        stage_names = set(v.keys())

        for _stage_name, stage_config in v.items():
            missing_deps = set(stage_config.depends_on) - stage_names
            if missing_deps:
                msg = f"Stage '{_stage_name}' \
                has missing dependencies: {missing_deps}"
                raise ValueError(msg)

        if has_circular_dependencies(v):
            msg = "Circular dependencies detected in stages"
            raise ValueError(msg)

        return v

    def get_effective_resilience(self, stage_name: str) -> RetryConfig:
        """Get the effective retry_policy configuration for the stage."""
        stage = self.stages.get(stage_name)
        if not stage:
            return self.default_resilience

        default_dict = self.default_resilience.model_dump()
        stage_dict = stage.resilience.model_dump()

        merged = {
            **default_dict,
            **{k: v for k, v in stage_dict.items() if v is not None},
        }

        return RetryConfig(**merged)
