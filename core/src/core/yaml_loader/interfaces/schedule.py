import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

PATTER_INTERVAL = r"^(\d+)([smhd])$"
RANGE_CRON = 5


class ScheduleConfig(BaseModel):
    enabled: bool = Field(default=False, description="Включить расписание")

    cron: str | None = None
    interval: str | None = None

    timezone: str = "UTC"
    overlap_policy: Literal[
        "SKIP", "BUFFER_ONE", "BUFFER_ALL", "CANCEL_OTHER", "TERMINATE_OTHER"
    ] = "SKIP"
    catchup_window: str | None = None
    pause_on_failure: bool = False

    jitter: str | None = None

    notes: str = ""

    @model_validator(mode="after")
    def validate_schedule_config(self) -> "ScheduleConfig":
        if self.cron and self.interval:
            msg = "cron and interval cannot both be set"
            raise ValueError(msg)

        if self.cron and len(self.cron.split()) != RANGE_CRON:
            msg = f"cron must have exactly {RANGE_CRON} elements"
            raise ValueError(msg)

        if self.interval and not re.match(PATTER_INTERVAL, self.interval):
            msg = f"don't pattern {PATTER_INTERVAL}"
            raise ValueError(msg)
