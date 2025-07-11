import re
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator

PATTER_INTERVAL = r"^(\d+)([smhd])$"
RANGE_CRON = 5
CRON_REGEX = r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$"


class ScheduleConfig(BaseModel):
    enabled: bool = Field(default=False, description="Включить расписание")

    cron: str | None = Field(None, pattern=CRON_REGEX)
    interval: str | None = Field(None, pattern=PATTER_INTERVAL)

    timezone: str = "UTC"
    overlap_policy: Literal[
        "SKIP", "BUFFER_ONE", "BUFFER_ALL", "CANCEL_OTHER", "TERMINATE_OTHER"
    ] = "SKIP"
    catchup_window: str | None = None
    pause_on_failure: bool = False

    jitter: str | None = None

    notes: str = ""

    @model_validator(mode="after")
    def validate_schedule_config(self) -> Self:
        if self.cron and self.interval:
            msg = "cron and interval cannot both be set"
            raise ValueError(msg)

        if self.cron and not re.match(CRON_REGEX, self.cron):
            msg = f"cron must have exactly {RANGE_CRON} elements"
            raise ValueError(msg)

        if self.interval and not re.match(PATTER_INTERVAL, self.interval):
            msg = f"don't pattern {PATTER_INTERVAL}"
            raise ValueError(msg)

        return self
