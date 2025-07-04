import re
from typing import Literal

from pydantic import model_validator, Field, BaseModel


PATTER_INTERVAL = r"^(\d+)([smhd])$"


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
    def check_cron_or_interval(self):
        if self.cron and self.interval:
            raise ValueError("cron and interval cannot both be set")

        if self.cron and len(self.cron.split()) != 5:
            raise ValueError("cron must have exactly 5 elements")

        if self.interval and not re.match(PATTER_INTERVAL, self.interval):
            raise ValueError(f"don't pattern {PATTER_INTERVAL}")
