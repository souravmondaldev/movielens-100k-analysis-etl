import pendulum
from pendulum import DateTime
from typing import Optional
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class WorkingDayScheduleTimeTable(Timetable):
    """
    Custom timetable to schedule a invoke dag only on the working days
    """

    description = "Custom timetable to schedule a DAG only on working days."

    def __init__(self, hour: int = 0, minutes: int = 0):
        self.hour = hour
        self.minutes = minutes

    @property
    def summary(self) -> str:
        return f"working_day(hour={self.hour}, minutes={self.minutes})"

    def _is_working_day(self, year, month, day, hour, min):
        current_date = pendulum.datetime(year, month, day, hour, min)
        return current_date.weekday() < 5 #Weekday starts from Monday to Friday denotaed with 0-4

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval(start=run_after, end=run_after.add(days=1))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        now = pendulum.now()

        # Running for the first time
        if last_automated_data_interval is None:
            next_run_date = now
        else:
            last_run = last_automated_data_interval.start
            next_run_date = last_run.add(days=1)

        next_run_date = next_run_date if self._is_working_day(next_run_date.year, next_run_date.month, next_run_date.day, self.hour, self.minutes) else next_run_date.add(days=3)

        # Ensure run is within time restrictions
        if restriction.earliest and next_run_date < restriction.earliest:
            next_run_date = restriction.earliest
        if restriction.latest and next_run_date > restriction.latest:
            return None

        data_interval = DataInterval(start=next_run_date, end=next_run_date.add(days=1))
        return DagRunInfo(run_after=next_run_date, data_interval=data_interval)