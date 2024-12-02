from airflow.plugins_manager import AirflowPlugin
from custom_schedule.working_day_schedule_timetable import WorkingDayScheduleTimeTable

class CustomSchedulePlugin(AirflowPlugin):
    name = "custom_schedule_plugin"
    timetables = [WorkingDayScheduleTimeTable]
