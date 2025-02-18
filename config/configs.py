# Creating configs class to be used across project
import numpy as np
from datetime import datetime
from datetime import timedelta

class channel_pref_analysis_config():
    def __init__(self) -> None:
        self.s3_bucket = 'nmg-analytics-ds-prod'
        self.s3_project_path = 'ds/prod/channel_preference/'
        self.sns_topic_arn = 'arn:aws:sns:us-west-2:786228325737:ProductionScheduleJobNotification'
        self.project_name = 'channel_preference'
        self.current_date = datetime.now().date()
        self.todays_date = self.current_date.strftime("%Y-%m-%d")
        self.initial_datetime = self.current_date + timedelta(days=-365)
        self.initial_date = self.initial_datetime.strftime("%Y-%m-%d")
        self.prod_instance_name = 'Emerald'
        
        
        #'DS918'
