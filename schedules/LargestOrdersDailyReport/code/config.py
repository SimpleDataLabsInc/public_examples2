
from airflow.models import Variable

class Config(object):
    def __init__(self,
                 team: str = None,
                 project: str = None,
                 schedule: str = None,
                 deployment: str = None,
                 dag_name: str = None,
                 email: str = None,
                 fabric: dict = None,
                 owner:str = None,
                 connId:str = None,
                 start_date: str = None,
                 timezone: str = None,
                 schedule_interval: str = None,
                 job_size: str = None,
                 custom_arg1: str = None):
        self.team = team
        self.project = project
        self.schedule = schedule
        self.deployment = deployment
        self.dag_name = dag_name
        self.owner = owner
        self.connId = connId
        self.fabric = fabric
        self.email = email
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.job_size = job_size
        self.custom_arg1 = custom_arg1

    def dag_args(self):
        return {
            'dag_id': self.dag_name,
            'default_args': {
                "owner": self.owner,
                "email": [self.email],
                "depends_on_past": False,
                "start_date": self.start_date
            },
            "schedule_interval": self.schedule_interval
        }

