from .graph import *
from .config import *

if __name__ == '__main__':
    try:
        import json
        config = Config(**json.loads(configJson))
    except NameError:
        config = Config()  # todo parse from args
    from airflow import DAG
    with DAG(**config.dag_args()) as the_dag:
        prophecy_workflow1(config) >> on_failure(config)

