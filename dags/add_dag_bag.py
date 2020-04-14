""" add additional DAGs folders """
from airflow.models import DagBag

dags_dirs = ['/Users/dkulemza/airflow/dags_dir']

for dir in dags_dirs:
 dag_bag = DagBag(dir)

 if dag_bag:
    for dag_id, dag in dag_bag.dags.items():
        globals()[dag_id] = dag