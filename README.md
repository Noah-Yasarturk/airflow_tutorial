# airflow_tutorial
Following along with the Apache Airflow tutorial and implementing my own versions to test understanding of Apache Airflow concepts.
 Data and logs excluded. Docker file included. This guide was my guide to the initial installation: https://medium.com/@ayoub10hamaoui/how-to-run-airflow-on-windows-with-docker-part-2-f5024e8f8a21
 From there I used the Apache Airflow documentatation to facilitate learning, starting with the Architecture Overview and branching off as appropriate: https://airflow.apache.org/docs/apache-airflow/stable/index.html


The DAGs are under airflow/dags

Concepts covered thus far:
- DAGS, tasks, Operators, documentation
- Querying SQL data
- Conditional branching - on_success/failure_callback based and BranchPythonOperator based
- Subdags
- Xcoms (pull, push)
- UI - Labelling edges, Task Groups, tagging
- Jinja & templating
