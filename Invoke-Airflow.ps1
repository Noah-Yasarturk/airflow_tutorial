$Network = "{0}_airflow" -f @(Split-Path $PSScriptRoot -Leaf)
docker run --rm --network $Network --volume "${PSScriptRoot}\airflow:/opt/airflow" apache/airflow @Args