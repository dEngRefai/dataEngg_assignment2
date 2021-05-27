# dataEngg_assignment2

"COVID-19 insights" AirFlow DAG with the following output: 

Jordan_scoring_report.png

Jordan_scoring_report.csv

PostgreSQL table Jordan_scoring_report


instructions:
1. clone the repo
2. navigate to the project folder and then do the following
3. ~$ mkdir dags logs plugins 
4. ~$ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
5. copy the provided "assignment_2_dag.py" to the dags folder you just created.
6. ~$ docker-compose up airflow-init
7. ~$ docker-compose up
8. use the port 8886 for jupyter/minimal-notebook
9. use the port 8087 for airflow
10. use the port 8089 for pgadmin4
11. the ports and passords can be found in the docker-compose file, however, they were not changed from the defaults originaly provided in class.
12. after signing to jupter notebook, configuring the pdadmine, and running the dag successfully, the output can be viewed via the jupter notebook environment and the pgadmin.
13. the following Youtube link provide a demo using google vm and Ubuntu 20.4 LTS, the docker version used can be installed following the instructions in the utilities folder.
14. https://youtu.be/k7SfI7HjnNk

oRefai
