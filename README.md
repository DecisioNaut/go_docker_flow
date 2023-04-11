# Udacity's Data Engineering With AWS Nano-Degree - Project: Automate Data Pipelines


This is a fictional __project for lesson 4 of Udacity's Data Engineering with AWS nano-degree__ to be reviewed by Udacity.

__Table of Contents__
1. [The (Fictional) Task](#1-the-fictional-task)
2. [Repository structure](#2-repository-structure)
3. [Usage](#3-usage)

## 1. The (Fictional) Task
The fictional task is to help the startup "Sparkify". It is more or less the same task as in lesson 2, now just using [Apache Airflow](airflow.apache.org). For more on Sparkify and the data, please see [here in my previous repo](https://github.com/DecisioNaut/cloud_warehouse).  

__Please note the following__:
- In the aforementioned repo, I intensively explored the data and therefore came up with slightly different design choices for the dimension and fact table construction than Udacity (which had a rather naive approach). The dimensions are better designed when mostly based on the log-data and only partially enriched by song-data where applicable. This also leads to dependencies of the songs dimension table on the artists dimension table, which therefore should be reflected in the pipeline.
- The song data in Udacity's S3 bucket is quite large and in a nested folder structure, which sometimes makes loading the data break. However, the song data is not well aligned with the log data, and therefore I suggest that - for this demonstration - it is also sufficient to load them only partially (like from s3://udacity-dend/song-data/A/A/A) (for Udacity students see also [here](https://knowledge.udacity.com/questions/977213)).  
- Although the rubric asks for an hourly schedule of the project DAG/pipeline, I decided to set it to daily to avoid it from running wild directly after starting Airflow triggering numerous runs.

## 2. Repository structure
```
dags/
    custom_operators/
        __init__.py
        data_quality.py
        load_dimension.py
        load_fact.py
        stage_redshift.py
        static_query.py     # Operator for creating and dropping tables
    helpers/
        __init__.py
        sql_queries.py      # Including the static queries
    create_tables_dag.py    # DAG to create the table necessary
    sparkify_dag.py         # Project DAG
    drop_tables_dag.py      # DAG to drop the tables
plugins/
    # Empty as Airflow does not accept operators as plugins since version 2.0 (see above in README.md)
.gitignore
docker_compose.yaml
README.md
```

Please note that [plugins do not allow for importing operators since Airflow version 2.0](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html) as well as hooks and sensors any longer. Therefore I rearranged the files structure compared to the template provided by Udacity.
Furthermore, I decided to build Airflow DAGs for creating and destroying the tables in Redshift as `create_tables_dag.py` and `drop_tables_dag.py`, both using the Operator defined in `static_query.py`.


## 3. Usage

To make use of this repo, please ensure that you have installed
- Docker and
- PostgeSQL
on your local machine.

In addition, of course, you'll need a setup on AWS (which I used my free-tier account for):
- An account (I used the free tier)
- An IAM user, e.g. called awsuser, 
    - being granted programmatic access via access key ID and secret access key 
    - having AdministratorAccess, AmazonRedshiftFullAccess and AmazonS3FullAccess
- A role, e.g. called `my-redshift-service-role` for accessing Redshift with 
    - this policy document:
        ```
        '{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                    "Service": "redshift.amazonaws.com"
                    },
                "Action": "sts:AssumeRole"
                }
            ]
        }'
        ```
    - add AmazonS3FullAccess
- A Redshift Serverless service, having
    - the IAM user above as admin with a password of your choice
    - the role above attached to the service
    - the corresponding VPC made publicly available and adding inbound rules for Custom TCP, with a port range of 0 - 5000 and Anywhere-iPv4 as source
    - the service itself made publicly accessible


```
docker compose up airflow-init
docker compose up
```  

Go to Airflow GUI at [0.0.0.0:8081](http://0.0.0.0:8081) and log in as user "airflow" with password "airflow"  

Now you need to setup the following Airflow connections to
- Amazon Web Services via the IAM users from above and
- Amazon Redshift (serverless) from above

Now, finally, you should be able to run your DAGs by first run the `create_tables` DAG and then the `sparkify_pipe` DAG.

If you're done, you can of course run the `drop_tables` DAG to clean up your Redshift database and terminate your docker containers by running

```
docker compose down
```

If you want to remove the data saved in the volumes as well (e.g. the variables and the connections saved in PostgreSQL) run
```  
docker compose down --volumes --remove-orphans
```  
  