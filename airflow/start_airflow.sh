#!/bin/bash

# Initialiser la base de données si ce n'est pas déjà fait
#echo "Initialisation de la base de données Airflow..."
#/home/angeulrich/Services/Airflow/airflow_env/bin/airflow db init

# Démarrer le Scheduler
echo "Démarrage du Scheduler..."
/home/angeulrich/Services/Airflow/airflow_env/bin/airflow scheduler &


# Démarrer le Webserver
echo "Démarrage du Webserver..."
/home/angeulrich/Services/Airflow/airflow_env/bin/airflow webserver --port 8092 &
