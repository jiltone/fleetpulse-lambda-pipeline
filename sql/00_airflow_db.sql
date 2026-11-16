-- =====================================================================
-- 00_airflow_db.sql
-- Creates a SEPARATE database + user for Airflow's metadata, so Airflow's
-- internal tables never mix with our business tables in the `fleet` DB.
--
-- Postgres runs every file in /docker-entrypoint-initdb.d/ (alphabetical
-- order) the first time the data volume is created. Files are run by psql,
-- so we can use \getenv to read the password from the environment instead
-- of hard-coding it here.
-- =====================================================================
\getenv airflow_user AIRFLOW_DB_USER
\getenv airflow_password AIRFLOW_DB_PASSWORD
\getenv airflow_db AIRFLOW_DB_NAME

CREATE ROLE :"airflow_user" WITH LOGIN PASSWORD :'airflow_password';
CREATE DATABASE :"airflow_db" OWNER :"airflow_user";
