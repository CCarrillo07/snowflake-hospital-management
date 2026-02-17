-------------------
--Create database--
-------------------
CREATE DATABASE bear_hospital_management;

USE DATABASE bear_hospital_management;

------------------
--Create schemas--
------------------
CREATE SCHEMA raw;
CREATE SCHEMA harmonized;
CREATE SCHEMA analytics;
CREATE SCHEMA automation;

-------------------------------------------------------
-- Create storage integration, file format, and stage--
-------------------------------------------------------
USE SCHEMA public;

--Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION bear_s3_role_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::457151801201:role/snowflake_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snow-hospital-management/');

DESCRIBE INTEGRATION bear_s3_role_integration;

-- Create file format
CREATE OR REPLACE FILE FORMAT public.csv_ff
    TYPE = 'csv'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL');

-- Create stage
CREATE OR REPLACE STAGE public.s3_load_stage
    URL = 's3://snow-hospital-management/'
    STORAGE_INTEGRATION = bear_s3_role_integration
    FILE_FORMAT = public.csv_ff;

LIST @public.s3_load_stage/patients;

SELECT $1, $2, $3, $4 FROM @public.s3_load_stage/patients;

-------------------------------
--Create tables in raw schema--
-------------------------------

--Patients table
USE SCHEMA raw;

CREATE OR REPLACE TABLE patients (
    patient_id         VARCHAR(50),
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    gender             VARCHAR(50),
    date_of_birth      VARCHAR(10),
    contact_number     VARCHAR(20),
    address            VARCHAR(255),
    registration_date  VARCHAR(10),
    insurance_provider VARCHAR(100),
    insurance_number   VARCHAR(50),
    email              VARCHAR(255)
);

SELECT * FROM raw.patients;

COPY INTO raw.patients
FROM @public.s3_load_stage/patients/
FILE_FORMAT = public.csv_ff;

SELECT * FROM raw.patients;

--PIPE for patients table
USE SCHEMA automation;

CREATE OR REPLACE PIPE patients_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO raw.patients
FROM @public.s3_load_stage/patients/
FILE_FORMAT = public.csv_ff;

DESCRIBE PIPE patients_pipe;

SELECT *
FROM raw.patients;

--Doctors table
CREATE TABLE raw.doctors (
    doctor_id        VARCHAR(10),
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    specialization   VARCHAR(150),
    phone_number     VARCHAR(50),
    years_experience INTEGER,
    hospital_branch  VARCHAR(150),
    email            VARCHAR(150)
);

--PIPE for doctors table
CREATE OR REPLACE PIPE automation.doctors_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO raw.doctors
FROM @public.s3_load_stage/doctors/
FILE_FORMAT = public.csv_ff;

DESCRIBE PIPE automation.doctors_pipe;

SELECT *
FROM raw.doctors;

--Appointments table
CREATE TABLE raw.appointments (
    appointment_id    VARCHAR(10),
    patient_id        VARCHAR(10),
    doctor_id         VARCHAR(10),
    appointment_date  VARCHAR(10),
    appointment_time  VARCHAR(20),
    reason_for_visit  VARCHAR(255),
    status            VARCHAR(50)
);

--PIPE for appointments table
CREATE OR REPLACE PIPE automation.appointments_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO raw.appointments
FROM @public.s3_load_stage/appointments/
FILE_FORMAT = public.csv_ff;

DESCRIBE PIPE automation.appointments_pipe;

SELECT *
FROM raw.appointments;

--Treatments table
CREATE TABLE raw.treatments (
    treatment_id   VARCHAR(10),
    appointment_id VARCHAR(10),
    treatment_type VARCHAR(150),
    description    VARCHAR(255),
    treatment_date VARCHAR(10)
);

--PIPE for treatments table
CREATE OR REPLACE PIPE automation.treatments_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO raw.treatments
FROM @public.s3_load_stage/treatments/
FILE_FORMAT = public.csv_ff;

DESCRIBE PIPE automation.treatments_pipe;

SELECT *
FROM raw.treatments;

--Billing table
CREATE TABLE raw.billing (
    bill_id        VARCHAR(10),
    patient_id     VARCHAR(10),
    treatment_id   VARCHAR(10),
    bill_date      VARCHAR(10),
    amount         VARCHAR(20),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50)
);

--PIPE for billing table
CREATE OR REPLACE PIPE automation.billing_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO raw.billing
FROM @public.s3_load_stage/billing/
FILE_FORMAT = public.csv_ff;

DESCRIBE PIPE automation.billing_pipe;

SELECT *
FROM raw.billing;
