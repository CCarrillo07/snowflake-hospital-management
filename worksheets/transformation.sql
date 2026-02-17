USE DATABASE bear_hospital_management;
USE SCHEMA raw;

--------------------------------------------
------Patients table transformations--------
/*
1. Gender: Standardize values to F or M only.
2. Date of birth: Convert to DATE data type
3. Contact number: Keep numbers only
4. Registration date: Convert to DATE data type
5. Email address: Convert to lowercase
*/
--------------------------------------------
SELECT *
FROM raw.patients
ORDER BY patient_id;

DESCRIBE TABLE raw.patients;

SELECT
    patient_id,
    first_name,
    last_name,
    CASE
        WHEN LOWER(gender) = 'female' THEN 'F'
        WHEN LOWER(gender) = 'male' THEN 'M'
        ELSE 'O'
    END AS gender,
    TO_DATE(date_of_birth, 'DD/MM/YYYY') AS date_of_birth,
    REGEXP_REPLACE(contact_number, '[^0-9]', '') AS contact_number,
    address,
    TO_DATE(registration_date, 'DD/MM/YYYY') AS registration_date,
    insurance_number,
    LOWER(email) AS email
FROM raw.patients;

SELECT *
FROM raw.patients;

CREATE OR REPLACE TABLE harmonized.patients (
    patient_id         VARCHAR(50),
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    gender             VARCHAR(1),
    date_of_birth      DATE,
    contact_number     VARCHAR(20),
    address            VARCHAR(255),
    registration_date  DATE,
    insurance_provider VARCHAR(100),
    insurance_number   VARCHAR(50),
    email              VARCHAR(255)
);

CREATE OR REPLACE PROCEDURE automation.sp_transform_patients()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT OVERWRITE INTO harmonized.patients
    SELECT
        patient_id,
        first_name,
        last_name,
        CASE
            WHEN LOWER(gender) = 'female' THEN 'F'
            WHEN LOWER(gender) = 'male' THEN 'M'
            ELSE 'O'
        END AS gender,
        TO_DATE(date_of_birth, 'DD/MM/YYYY') AS date_of_birth,
        REGEXP_REPLACE(contact_number, '[^0-9]', '') AS contact_number,
        address,
        TO_DATE(registration_date, 'DD/MM/YYYY') AS registration_date,
        insurance_provider,
        insurance_number,
        LOWER(email) AS email
    FROM raw.patients;

    RETURN 'Patients table transformation completed successfully';
END;
$$;

SELECT *
FROM harmonized.patients;

CALL automation.sp_transform_patients();

SELECT *
FROM harmonized.patients;

--------------------------------------------
------Doctors table transformations--------
/*
1. Specialization: Capitalize the first letter
2. Email address: Convert to lowercase
*/
--------------------------------------------
SELECT * 
FROM raw.doctors;

DESCRIBE TABLE raw.doctors;

SELECT 
    doctor_id,
    first_name,
    last_name,
    INITCAP(specialization) AS specialization,
    REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number,
    years_experience,
    hospital_branch,
    LOWER(email) AS email,
FROM raw.doctors;

CREATE OR REPLACE TABLE harmonized.doctors (
    doctor_id        VARCHAR(10),
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    specialization   VARCHAR(150),
    phone_number     VARCHAR(50),
    years_experience INTEGER,
    hospital_branch  VARCHAR(150),
    email            VARCHAR(150)
);

CREATE OR REPLACE PROCEDURE automation.sp_transform_doctors()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT OVERWRITE INTO harmonized.doctors
    SELECT
        doctor_id,
        first_name,
        last_name,
        INITCAP(specialization) AS specialization,
        REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number,
        years_experience,
        hospital_branch,
        LOWER(email) AS email,
    FROM raw.doctors;

    RETURN 'Doctors table transformation completed successfully';
END;
$$;

CALL automation.sp_transform_doctors();

SELECT * FROM
harmonized.doctors;

--------------------------------------------
------Appointments table transformations--------
/*
1. Appointment date: Convert to DATE data type
2. Appointment type: Convert to TIME data type
*/
--------------------------------------------
SELECT * 
FROM raw.appointments;

DESCRIBE TABLE raw.appointments;

SELECT 
    appointment_id,
    patient_id,
    doctor_id,
    TO_DATE(appointment_date, 'YYYY-MM-DD') AS appointment_date,
    CASE 
        -- If contains AM or PM → parse 12-hour input
        WHEN UPPER(appointment_time) LIKE '%AM%' 
            OR UPPER(appointment_time) LIKE '%PM%' 
        THEN TO_TIME(appointment_time, 'HH12:MI AM')
        -- Otherwise assume 24-hour with seconds
        ELSE TO_TIME(appointment_time, 'HH24:MI:SS')
    END AS appointment_time,
    reason_for_visit,
    status
FROM raw.appointments;

CREATE OR REPLACE TABLE harmonized.appointments (
    appointment_id    VARCHAR(10),
    patient_id        VARCHAR(10),
    doctor_id         VARCHAR(10),
    appointment_date  DATE,
    appointment_time  TIME,
    reason_for_visit  VARCHAR(255),
    status            VARCHAR(50)
);

CREATE OR REPLACE PROCEDURE automation.sp_transform_appointments()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT OVERWRITE INTO harmonized.appointments
    SELECT
        appointment_id,
        patient_id,
        doctor_id,
        TO_DATE(appointment_date, 'YYYY-MM-DD') AS appointment_date,
        CASE 
            -- If contains AM or PM → parse 12-hour input
            WHEN UPPER(appointment_time) LIKE '%AM%' 
            OR UPPER(appointment_time) LIKE '%PM%' 
            THEN TO_TIME(appointment_time, 'HH12:MI AM')
            -- Otherwise assume 24-hour with seconds
            ELSE TO_TIME(appointment_time, 'HH24:MI:SS')
        END AS appointment_time,
        reason_for_visit,
        status
    FROM raw.appointments;

    RETURN 'Appointments table transformation completed successfully';
END;
$$;

CALL automation.sp_transform_appointments();

SELECT * 
FROM harmonized.appointments;

----------------------------------------------
--------Treatments table transformations------
/*
1. Treatment date: Convert to DATE data type
*/
----------------------------------------------
SELECT * 
FROM raw.treatments;

DESCRIBE TABLE raw.treatments;

SELECT 
    treatment_id,
    appointment_id,
    treatment_type,
    description, 
    TO_DATE(treatment_date, 'DD/MM/YYYY') as treatment_date
FROM raw.treatments;

CREATE OR REPLACE TABLE harmonized.treatments (
   treatment_id   VARCHAR(10),
   appointment_id VARCHAR(10),
   treatment_type VARCHAR(150),
   description    VARCHAR(255),
   treatment_date DATE
);

CREATE OR REPLACE PROCEDURE automation.sp_transform_treatments()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT OVERWRITE INTO harmonized.treatments
    SELECT
        treatment_id,
        appointment_id,
        treatment_type,
        description, 
        TO_DATE(treatment_date, 'DD/MM/YYYY') as treatment_date
    FROM raw.treatments;

    RETURN 'Treatments table transformation completed successfully';
    
END;
$$;

CALL automation.sp_transform_treatments();

SELECT * 
FROM harmonized.treatments;

----------------------------------------------
---------Billing table transformations--------
/*
1. Bill date: Convert to DATE data type
2. Amount: lean and standardize values, then convert to a NUMBER data type.
3. Payment method: Standardize payment method values to consistent categories.
*/
----------------------------------------------
SELECT * 
FROM raw.billing;

SELECT DISTINCT payment_method
FROM raw.billing;

DESCRIBE TABLE raw.billing;

SELECT 
    bill_id,
    patient_id,
    treatment_id,
    TO_DATE(bill_date, 'DD/MM/YYYY') AS bill_date,
    REGEXP_REPLACE(amount, '[^0-9.]', '') AS amount,
    CASE
    WHEN LOWER(REPLACE(payment_method,' ','')) = 'insurance'
        THEN 'Insurance'
    WHEN LOWER(REPLACE(payment_method,' ','')) = 'creditcard'
        THEN 'Credit Card'
    WHEN LOWER(REPLACE(payment_method,' ','')) = 'cash'
        THEN 'Cash'
    ELSE 'Unknown'
    END AS payment_method,
    payment_status
FROM raw.billing;

CREATE OR REPLACE TABLE harmonized.billing (
    bill_id        VARCHAR(10),
    patient_id     VARCHAR(10),
    treatment_id   VARCHAR(10),
    bill_date      DATE,
    amount         NUMBER(10,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50)
);

CREATE OR REPLACE PROCEDURE automation.sp_transform_billing()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT OVERWRITE INTO harmonized.billing
    SELECT
         bill_id,
    patient_id,
    treatment_id,
    TO_DATE(bill_date, 'DD/MM/YYYY') AS bill_date,
    REGEXP_REPLACE(amount, '[^0-9.]', '') AS amount,
    CASE
    WHEN LOWER(REPLACE(payment_method,' ','')) = 'insurance'
        THEN 'Insurance'
    WHEN LOWER(REPLACE(payment_method,' ','')) = 'creditcard'
        THEN 'Credit Card'
    WHEN LOWER(REPLACE(payment_method,' ','')) = 'cash'
        THEN 'Cash'
    ELSE 'Unknown'
    END AS payment_method,
    payment_status
    FROM raw.billing;

    RETURN 'Billing table transformation completed successfully';
    
END;
$$;

CALL automation.sp_transform_billing();

SELECT * 
FROM harmonized.billing;
