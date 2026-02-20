USE DATABASE bear_hospital_management;
USE SCHEMA analytics;

--------------------------------------------
------------Create dim tables---------------
--------------------------------------------

-- Dim patients
CREATE OR REPLACE TABLE analytics.dim_patients (
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

INSERT INTO analytics.dim_patients
    SELECT * FROM harmonized.patients;

SELECT * FROM analytics.dim_patients;

-- Dim doctors
CREATE OR REPLACE TABLE analytics.dim_doctors (
    doctor_id        VARCHAR(10),
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    specialization   VARCHAR(150),
    phone_number     VARCHAR(50),
    years_experience INTEGER,
    hospital_branch  VARCHAR(150),
    email            VARCHAR(150)
);

INSERT INTO analytics.dim_doctors
    SELECT * FROM harmonized.doctors;

SELECT * FROM analytics.dim_doctors;

--------------------------------------------
------------Create fact tables--------------
--------------------------------------------

-- fact appointments
CREATE OR REPLACE TABLE analytics.fact_appointments (
    appointment_id    VARCHAR(10),
    patient_id        VARCHAR(10),
    doctor_id         VARCHAR(10),
    appointment_date  DATE,
    appointment_time  TIME,
    reason_for_visit  VARCHAR(255),
    status            VARCHAR(50)
);

INSERT INTO analytics.fact_appointments
    SELECT * FROM harmonized.appointments;

SELECT * FROM analytics.fact_appointments;

-- fact treatments
CREATE OR REPLACE TABLE analytics.fact_treatments (
   treatment_id   VARCHAR(10),
   appointment_id VARCHAR(10),
   treatment_type VARCHAR(150),
   description    VARCHAR(255),
   treatment_date DATE
);

INSERT INTO analytics.fact_treatments
    SELECT * FROM harmonized.treatments;

SELECT * FROM analytics.fact_treatments;

-- fact billing 
CREATE OR REPLACE TABLE analytics.fact_billing (
    bill_id        VARCHAR(10),
    patient_id     VARCHAR(10),
    treatment_id   VARCHAR(10),
    bill_date      DATE,
    amount         NUMBER(10,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50)
);

INSERT INTO analytics.fact_billing
    SELECT * FROM harmonized.billing;

SELECT * FROM analytics.fact_billing;

---------------------------------------------
----Dim patients table incremental load------
---------------------------------------------

CREATE OR REPLACE STREAM harmonized.patients_stream
ON TABLE harmonized.patients
APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE automation.sp_load_dim_patients_from_harmonized()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO analytics.dim_patients
    SELECT * 
    EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
    FROM harmonized.patients_stream;
    RETURN 'Dim patients tables load completed';
END;
$$;

-- One line of code is intentionally missing here.

CREATE OR REPLACE TASK automation.load_dim_patients
TARGET_COMPLETION_INTERVAL = '5 MINUTES'
AFTER automation.transform_patients
WHEN SYSTEM$STREAM_HAS_DATA('harmonized.patients_stream')
AS
CALL automation.sp_load_dim_patients_from_harmonized();

ALTER TASK automation.load_dim_patients RESUME;
-- One line of code is intentionally missing here.

SELECT * FROM analytics.dim_patients;

---------------------------------------------
----Dim doctors table incremental load-------
---------------------------------------------

CREATE OR REPLACE STREAM harmonized.doctors_stream
ON TABLE harmonized.doctors
APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE automation.sp_load_dim_doctors_from_harmonized()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO analytics.dim_doctors
    SELECT * 
    EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
    FROM harmonized.doctors_stream;
    RETURN 'Dim doctors tables load completed';
END;
$$;

-- One line of code is intentionally missing here.

CREATE OR REPLACE TASK automation.load_dim_doctors
TARGET_COMPLETION_INTERVAL = '5 MINUTES'
AFTER automation.transform_doctors
WHEN SYSTEM$STREAM_HAS_DATA('harmonized.doctors_stream')
AS
CALL automation.sp_load_dim_doctors_from_harmonized();

ALTER TASK automation.load_dim_doctors RESUME;
-- One line of code is intentionally missing here.

SELECT * FROM analytics.dim_doctors;

---------------------------------------------
--Fact appointments table incremental load---
---------------------------------------------

CREATE OR REPLACE STREAM harmonized.appointments_stream
ON TABLE harmonized.appointments
APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE automation.sp_load_fact_appointments_from_harmonized()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO analytics.fact_appointments
    SELECT * 
    EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
    FROM harmonized.appointments_stream;
    RETURN 'Fact appointments tables load completed';
END;
$$;

-- One line of code is intentionally missing here.

CREATE OR REPLACE TASK automation.load_fact_appointments
TARGET_COMPLETION_INTERVAL = '5 MINUTES'
AFTER automation.transform_appointments
WHEN SYSTEM$STREAM_HAS_DATA('harmonized.appointments_stream')
AS
CALL automation.sp_load_fact_appointments_from_harmonized();

ALTER TASK automation.load_fact_appointments RESUME;
-- One line of code is intentionally missing here.

SELECT * FROM analytics.fact_appointments;

---------------------------------------------
---Fact treatments table incremental load----
---------------------------------------------

CREATE OR REPLACE STREAM harmonized.treatments_stream
ON TABLE harmonized.treatments
APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE automation.sp_load_fact_treatments_from_harmonized()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO analytics.fact_treatments
    SELECT * 
    EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
    FROM harmonized.treatments_stream;
    RETURN 'Fact treatments tables load completed';
END;
$$;

-- One line of code is intentionally missing here.

CREATE OR REPLACE TASK automation.load_fact_treatments
TARGET_COMPLETION_INTERVAL = '5 MINUTES'
AFTER automation.transform_treatments
WHEN SYSTEM$STREAM_HAS_DATA('harmonized.treatments_stream')
AS
CALL automation.sp_load_fact_treatments_from_harmonized();

ALTER TASK automation.load_fact_treatments RESUME;
-- One line of code is intentionally missing here.

SELECT * FROM analytics.fact_treatments;

---------------------------------------------
-----Fact billing table incremental load-----
---------------------------------------------

CREATE OR REPLACE STREAM harmonized.billing_stream
ON TABLE harmonized.billing
APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE automation.sp_load_fact_billing_from_harmonized()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO analytics.fact_billing
    SELECT * 
     EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
    FROM harmonized.billing_stream;
    RETURN 'Fact billing tables load completed';
END;
$$;

-- One line of code is intentionally missing here.

CREATE OR REPLACE TASK automation.load_fact_billing
TARGET_COMPLETION_INTERVAL = '5 MINUTES'
AFTER automation.transform_billing
WHEN SYSTEM$STREAM_HAS_DATA('harmonized.billing_stream')
AS
CALL automation.sp_load_fact_billing_from_harmonized();

ALTER TASK automation.load_fact_billing RESUME;
-- One line of code is intentionally missing here.

SELECT * FROM analytics.fact_billing;

---------------------------------------------
---------------Create views------------------
---------------------------------------------

-- ============================================================
-- View: v_doctor_count_by_specialization
-- Description: This view calculates the total number of doctors per specialization.
-- ============================================================

CREATE OR REPLACE VIEW analytics.v_doctor_count_by_specialization AS
SELECT 
    dr.specialization,
    COUNT(dr.specialization) AS doctor_count
FROM dim_doctors dr
GROUP BY dr.specialization;

SELECT * FROM analytics.v_doctor_count_by_specialization;

-- ============================================================
-- View: v_patient_treatment_details
-- Description: This view provides detailed information about patient treatments.
-- ============================================================

CREATE OR REPLACE VIEW analytics.v_patient_treatment_details AS
SELECT
    p.first_name || ' ' || p.last_name AS patient_name,
    app.appointment_date,
    tr.treatment_type,
    tr.description AS treatment_description,
    b.amount AS treatment_cost
FROM analytics.fact_treatments tr
JOIN analytics.fact_appointments app ON app.appointment_id = tr.appointment_id
JOIN analytics.fact_billing b ON b.treatment_id = tr.treatment_id
JOIN analytics.dim_patients p ON p.patient_id = b.patient_id; 

SELECT * FROM analytics.v_patient_treatment_details;

-- ============================================================
-- View: v_avg_treatment_cost_by_type
-- Description: This view calculates the average cost per treatment type.
-- ============================================================

CREATE OR REPLACE VIEW analytics.v_avg_treatment_cost_by_type AS
SELECT 
    tr.treatment_type,
    ROUND(AVG(b.amount),2) AS avg_treatment_cost
FROM analytics.fact_treatments tr
JOIN analytics.fact_billing b ON b.treatment_id = tr.treatment_id
GROUP BY tr.treatment_type
ORDER BY avg_treatment_cost DESC;

SELECT * FROM analytics.v_avg_treatment_cost_by_type;
