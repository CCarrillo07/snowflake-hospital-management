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

CREATE TABLE raw.appointments (
    appointment_id    VARCHAR(10),
    patient_id        VARCHAR(10),
    doctor_id         VARCHAR(10),
    appointment_date  VARCHAR(10),
    appointment_time  VARCHAR(20),
    reason_for_visit  VARCHAR(255),
    status            VARCHAR(50)
);

CREATE TABLE raw.treatments (
    treatment_id   VARCHAR(10),
    appointment_id VARCHAR(10),
    treatment_type VARCHAR(150),
    description    VARCHAR(255),
    treatment_date VARCHAR(10)
);

CREATE TABLE raw.billing (
    bill_id        VARCHAR(10),
    patient_id     VARCHAR(10),
    treatment_id   VARCHAR(10),
    bill_date      VARCHAR(10),
    amount         VARCHAR(20),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50)
);
