-- =====================================================
-- Optional schema (recommended)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS hospital;
SET search_path TO hospital;

-- =====================================================
-- PATIENTS
-- =====================================================
CREATE TABLE patients (
    patient_id         INTEGER PRIMARY KEY,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    gender             VARCHAR(20),
    date_of_birth      DATE,
    contact_number     VARCHAR(50),
    address            TEXT,
    registration_date  DATE,
    insurance_provider VARCHAR(150),
    insurance_number   VARCHAR(100),
    email              VARCHAR(150)
);

-- =====================================================
-- DOCTORS
-- =====================================================
CREATE TABLE doctors (
    doctor_id         INTEGER PRIMARY KEY,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    specialization    VARCHAR(150),
    phone_number      VARCHAR(50),
    years_experience  INTEGER,
    hospital_branch   VARCHAR(150),
    email             VARCHAR(150)
);

-- =====================================================
-- APPOINTMENTS
-- =====================================================
CREATE TABLE appointments (
    appointment_id    VARCHAR(10) PRIMARY KEY,
    patient_id        VARCHAR(10) NOT NULL,
    doctor_id         VARCHAR(10) NOT NULL,
    appointment_date  DATE,
    appointment_time  TIME,
    reason_for_visit  TEXT,
    status            VARCHAR(50),

    CONSTRAINT fk_appt_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),

    CONSTRAINT fk_appt_doctor
        FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id)
);


-- =====================================================
-- TREATMENTS
-- =====================================================
CREATE TABLE treatments (
    treatment_id      INTEGER PRIMARY KEY,
    appointment_id    INTEGER NOT NULL,
    treatment_type    VARCHAR(150),
    description       TEXT,
    cost              NUMERIC(12,2),
    treatment_date    DATE,

    CONSTRAINT fk_treatment_appt
        FOREIGN KEY (appointment_id)
        REFERENCES appointments(appointment_id)
);

-- =====================================================
-- BILLING
-- =====================================================
CREATE TABLE billing (
    bill_id          INTEGER PRIMARY KEY,
    patient_id       INTEGER NOT NULL,
    treatment_id     INTEGER NOT NULL,
    bill_date        DATE,
    amount           NUMERIC(12,2),
    payment_method   VARCHAR(50),
    payment_status   VARCHAR(50),

    CONSTRAINT fk_bill_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),

    CONSTRAINT fk_bill_treatment
        FOREIGN KEY (treatment_id)
        REFERENCES treatments(treatment_id)
);
