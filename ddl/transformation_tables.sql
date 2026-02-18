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

CREATE OR REPLACE TABLE harmonized.appointments (
    appointment_id    VARCHAR(10),
    patient_id        VARCHAR(10),
    doctor_id         VARCHAR(10),
    appointment_date  DATE,
    appointment_time  TIME,
    reason_for_visit  VARCHAR(255),
    status            VARCHAR(50)
);

CREATE OR REPLACE TABLE harmonized.treatments (
   treatment_id   VARCHAR(10),
   appointment_id VARCHAR(10),
   treatment_type VARCHAR(150),
   description    VARCHAR(255),
   treatment_date DATE
);

CREATE OR REPLACE TABLE harmonized.billing (
    bill_id        VARCHAR(10),
    patient_id     VARCHAR(10),
    treatment_id   VARCHAR(10),
    bill_date      DATE,
    amount         NUMBER(10,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50)
);
