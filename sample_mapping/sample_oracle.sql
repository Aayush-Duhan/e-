CREATE TABLE department (
    department_id        NUMBER(10)        NOT NULL,
    department_name      VARCHAR2(100)      NOT NULL,
    department_code      VARCHAR2(20)       NOT NULL,
    manager_id           NUMBER(10),
    location             VARCHAR2(100),
    phone_number         VARCHAR2(20),
    budget               NUMBER(14,2),
    status               VARCHAR2(10)       DEFAULT 'ACTIVE',
    created_by           VARCHAR2(50),
    created_date         DATE               DEFAULT SYSDATE,
 
    CONSTRAINT dept_pk PRIMARY KEY (department_id),
    CONSTRAINT dept_code_uk UNIQUE (department_code),
    CONSTRAINT dept_status_ck CHECK (status IN ('ACTIVE','INACTIVE'))
);
 
CREATE TABLE employee (
    employee_id          NUMBER(10)        NOT NULL,
    first_name           VARCHAR2(50)       NOT NULL,
    last_name            VARCHAR2(50)       NOT NULL,
    email                VARCHAR2(100)      NOT NULL,
    phone_number         VARCHAR2(20),
    hire_date            DATE               NOT NULL,
    job_title            VARCHAR2(100),
    salary               NUMBER(12,2),
    department_id        NUMBER(10),
    status               VARCHAR2(10)       DEFAULT 'ACTIVE',
 
    CONSTRAINT emp_pk PRIMARY KEY (employee_id),
    CONSTRAINT emp_email_uk UNIQUE (email),
    CONSTRAINT emp_dept_fk FOREIGN KEY (department_id)
        REFERENCES department (department_id),
    CONSTRAINT emp_status_ck CHECK (status IN ('ACTIVE','INACTIVE'))
);