CREATE OR REPLACE PROCEDURE proc_emp_dept_reconcile (
    p_mode                 IN VARCHAR2 DEFAULT 'FULL',      -- VALIDATE | ENRICH | ENFORCE | FULL
    p_unassigned_dept_code  IN VARCHAR2 DEFAULT 'UNASSIGNED',
    p_unassigned_dept_name  IN VARCHAR2 DEFAULT 'Unassigned Department',
    p_budget_breach_action  IN VARCHAR2 DEFAULT 'FLAG',      -- FLAG | INACTIVATE_DEPT | INACTIVATE_EMPS
    p_commit                IN VARCHAR2 DEFAULT 'Y',         -- Y | N
    p_debug                 IN VARCHAR2 DEFAULT 'Y'          -- Y | N
) AS
    ---------------------------------------------------------------------------
    -- Single-procedure implementation (no inline procedures/functions).
    -- Uses ONLY:
    --   - DEPARTMENT
    --   - EMPLOYEE
    --
    -- What it does:
    --   1) Normalize inputs + validate mode/action flags.
    --   2) Ensure an "UNASSIGNED" department exists (creates if missing).
    --   3) Reassign employees with NULL/invalid department_id to UNASSIGNED.
    --   4) Validate/Fix department.manager_id based on highest paid ACTIVE employee.
    --   5) Run employee data-quality validations (email pattern, hire_date, salary).
    --   6) Calculate payroll totals by department and enforce budget actions:
    --        - FLAG: log only
    --        - INACTIVATE_DEPT: set department.status = INACTIVE
    --        - INACTIVATE_EMPS: set employee.status = INACTIVE in breached depts
    --   7) Commit or rollback based on p_commit.
    ---------------------------------------------------------------------------
 
    -- Constants
    c_mode_validate         CONSTANT VARCHAR2(20) := 'VALIDATE';
    c_mode_enrich           CONSTANT VARCHAR2(20) := 'ENRICH';
    c_mode_enforce          CONSTANT VARCHAR2(20) := 'ENFORCE';
    c_mode_full             CONSTANT VARCHAR2(20) := 'FULL';
 
    c_action_flag           CONSTANT VARCHAR2(30) := 'FLAG';
    c_action_inact_dept     CONSTANT VARCHAR2(30) := 'INACTIVATE_DEPT';
    c_action_inact_emps     CONSTANT VARCHAR2(30) := 'INACTIVATE_EMPS';
 
    c_status_active         CONSTANT VARCHAR2(10) := 'ACTIVE';
    c_status_inactive       CONSTANT VARCHAR2(10) := 'INACTIVE';
 
    -- Normalized parameter values
    v_mode                  VARCHAR2(20);
    v_action                VARCHAR2(30);
    v_commit                VARCHAR2(1);
    v_debug                 VARCHAR2(1);
 
    -- UNASSIGNED dept id
    v_unassigned_dept_id    NUMBER(10);
 
    -- Counters for summary
    v_cnt_unassigned_created     NUMBER := 0;
    v_cnt_emp_reassigned         NUMBER := 0;
    v_cnt_mgr_fixed              NUMBER := 0;
    v_cnt_budget_breaches        NUMBER := 0;
    v_cnt_dept_inactivated       NUMBER := 0;
    v_cnt_emp_inactivated        NUMBER := 0;
 
    -- Validation counts
    v_bad_email_count        NUMBER := 0;
    v_future_hire_count      NUMBER := 0;
    v_negative_salary_count  NUMBER := 0;
 
    -- Working variables for manager fixes
    v_mgr_ok_count           NUMBER;
    v_best_mgr_id            NUMBER(10);
 
    -- Payroll/Budget arrays
    TYPE t_num_nt IS TABLE OF NUMBER;
    v_dept_ids         t_num_nt;
    v_dept_budgets     t_num_nt;
    v_dept_payrolls    t_num_nt;
 
    -- Store breached departments (as nested table for PL/SQL loops + FORALL)
    v_breach_dept_ids  t_num_nt := t_num_nt();
 
    -- Simple local flags
    v_has_error         BOOLEAN := FALSE;
 
    -- Custom exceptions
    e_invalid_mode      EXCEPTION;
    e_invalid_action    EXCEPTION;
 
BEGIN
    ---------------------------------------------------------------------------
    -- 0) Normalize inputs
    ---------------------------------------------------------------------------
    v_mode   := UPPER(TRIM(NVL(p_mode, c_mode_full)));
    v_action := UPPER(TRIM(NVL(p_budget_breach_action, c_action_flag)));
    v_commit := UPPER(TRIM(NVL(p_commit, 'Y')));
    v_debug  := UPPER(TRIM(NVL(p_debug, 'Y')));
 
    IF v_mode NOT IN (c_mode_validate, c_mode_enrich, c_mode_enforce, c_mode_full) THEN
        RAISE e_invalid_mode;
    END IF;
 
    IF v_action NOT IN (c_action_flag, c_action_inact_dept, c_action_inact_emps) THEN
        RAISE e_invalid_action;
    END IF;
 
    IF v_commit NOT IN ('Y','N') THEN
        v_commit := 'Y';
    END IF;
 
    IF v_debug NOT IN ('Y','N') THEN
        v_debug := 'Y';
    END IF;
 
    IF v_debug = 'Y' THEN
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------');
        DBMS_OUTPUT.PUT_LINE('START proc_emp_dept_reconcile');
        DBMS_OUTPUT.PUT_LINE('mode=' || v_mode || ', action=' || v_action ||
                             ', commit=' || v_commit || ', debug=' || v_debug);
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------');
    END IF;
 
    ---------------------------------------------------------------------------
    -- 1) Ensure UNASSIGNED department exists (only if ENRICH/ENFORCE/FULL)
    ---------------------------------------------------------------------------
    IF v_mode IN (c_mode_enrich, c_mode_enforce, c_mode_full) THEN
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Step 1: Ensure UNASSIGNED department exists...');
        END IF;
 
        BEGIN
            SELECT department_id
              INTO v_unassigned_dept_id
              FROM department
             WHERE department_code = p_unassigned_dept_code;
 
            IF v_debug = 'Y' THEN
                DBMS_OUTPUT.PUT_LINE('UNASSIGNED exists. department_id=' || v_unassigned_dept_id);
            END IF;
 
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                SELECT NVL(MAX(department_id), 0) + 1
                  INTO v_unassigned_dept_id
                  FROM department;
 
                INSERT INTO department (
                    department_id,
                    department_name,
                    department_code,
                    manager_id,
                    location,
                    phone_number,
                    budget,
                    status,
                    created_by,
                    created_date
                ) VALUES (
                    v_unassigned_dept_id,
                    p_unassigned_dept_name,
                    p_unassigned_dept_code,
                    NULL,
                    NULL,
                    NULL,
                    0,
                    c_status_active,
                    USER,
                    SYSDATE
                );
 
                v_cnt_unassigned_created := 1;
 
                IF v_debug = 'Y' THEN
                    DBMS_OUTPUT.PUT_LINE('UNASSIGNED created. department_id=' || v_unassigned_dept_id);
                END IF;
        END;
 
    END IF;
 
    ---------------------------------------------------------------------------
    -- 2) Reassign employees with NULL/invalid department_id to UNASSIGNED
    --    (only if ENRICH/FULL)
    ---------------------------------------------------------------------------
    IF v_mode IN (c_mode_enrich, c_mode_full) THEN
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Step 2: Reassign orphan employees to UNASSIGNED...');
        END IF;
 
        SAVEPOINT sp_reassign_orphans;
 
        UPDATE employee e
           SET e.department_id = v_unassigned_dept_id
         WHERE e.department_id IS NULL
            OR NOT EXISTS (
                SELECT 1
                  FROM department d
                 WHERE d.department_id = e.department_id
            );
 
        v_cnt_emp_reassigned := SQL%ROWCOUNT;
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Orphan employees reassigned=' || v_cnt_emp_reassigned);
        END IF;
 
    END IF;
 
    ---------------------------------------------------------------------------
    -- 3) Validate/Fix department.manager_id
    --    (only if ENRICH/FULL)
    ---------------------------------------------------------------------------
    IF v_mode IN (c_mode_enrich, c_mode_full) THEN
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Step 3: Validate/Fix department managers...');
        END IF;
 
        FOR r IN (
            SELECT d.department_id, d.manager_id, NVL(d.status, c_status_active) AS status
              FROM department d
        )
        LOOP
            -- Optional: skip fixing managers for inactive depts in ENRICH mode
            IF r.status = c_status_inactive AND v_mode = c_mode_enrich THEN
                CONTINUE;
            END IF;
 
            -- Check if manager is valid and belongs to same department
            SELECT COUNT(*)
              INTO v_mgr_ok_count
              FROM employee e
             WHERE e.employee_id = r.manager_id
               AND e.department_id = r.department_id;
 
            IF r.manager_id IS NULL OR v_mgr_ok_count = 0 THEN
 
                -- Pick highest paid ACTIVE employee in that department
                BEGIN
                    SELECT x.employee_id
                      INTO v_best_mgr_id
                      FROM (
                          SELECT e.employee_id
                            FROM employee e
                           WHERE e.department_id = r.department_id
                             AND NVL(e.status, c_status_active) = c_status_active
                           ORDER BY NVL(e.salary, 0) DESC,
                                    e.hire_date ASC,
                                    e.employee_id ASC
                      ) x
                     WHERE ROWNUM = 1;
 
                    UPDATE department d
                       SET d.manager_id = v_best_mgr_id
                     WHERE d.department_id = r.department_id;
 
                    IF SQL%ROWCOUNT > 0 THEN
                        v_cnt_mgr_fixed := v_cnt_mgr_fixed + 1;
                        IF v_debug = 'Y' THEN
                            DBMS_OUTPUT.PUT_LINE('Dept ' || r.department_id ||
                                                 ' manager fixed => ' || v_best_mgr_id);
                        END IF;
                    END IF;
 
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        -- No eligible employees in that department; manager stays NULL
                        UPDATE department d
                           SET d.manager_id = NULL
                         WHERE d.department_id = r.department_id;
 
                        IF v_debug = 'Y' THEN
                            DBMS_OUTPUT.PUT_LINE('Dept ' || r.department_id ||
                                                 ' has no eligible employees; manager set NULL');
                        END IF;
                END;
 
            END IF;
        END LOOP;
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Managers fixed count=' || v_cnt_mgr_fixed);
        END IF;
 
    END IF;
 
    ---------------------------------------------------------------------------
    -- 4) Employee data quality validations (only if VALIDATE/FULL)
    --    No updates here; only counts/logging.
    ---------------------------------------------------------------------------
    IF v_mode IN (c_mode_validate, c_mode_full) THEN
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Step 4: Validate employee data quality...');
        END IF;
 
        SELECT COUNT(*)
          INTO v_bad_email_count
          FROM employee
         WHERE email IS NULL
            OR NOT REGEXP_LIKE(email,
                 '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
 
        SELECT COUNT(*)
          INTO v_future_hire_count
          FROM employee
         WHERE hire_date > SYSDATE;
 
        SELECT COUNT(*)
          INTO v_negative_salary_count
          FROM employee
         WHERE NVL(salary, 0) < 0;
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Bad emails=' || v_bad_email_count);
            DBMS_OUTPUT.PUT_LINE('Future hire dates=' || v_future_hire_count);
            DBMS_OUTPUT.PUT_LINE('Negative salaries=' || v_negative_salary_count);
        END IF;
 
    END IF;
 
    ---------------------------------------------------------------------------
    -- 5) Budget enforcement (only if ENFORCE/FULL)
    --    Compute payroll totals by dept, compare with budget, apply action.
    ---------------------------------------------------------------------------
    IF v_mode IN (c_mode_enforce, c_mode_full) THEN
 
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('Step 5: Payroll vs Budget enforcement...');
        END IF;
 
        -- Collect all departments with budget and payroll (ACTIVE payroll only)
        SELECT d.department_id,
               NVL(d.budget, 0) AS budget,
               NVL((
                   SELECT SUM(NVL(e.salary, 0))
                     FROM employee e
                    WHERE e.department_id = d.department_id
                      AND NVL(e.status, c_status_active) = c_status_active
               ), 0) AS payroll
          BULK COLLECT INTO v_dept_ids, v_dept_budgets, v_dept_payrolls
          FROM department d;
 
        IF v_dept_ids.COUNT = 0 THEN
            IF v_debug = 'Y' THEN
                DBMS_OUTPUT.PUT_LINE('No departments found. Skipping enforcement.');
            END IF;
        ELSE
            -- Identify breached departments
            v_breach_dept_ids.DELETE;
 
            FOR i IN 1 .. v_dept_ids.COUNT LOOP
                IF v_dept_payrolls(i) > v_dept_budgets(i) AND v_dept_budgets(i) >= 0 THEN
                    v_breach_dept_ids.EXTEND;
                    v_breach_dept_ids(v_breach_dept_ids.LAST) := v_dept_ids(i);
                    v_cnt_budget_breaches := v_cnt_budget_breaches + 1;
 
                    IF v_debug = 'Y' THEN
                        DBMS_OUTPUT.PUT_LINE(
                            'BUDGET BREACH dept_id=' || v_dept_ids(i) ||
                            ' payroll=' || v_dept_payrolls(i) ||
                            ' budget='  || v_dept_budgets(i)
                        );
                    END IF;
                END IF;
            END LOOP;
 
            IF v_breach_dept_ids.COUNT = 0 THEN
                IF v_debug = 'Y' THEN
                    DBMS_OUTPUT.PUT_LINE('No budget breaches detected.');
                END IF;
            ELSE
                -- Apply action
                IF v_action = c_action_flag THEN
                    IF v_debug = 'Y' THEN
                        DBMS_OUTPUT.PUT_LINE('Action=FLAG. No status updates applied.');
                    END IF;
 
                ELSIF v_action = c_action_inact_dept THEN
                    IF v_debug = 'Y' THEN
                        DBMS_OUTPUT.PUT_LINE('Action=INACTIVATE_DEPT. Inactivating breached departments...');
                    END IF;
 
                    SAVEPOINT sp_inact_dept;
 
                    FORALL j IN 1 .. v_breach_dept_ids.COUNT
                        UPDATE department d
                           SET d.status = c_status_inactive
                         WHERE d.department_id = v_breach_dept_ids(j)
                           AND NVL(d.status, c_status_active) <> c_status_inactive;
 
                    v_cnt_dept_inactivated := SQL%ROWCOUNT;
 
                    IF v_debug = 'Y' THEN
                        DBMS_OUTPUT.PUT_LINE('Departments inactivated=' || v_cnt_dept_inactivated);
                    END IF;
 
                ELSIF v_action = c_action_inact_emps THEN
                    IF v_debug = 'Y' THEN
                        DBMS_OUTPUT.PUT_LINE('Action=INACTIVATE_EMPS. Inactivating employees in breached departments...');
                    END IF;
 
                    SAVEPOINT sp_inact_emps;
 
                    -- Update employees dept-by-dept to avoid SQL TABLE() on PL/SQL collections
                    FOR j IN 1 .. v_breach_dept_ids.COUNT LOOP
                        UPDATE employee e
                           SET e.status = c_status_inactive
                         WHERE e.department_id = v_breach_dept_ids(j)
                           AND NVL(e.status, c_status_active) <> c_status_inactive;
 
                        v_cnt_emp_inactivated := v_cnt_emp_inactivated + SQL%ROWCOUNT;
 
                        IF v_debug = 'Y' THEN
                            DBMS_OUTPUT.PUT_LINE('Dept ' || v_breach_dept_ids(j) ||
                                                 ' employees inactivated=' || SQL%ROWCOUNT);
                        END IF;
                    END LOOP;
 
                    IF v_debug = 'Y' THEN
                        DBMS_OUTPUT.PUT_LINE('Total employees inactivated=' || v_cnt_emp_inactivated);
                    END IF;
                END IF;
            END IF;
        END IF;
    END IF;
 
    ---------------------------------------------------------------------------
    -- 6) Summary
    ---------------------------------------------------------------------------
    IF v_debug = 'Y' THEN
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------');
        DBMS_OUTPUT.PUT_LINE('SUMMARY');
        DBMS_OUTPUT.PUT_LINE('UNASSIGNED created:      ' || v_cnt_unassigned_created);
        DBMS_OUTPUT.PUT_LINE('Employees reassigned:    ' || v_cnt_emp_reassigned);
        DBMS_OUTPUT.PUT_LINE('Managers fixed:          ' || v_cnt_mgr_fixed);
        DBMS_OUTPUT.PUT_LINE('Budget breaches found:   ' || v_cnt_budget_breaches);
        DBMS_OUTPUT.PUT_LINE('Depts inactivated:       ' || v_cnt_dept_inactivated);
        DBMS_OUTPUT.PUT_LINE('Emps inactivated:        ' || v_cnt_emp_inactivated);
        DBMS_OUTPUT.PUT_LINE('Bad emails:              ' || v_bad_email_count);
        DBMS_OUTPUT.PUT_LINE('Future hire dates:       ' || v_future_hire_count);
        DBMS_OUTPUT.PUT_LINE('Negative salaries:       ' || v_negative_salary_count);
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------');
    END IF;
 
    ---------------------------------------------------------------------------
    -- 7) Commit/Rollback
    ---------------------------------------------------------------------------
    IF v_commit = 'Y' THEN
        COMMIT;
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('COMMIT completed.');
            DBMS_OUTPUT.PUT_LINE('END proc_emp_dept_reconcile');
        END IF;
    ELSE
        ROLLBACK;
        IF v_debug = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('ROLLBACK completed (p_commit=N).');
            DBMS_OUTPUT.PUT_LINE('END proc_emp_dept_reconcile');
        END IF;
    END IF;
 
EXCEPTION
    WHEN e_invalid_mode THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20010,
            'Invalid p_mode. Use VALIDATE | ENRICH | ENFORCE | FULL');
 
    WHEN e_invalid_action THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20011,
            'Invalid p_budget_breach_action. Use FLAG | INACTIVATE_DEPT | INACTIVATE_EMPS');
 
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('BACKTRACE: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END proc_emp_dept_reconcile;
/