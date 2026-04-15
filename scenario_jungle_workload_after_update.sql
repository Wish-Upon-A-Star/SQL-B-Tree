-- Checkpoint queries after generated_sql/jungle_update_1000000.sql
-- Expected examples:
-- id=1 -> pretest_pass
-- id=2 -> final_wait
-- id=3 -> rejected
-- id=777777 -> pretest_pass

SELECT id, email, phone, name, status FROM jungle_workload_users WHERE id = 1;
SELECT id, email, phone, name, status FROM jungle_workload_users WHERE id = 2;
SELECT id, email, phone, name, status FROM jungle_workload_users WHERE id = 3;
SELECT id, email, phone, name, status FROM jungle_workload_users WHERE id = 777777;
