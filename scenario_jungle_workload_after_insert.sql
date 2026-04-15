-- Checkpoint queries after generated_sql/jungle_insert_1000000.sql

SELECT id, email, phone, name, track, status FROM jungle_workload_users WHERE id = 1;
SELECT id, email, phone, name, track, status FROM jungle_workload_users WHERE email = 'jungle0777777@apply.kr';
SELECT id, email, phone, name, track, status FROM jungle_workload_users WHERE phone = '010-0077-7777';
SELECT id, email, phone, name, track, status FROM jungle_workload_users WHERE id = 1000000;
