-- Repeatable regression scenario for the split-column jungle schema.
-- Reset a tiny table so UPDATE/DELETE/constraint tests are safe to rerun.

DELETE FROM jungle_test_users WHERE id = 1001;
DELETE FROM jungle_test_users WHERE id = 1002;
DELETE FROM jungle_test_users WHERE id = 1003;
DELETE FROM jungle_test_users WHERE id = 1004;

INSERT INTO jungle_test_users VALUES (1001, 'jungle0001001@apply.kr', '010-0000-1001', '김민준', 'sw_ai_lab', 'student', 'major_cs_grade_3', 87, 'gh_0001001', 'interview_wait', '2026_spring');
INSERT INTO jungle_test_users VALUES (1002, 'jungle0001002@apply.kr', '010-0000-1002', '이서연', 'game_lab', 'student', 'major_design_grade_4', 72, 'gh_0001002', 'pretest_pass', '2026_spring');
INSERT INTO jungle_test_users VALUES (1003, 'jungle0001003@apply.kr', '010-0000-1003', '박지후', 'game_tech_lab', 'incumbent', 'frontend_3y', 61, 'gh_0001003', 'submitted', '2026_spring');
INSERT INTO jungle_test_users VALUES (1004, 'jungle0001004@apply.kr', '010-0000-1004', '최도윤', 'sw_ai_lab', 'switcher', 'designer_4y', 95, 'gh_0001004', 'final_wait', '2026_spring');

-- 1. PK lookup should use the B+ tree.
SELECT id, email, phone, name, track, pretest, status FROM jungle_test_users WHERE id = 1003;

-- 2. UK lookup should use the email index.
SELECT id, email, phone, name, track, pretest, status FROM jungle_test_users WHERE email = 'jungle0001003@apply.kr';

-- 3. Second UK lookup should use the phone index.
SELECT id, email, phone, name, track, pretest, status FROM jungle_test_users WHERE phone = '010-0000-1003';

-- 4. Non-index columns should use linear scan.
SELECT id, name, track, history, pretest, status FROM jungle_test_users WHERE name = '박지후';
SELECT id, name, track, pretest, status FROM jungle_test_users WHERE track = 'sw_ai_lab';
SELECT id, name, track, pretest, status FROM jungle_test_users WHERE status = 'final_wait';

-- 5. UPDATE should keep the row reachable by PK and both UK paths after rewrite.
UPDATE jungle_test_users SET status = 'final_pass' WHERE id = 1004;
SELECT id, email, phone, name, status FROM jungle_test_users WHERE id = 1004;
SELECT id, email, phone, name, status FROM jungle_test_users WHERE email = 'jungle0001004@apply.kr';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE phone = '010-0000-1004';

-- 6. DELETE should remove the row from scan and both UK paths.
DELETE FROM jungle_test_users WHERE id = 1002;
SELECT id, email, phone, name, status FROM jungle_test_users WHERE id = 1002;
SELECT id, email, phone, name, status FROM jungle_test_users WHERE email = 'jungle0001002@apply.kr';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE phone = '010-0000-1002';

-- 7. Constraint checks: duplicate PK, duplicate email, duplicate phone, NN violation should fail.
INSERT INTO jungle_test_users VALUES (1001, 'jungle9999001@apply.kr', '010-9999-0001', '중복PK', 'sw_ai_lab', 'student', 'major_ai_grade_2', 90, 'gh_dup_pk', 'submitted', '2026_spring');
INSERT INTO jungle_test_users VALUES (2001, 'jungle0001001@apply.kr', '010-9999-0002', '중복메일', 'game_lab', 'student', 'major_game_grade_2', 88, 'gh_dup_email', 'submitted', '2026_spring');
INSERT INTO jungle_test_users VALUES (2002, 'jungle9999002@apply.kr', '010-0000-1001', '중복전화', 'game_lab', 'student', 'major_game_grade_2', 88, 'gh_dup_phone', 'submitted', '2026_spring');
INSERT INTO jungle_test_users VALUES (2003, 'jungle9999003@apply.kr', '010-9999-0003', 'NN실패', '', 'student', 'major_game_grade_2', 88, 'gh_nn_fail', 'submitted', '2026_spring');

-- 8. Cleanup so the scenario stays easier to rerun.
DELETE FROM jungle_test_users WHERE id = 1001;
DELETE FROM jungle_test_users WHERE id = 1003;
DELETE FROM jungle_test_users WHERE id = 1004;
