-- UPDATE constraint scenario for the split-column jungle schema.
-- Focus: duplicate UK on UPDATE, NN violation on UPDATE, PK immutability.

DELETE FROM jungle_test_users WHERE id = 1201;
DELETE FROM jungle_test_users WHERE id = 1202;
DELETE FROM jungle_test_users WHERE id = 1203;
DELETE FROM jungle_test_users WHERE id = 1204;

INSERT INTO jungle_test_users VALUES (1201, 'jungle0001201@apply.kr', '010-0012-1201', '김예준', 'sw_ai_lab', 'student', 'major_cs_grade_4', 88, 'gh_001201', 'submitted', '2026_spring');
INSERT INTO jungle_test_users VALUES (1202, 'jungle0001202@apply.kr', '010-0012-1202', '박서윤', 'game_lab', 'student', 'major_game_grade_3', 73, 'gh_001202', 'pretest_pass', '2026_spring');
INSERT INTO jungle_test_users VALUES (1203, 'jungle0001203@apply.kr', '010-0012-1203', '이현우', 'game_tech_lab', 'incumbent', 'frontend_2y', 79, 'gh_001203', 'interview_wait', '2026_spring');
INSERT INTO jungle_test_users VALUES (1204, 'jungle0001204@apply.kr', '010-0012-1204', '최서진', 'sw_ai_lab', 'switcher', 'pm_3y', 92, 'gh_001204', 'final_wait', '2026_spring');

-- 1. Duplicate UK updates should fail and leave the target row unchanged.
UPDATE jungle_test_users SET email = 'jungle0001201@apply.kr' WHERE id = 1202;
UPDATE jungle_test_users SET phone = '010-0012-1201' WHERE id = 1203;

-- 2. NN update to empty string should fail.
UPDATE jungle_test_users SET track = '' WHERE id = 1204;

-- 3. PK column must not be updatable.
UPDATE jungle_test_users SET id = 9999 WHERE id = 1201;

-- 4. Verify rows still keep their original values after failed UPDATEs.
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE id = 1202;
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE id = 1203;
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE id = 1204;
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE id = 1201;

-- 5. A later valid UK update should still succeed after the failed attempts.
UPDATE jungle_test_users SET email = 'jungle0091202@apply.kr' WHERE id = 1202;
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE email = 'jungle0091202@apply.kr';
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE email = 'jungle0001202@apply.kr';

-- 6. UK WHERE + non-UK SET path should also keep working.
UPDATE jungle_test_users SET status = 'final_pass' WHERE email = 'jungle0091202@apply.kr';
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE id = 1202;

-- 7. Cleanup so the scenario stays easier to rerun.
DELETE FROM jungle_test_users WHERE id = 1201;
DELETE FROM jungle_test_users WHERE id = 1202;
DELETE FROM jungle_test_users WHERE id = 1203;
DELETE FROM jungle_test_users WHERE id = 1204;
