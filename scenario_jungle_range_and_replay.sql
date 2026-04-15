-- Range lookup + UK mutation + reopen replay scenario.
-- Reset a dedicated window of rows so the script is safe to rerun.

DELETE FROM jungle_test_users WHERE id = 1101;
DELETE FROM jungle_test_users WHERE id = 1102;
DELETE FROM jungle_test_users WHERE id = 1103;
DELETE FROM jungle_test_users WHERE id = 1104;
DELETE FROM jungle_test_users WHERE id = 1105;

INSERT INTO jungle_test_users VALUES (1101, 'jungle0001101@apply.kr', '010-0011-1101', '강하준', 'sw_ai_lab', 'student', 'major_ai_grade_3', 81, 'gh_001101', 'submitted', '2026_spring');
INSERT INTO jungle_test_users VALUES (1102, 'jungle0001102@apply.kr', '010-0011-1102', '윤서아', 'game_lab', 'student', 'major_game_grade_4', 77, 'gh_001102', 'pretest_pass', '2026_spring');
INSERT INTO jungle_test_users VALUES (1103, 'jungle0001103@apply.kr', '010-0011-1103', '정이안', 'game_tech_lab', 'incumbent', 'backend_2y', 84, 'gh_001103', 'interview_wait', '2026_spring');
INSERT INTO jungle_test_users VALUES (1104, 'jungle0001104@apply.kr', '010-0011-1104', '한지우', 'sw_ai_lab', 'switcher', 'designer_5y', 91, 'gh_001104', 'final_wait', '2026_spring');
INSERT INTO jungle_test_users VALUES (1105, 'jungle0001105@apply.kr', '010-0011-1105', '서도현', 'game_lab', 'student', 'major_cs_grade_2', 69, 'gh_001105', 'submitted', '2026_spring');

-- 1. PK/UK range SELECT should use the B+ tree range path.
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE id BETWEEN 1102 AND 1104;
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE email BETWEEN 'jungle0001102@apply.kr' AND 'jungle0001104@apply.kr';
SELECT id, email, phone, name, track, status FROM jungle_test_users WHERE phone BETWEEN '010-0011-1102' AND '010-0011-1104';

-- 2. UPDATE by UK should find the target through the UK index and keep the new UK searchable.
UPDATE jungle_test_users SET email = 'jungle9991103@apply.kr' WHERE email = 'jungle0001103@apply.kr';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE email = 'jungle9991103@apply.kr';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE email = 'jungle0001103@apply.kr';

UPDATE jungle_test_users SET phone = '010-9999-1104' WHERE phone = '010-0011-1104';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE phone = '010-9999-1104';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE phone = '010-0011-1104';

-- 3. DELETE by UK should remove the row from both exact lookup and later range results.
DELETE FROM jungle_test_users WHERE email = 'jungle0001102@apply.kr';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE id = 1102;
SELECT id, email, phone, name, status FROM jungle_test_users WHERE email = 'jungle0001102@apply.kr';

-- 4. Touch another table to force jungle_test_users to close, then reopen it.
SELECT id, email, name FROM case_basic_users WHERE id = 1;

-- 5. After reopen, delta replay should preserve the previous UPDATE/DELETE results.
SELECT id, email, phone, name, status FROM jungle_test_users WHERE email = 'jungle9991103@apply.kr';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE phone = '010-9999-1104';
SELECT id, email, phone, name, status FROM jungle_test_users WHERE id BETWEEN 1101 AND 1105;
