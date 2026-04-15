-- Jungle applicant demo on the 1,000,000-row benchmark dataset.
SELECT id, email, phone, name, track, background, history, pretest, github, status, round FROM bptree_benchmark_users WHERE id = 777777;

-- UK lookup path.
SELECT id, email, phone, name, track, pretest, status FROM bptree_benchmark_users WHERE email = 'jungle0777777@apply.kr';

-- Second UK lookup path.
SELECT id, email, phone, name, track, pretest, status FROM bptree_benchmark_users WHERE phone = '010-0077-7777';

-- Non-index column path for comparison. Names are intentionally duplicated.
SELECT id, phone, name, track, history, pretest, status FROM bptree_benchmark_users WHERE name = '김민준';
