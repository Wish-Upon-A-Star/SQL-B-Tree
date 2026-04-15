-- B+ Tree demo: reset sample rows first.
DELETE FROM case_basic_users WHERE id = 1;
DELETE FROM case_basic_users WHERE id = 2;
DELETE FROM case_basic_users WHERE id = 3;
DELETE FROM case_basic_users WHERE id = 4;

INSERT INTO case_basic_users VALUES (1, 'admin@test.com', '010-1111', 'pass123', 'Admin');
INSERT INTO case_basic_users VALUES (2, 'user1@test.com', '010-2222', 'qwerty', 'UserOne');
INSERT INTO case_basic_users VALUES (3, 'user2@test.com', '010-3333', 'hello123', 'UserTwo');

-- Auto ID insert: id(PK) is omitted, so the executor assigns the next id.
INSERT INTO case_basic_users VALUES ('auto1@test.com', '010-5555', 'pw5555', 'AutoUser');

-- ID condition uses the in-memory B+ tree index.
SELECT * FROM case_basic_users WHERE id = 4;

-- Non-ID condition still uses a linear scan for comparison.
SELECT * FROM case_basic_users WHERE name = 'AutoUser';
