-- Auto ID insert: id(PK) is omitted, so the executor assigns the next id.
INSERT INTO case_basic_users VALUES ('auto1@test.com', '010-5555', 'pw5555', 'AutoUser');

-- ID condition uses the in-memory B+ tree index.
SELECT * FROM case_basic_users WHERE id = 4;

-- Non-ID condition still uses a linear scan for comparison.
SELECT * FROM case_basic_users WHERE name = 'AutoUser';
