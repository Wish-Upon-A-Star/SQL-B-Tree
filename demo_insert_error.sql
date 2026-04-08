-- INSERT 중복 에러와 제약조건을 시연합니다.
INSERT INTO case_basic_users VALUES (4, 'dup-pk@test.com', '010-9999', 'pw9999', 'DuplicatePk');
INSERT INTO case_basic_users VALUES (5, 'newuser@test.com', '010-5555', 'pw5555', 'DuplicateUk');
