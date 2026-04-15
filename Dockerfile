FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        make \
        python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN make clean && make build

CMD ["sh", "-lc", "printf \"%s\\n\" \"SELECT * FROM case_basic_users WHERE id = 2;\" \"SELECT * FROM case_basic_users WHERE email = 'user1@test.com';\" > /tmp/docker_demo.sql && ./sqlsprocessor /tmp/docker_demo.sql"]
