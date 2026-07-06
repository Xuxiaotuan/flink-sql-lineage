CREATE TABLE listener_ods_user (
    id BIGINT,
    name STRING,
    birthday TIMESTAMP(3),
    score DECIMAL(10, 2)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE listener_dwd_user (
    id BIGINT,
    name_upper STRING,
    birthday_day STRING,
    score_text STRING
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO listener_dwd_user
SELECT
    id,
    UPPER(name),
    DATE_FORMAT(birthday, 'yyyyMMdd'),
    CAST(score AS STRING)
FROM listener_ods_user;
