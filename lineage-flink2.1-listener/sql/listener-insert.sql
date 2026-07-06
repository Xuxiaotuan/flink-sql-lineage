INSERT INTO listener_dwd_user
SELECT
    id,
    UPPER(name),
    DATE_FORMAT(birthday, 'yyyyMMdd'),
    CAST(score AS STRING)
FROM listener_ods_user;
