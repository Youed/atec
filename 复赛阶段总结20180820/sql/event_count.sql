
-- 用户所有的 event 数量
DROP TABLE IF EXISTS train_user_event_count_mapping;
CREATE TABLE train_user_event_count_mapping AS
SELECT count(event_id2) as event_count, user_id, event_id
FROM tmp_atec_train_join
GROUP BY user_id, event_id;

DROP TABLE IF EXISTS test_user_event_count_mapping;
CREATE TABLE test_user_event_count_mapping AS
SELECT count(event_id2) as event_count, user_id, event_id
FROM tmp_atec_test_join
GROUP BY user_id, event_id;


-- 用户0小时以内的event数量
DROP TABLE IF EXISTS train_user_event_count_0h_mapping;
CREATE TABLE train_user_event_count_0h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_0h
FROM tmp_atec_train_join WHERE gmt_occur_dt = gmt_occur_dt2
GROUP BY user_id, event_id;

DROP TABLE IF EXISTS test_user_event_count_0h_mapping;
CREATE TABLE test_user_event_count_0h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_0h
FROM tmp_atec_test_join WHERE gmt_occur_dt = gmt_occur_dt2
GROUP BY user_id, event_id;


-- 用户1小时以内的event数量
DROP TABLE IF EXISTS train_user_event_count_1h_mapping;
CREATE TABLE train_user_event_count_1h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_1h
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600)
GROUP BY user_id, event_id;

DROP TABLE IF EXISTS test_user_event_count_1h_mapping;
CREATE TABLE test_user_event_count_1h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_1h
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600)
GROUP BY user_id, event_id;


-- 用户6小时以内的event数量
DROP TABLE IF EXISTS train_user_event_count_6h_mapping;
CREATE TABLE train_user_event_count_6h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_6h
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 6)
GROUP BY user_id, event_id;

DROP TABLE IF EXISTS test_user_event_count_6h_mapping;
CREATE TABLE test_user_event_count_6h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_6h
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 6)
GROUP BY user_id, event_id;


-- 用户12小时以内的event数量
DROP TABLE IF EXISTS train_user_event_count_12h_mapping;
CREATE TABLE train_user_event_count_12h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_12h
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 12)
GROUP BY user_id, event_id;

DROP TABLE IF EXISTS test_user_event_count_12h_mapping;
CREATE TABLE test_user_event_count_12h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_12h
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 12)
GROUP BY user_id, event_id;


-- 用户24小时以内的event数量
DROP TABLE IF EXISTS train_user_event_count_24h_mapping;
CREATE TABLE train_user_event_count_24h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_24h
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 24)
GROUP BY user_id, event_id;

DROP TABLE IF EXISTS test_user_event_count_24h_mapping;
CREATE TABLE test_user_event_count_24h_mapping AS
SELECT user_id, event_id, count(1) as user_event_count_24h
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 24)
GROUP BY user_id, event_id;



-- 用户数量特征维表
DROP TABLE IF EXISTS dim_train_user_event_count;
CREATE TABLE dim_train_user_event_count AS
SELECT
		t1.event_id,
		t2.event_count,
		t3.user_event_count_0h,
		t4.user_event_count_1h,
		t5.user_event_count_24h,
		t3.user_event_count_0h / t2.event_count AS user_event_count_0h_ratio,
		t4.user_event_count_1h / t2.event_count AS user_event_count_1h_ratio,
		t5.user_event_count_24h / t2.event_count AS user_event_count_24h_ratio
FROM atec_train t1
	LEFT JOIN train_user_event_count_mapping t2 ON t1.event_id = t2.event_id
	LEFT JOIN train_user_event_count_0h_mapping t3 ON t1.event_id = t3.event_id
	LEFT JOIN train_user_event_count_1h_mapping t4 ON t1.event_id = t4.event_id
	LEFT JOIN train_user_event_count_24h_mapping t5 ON t1.event_id = t5.event_id
;


DROP TABLE IF EXISTS dim_test_user_event_count;
CREATE TABLE dim_test_user_event_count AS
SELECT
		t1.event_id,
		t2.event_count,
		t3.user_event_count_0h,
		t4.user_event_count_1h,
		t5.user_event_count_24h,
		t3.user_event_count_0h / t2.event_count AS user_event_count_0h_ratio,
		t4.user_event_count_1h / t2.event_count AS user_event_count_1h_ratio,
		t5.user_event_count_24h / t2.event_count AS user_event_count_24h_ratio
FROM atec_test t1
	LEFT JOIN test_user_event_count_mapping t2 ON t1.event_id = t2.event_id
	LEFT JOIN test_user_event_count_0h_mapping t3 ON t1.event_id = t3.event_id
	LEFT JOIN test_user_event_count_1h_mapping t4 ON t1.event_id = t4.event_id
	LEFT JOIN test_user_event_count_24h_mapping t5 ON t1.event_id = t5.event_id
;