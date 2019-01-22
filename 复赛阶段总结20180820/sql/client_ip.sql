-- 所有的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_mapping;
CREATE TABLE train_client_ip_count_mapping AS
SELECT count(client_ip) as client_ip_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_mapping;
CREATE TABLE test_client_ip_count_mapping AS
SELECT count(client_ip) as client_ip_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2
GROUP BY event_id;

-- 1 小时前的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_b1_mapping;
CREATE TABLE train_client_ip_count_b1_mapping AS
SELECT count(client_ip) as client_ip_b1_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600)
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_b1_mapping;
CREATE TABLE test_client_ip_count_b1_mapping AS
SELECT count(client_ip) as client_ip_b1_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600)
GROUP BY event_id;


-- 24 小时前的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_b24_mapping;
CREATE TABLE train_client_ip_count_b24_mapping AS
SELECT count(client_ip) as client_ip_b24_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 24)
GROUP BY event_id;


DROP TABLE IF EXISTS test_client_ip_count_b24_mapping;
CREATE TABLE test_client_ip_count_b24_mapping AS
SELECT count(client_ip) as client_ip_b24_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 24)
GROUP BY event_id;


-- 168 小时前的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_b168_mapping;
CREATE TABLE train_client_ip_count_b168_mapping AS
SELECT count(client_ip) as client_ip_b168_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 168)
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_b168_mapping;
CREATE TABLE test_client_ip_count_b168_mapping AS
SELECT count(client_ip) as client_ip_b168_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 168)
GROUP BY event_id;


-- 648 小时前的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_b648_mapping;
CREATE TABLE train_client_ip_count_b648_mapping AS
SELECT count(client_ip) as client_ip_b648_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 648)
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_b648_mapping;
CREATE TABLE test_client_ip_count_b648_mapping AS
SELECT count(client_ip) as client_ip_b648_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 648)
GROUP BY event_id;


-- 0小时以内的 client_ip count，如果出现的次数多，说明该 ip 在这个小时是频发的，越高风险越大
DROP TABLE IF EXISTS train_client_ip_count_0h_mapping;
CREATE TABLE train_client_ip_count_0h_mapping AS
SELECT count(client_ip) as client_ip_0h_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and gmt_occur_dt = gmt_occur_dt2
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_0h_mapping;
CREATE TABLE test_client_ip_count_0h_mapping AS
SELECT count(client_ip) as client_ip_0h_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and gmt_occur_dt = gmt_occur_dt2
GROUP BY event_id;


-- 1小时以内的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_1h_mapping;
CREATE TABLE train_client_ip_count_1h_mapping AS
SELECT count(client_ip) as client_ip_1h_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600)
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_1h_mapping;
CREATE TABLE test_client_ip_count_1h_mapping AS
SELECT count(client_ip) as client_ip_1h_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600)
GROUP BY event_id;


-- 24小时以内的 client_ip count
DROP TABLE IF EXISTS train_client_ip_count_24h_mapping;
CREATE TABLE train_client_ip_count_24h_mapping AS
SELECT count(client_ip) as client_ip_24h_count, event_id
FROM tmp_atec_train_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 24)
GROUP BY event_id;

DROP TABLE IF EXISTS test_client_ip_count_24h_mapping;
CREATE TABLE test_client_ip_count_24h_mapping AS
SELECT count(client_ip) as client_ip_24h_count, event_id
FROM tmp_atec_test_join WHERE client_ip = client_ip2 and (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 24)
GROUP BY event_id;


-- 准备 client_ip 特征维表
SET odps.sql.type.system.odps2 = true;

DROP TABLE IF EXISTS dim_train_client_ip;
CREATE TABLE dim_train_client_ip AS
SELECT
		t1.event_id,
		t3.client_ip_count,
		t4.client_ip_b1_count,
		t5.client_ip_b24_count,
		t6.client_ip_b168_count,
		t7.client_ip_b648_count,
		t8.client_ip_0h_count,
		t9.client_ip_1h_count,
		t10.client_ip_24h_count,
		t3.client_ip_count / t2.event_count AS client_ip_count_ratio,
		t4.client_ip_b1_count / t2.event_count AS client_ip_b1_count_ratio,
		t5.client_ip_b24_count / t2.event_count AS client_ip_b24_count_ratio,
		t6.client_ip_b168_count / t2.event_count AS client_ip_b168_count_ratio,
		t7.client_ip_b648_count / t2.event_count AS client_ip_b648_count_ratio,
		t8.client_ip_0h_count / t2.event_count AS client_ip_0h_count_ratio,
		t9.client_ip_1h_count / t2.event_count AS client_ip_1h_count_ratio,
		t10.client_ip_24h_count / t2.event_count AS client_ip_24h_count_ratio
FROM atec_train t1
	LEFT JOIN dim_train_user_event_count t2 ON t1.event_id = t2.event_id
	LEFT JOIN train_client_ip_count_mapping t3 ON t1.event_id = t3.event_id
	LEFT JOIN train_client_ip_count_b1_mapping t4 ON t1.event_id = t4.event_id
	LEFT JOIN train_client_ip_count_b24_mapping t5 ON t1.event_id = t5.event_id
	LEFT JOIN train_client_ip_count_b168_mapping t6 ON t1.event_id = t6.event_id
	LEFT JOIN train_client_ip_count_b648_mapping t7 ON t1.event_id = t7.event_id
	LEFT JOIN train_client_ip_count_0h_mapping t8 ON t1.event_id = t8.event_id
	LEFT JOIN train_client_ip_count_1h_mapping t9 ON t1.event_id = t9.event_id
	LEFT JOIN train_client_ip_count_24h_mapping t10 ON t1.event_id = t10.event_id
;
    
    
DROP TABLE IF EXISTS dim_test_client_ip;
CREATE TABLE dim_test_client_ip AS
SELECT
		t1.event_id,
		t3.client_ip_count,
		t4.client_ip_b1_count,
		t5.client_ip_b24_count,
		t6.client_ip_b168_count,
		t7.client_ip_b648_count,
		t8.client_ip_0h_count,
		t9.client_ip_1h_count,
		t10.client_ip_24h_count,
		t3.client_ip_count / t2.event_count AS client_ip_count_ratio,
		t4.client_ip_b1_count / t2.event_count AS client_ip_b1_count_ratio,
		t5.client_ip_b24_count / t2.event_count AS client_ip_b24_count_ratio,
		t6.client_ip_b168_count / t2.event_count AS client_ip_b168_count_ratio,
		t7.client_ip_b648_count / t2.event_count AS client_ip_b648_count_ratio,
		t8.client_ip_0h_count / t2.event_count AS client_ip_0h_count_ratio,
		t9.client_ip_1h_count / t2.event_count AS client_ip_1h_count_ratio,
		t10.client_ip_24h_count / t2.event_count AS client_ip_24h_count_ratio
FROM atec_test t1
	LEFT JOIN dim_test_user_event_count t2 ON t1.event_id = t2.event_id
	LEFT JOIN test_client_ip_count_mapping t3 ON t1.event_id = t3.event_id
	LEFT JOIN test_client_ip_count_b1_mapping t4 ON t1.event_id = t4.event_id
	LEFT JOIN test_client_ip_count_b24_mapping t5 ON t1.event_id = t5.event_id
	LEFT JOIN test_client_ip_count_b168_mapping t6 ON t1.event_id = t6.event_id
	LEFT JOIN test_client_ip_count_b648_mapping t7 ON t1.event_id = t7.event_id
	LEFT JOIN test_client_ip_count_0h_mapping t8 ON t1.event_id = t8.event_id
	LEFT JOIN test_client_ip_count_1h_mapping t9 ON t1.event_id = t9.event_id
	LEFT JOIN test_client_ip_count_24h_mapping t10 ON t1.event_id = t10.event_id
;