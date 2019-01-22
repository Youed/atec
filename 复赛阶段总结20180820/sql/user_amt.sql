-- 所有的 amt sum
DROP TABLE IF EXISTS train_amt_sum_mapping;
CREATE TABLE train_amt_sum_mapping AS
SELECT sum(amt) as amt_sum, event_id
FROM tmp_atec_train_join
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_mapping;
CREATE TABLE test_amt_sum_mapping AS
SELECT sum(amt) as amt_sum, event_id
FROM tmp_atec_test_join
GROUP BY event_id;

-- 1 小时前的 amt sum
DROP TABLE IF EXISTS train_amt_sum_b1_mapping;
CREATE TABLE train_amt_sum_b1_mapping AS
SELECT sum(amt) as amt_b1_sum, event_id
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600)
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_b1_mapping;
CREATE TABLE test_amt_sum_b1_mapping AS
SELECT sum(amt) as amt_b1_sum, event_id
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600)
GROUP BY event_id;


-- 24 小时前的 amt sum
DROP TABLE IF EXISTS train_amt_sum_b24_mapping;
CREATE TABLE train_amt_sum_b24_mapping AS
SELECT sum(amt) as amt_b24_sum, event_id
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 24)
GROUP BY event_id;


DROP TABLE IF EXISTS test_amt_sum_b24_mapping;
CREATE TABLE test_amt_sum_b24_mapping AS
SELECT sum(amt) as amt_b24_sum, event_id
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 24)
GROUP BY event_id;


-- 168 小时前的 amt sum
DROP TABLE IF EXISTS train_amt_sum_b168_mapping;
CREATE TABLE train_amt_sum_b168_mapping AS
SELECT sum(amt) as amt_b168_sum, event_id
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 168)
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_b168_mapping;
CREATE TABLE test_amt_sum_b168_mapping AS
SELECT sum(amt) as amt_b168_sum, event_id
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 168)
GROUP BY event_id;


-- 648 小时前的 amt sum
DROP TABLE IF EXISTS train_amt_sum_b648_mapping;
CREATE TABLE train_amt_sum_b648_mapping AS
SELECT sum(amt) as amt_b648_sum, event_id
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 648)
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_b648_mapping;
CREATE TABLE test_amt_sum_b648_mapping AS
SELECT sum(amt) as amt_b648_sum, event_id
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) >= 3600 * 648)
GROUP BY event_id;


-- 0小时以内的 amt sum，如果出现的次数多，说明该 ip 在这个小时是频发的，越高风险越大
DROP TABLE IF EXISTS train_amt_sum_0h_mapping;
CREATE TABLE train_amt_sum_0h_mapping AS
SELECT sum(amt) as amt_0h_sum, event_id
FROM tmp_atec_train_join WHERE gmt_occur_dt = gmt_occur_dt2
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_0h_mapping;
CREATE TABLE test_amt_sum_0h_mapping AS
SELECT sum(amt) as amt_0h_sum, event_id
FROM tmp_atec_test_join WHERE gmt_occur_dt = gmt_occur_dt2
GROUP BY event_id;


-- 1小时以内的 amt sum
DROP TABLE IF EXISTS train_amt_sum_1h_mapping;
CREATE TABLE train_amt_sum_1h_mapping AS
SELECT sum(amt) as amt_1h_sum, event_id
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600)
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_1h_mapping;
CREATE TABLE test_amt_sum_1h_mapping AS
SELECT sum(amt) as amt_1h_sum, event_id
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600)
GROUP BY event_id;


-- 24小时以内的 amt sum
DROP TABLE IF EXISTS train_amt_sum_24h_mapping;
CREATE TABLE train_amt_sum_24h_mapping AS
SELECT sum(amt) as amt_24h_sum, event_id
FROM tmp_atec_train_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 24)
GROUP BY event_id;

DROP TABLE IF EXISTS test_amt_sum_24h_mapping;
CREATE TABLE test_amt_sum_24h_mapping AS
SELECT sum(amt) as amt_24h_sum, event_id
FROM tmp_atec_test_join WHERE (unix_timestamp(gmt_occur_dt) - unix_timestamp(gmt_occur_dt2) <= 3600 * 24)
GROUP BY event_id;


-- 准备 amt 特征维表
SET odps.sql.type.system.odps2 = true;

DROP TABLE IF EXISTS dim_train_amt;
CREATE TABLE dim_train_amt AS
SELECT
		t1.event_id,
		t3.amt_sum,
		t4.amt_b1_sum,
		t5.amt_b24_sum,
		t6.amt_b168_sum,
		t7.amt_b648_sum,
		t8.amt_0h_sum,
		t9.amt_1h_sum,
		t10.amt_24h_sum,
		t4.amt_b1_sum / t3.amt_sum AS amt_b1_sum_ratio,
		t5.amt_b24_sum / t3.amt_sum AS amt_b24_sum_ratio,
		t6.amt_b168_sum / t3.amt_sum AS amt_b168_sum_ratio,
		t7.amt_b648_sum / t3.amt_sum AS amt_b648_sum_ratio,
		t8.amt_0h_sum / t3.amt_sum AS amt_0h_sum_ratio,
		t9.amt_1h_sum / t3.amt_sum AS amt_1h_sum_ratio,
		t10.amt_24h_sum / t3.amt_sum AS amt_24h_sum_ratio
FROM atec_train t1
	LEFT JOIN train_amt_sum_mapping t3 ON t1.event_id = t3.event_id
	LEFT JOIN train_amt_sum_b1_mapping t4 ON t1.event_id = t4.event_id
	LEFT JOIN train_amt_sum_b24_mapping t5 ON t1.event_id = t5.event_id
	LEFT JOIN train_amt_sum_b168_mapping t6 ON t1.event_id = t6.event_id
	LEFT JOIN train_amt_sum_b648_mapping t7 ON t1.event_id = t7.event_id
	LEFT JOIN train_amt_sum_0h_mapping t8 ON t1.event_id = t8.event_id
	LEFT JOIN train_amt_sum_1h_mapping t9 ON t1.event_id = t9.event_id
	LEFT JOIN train_amt_sum_24h_mapping t10 ON t1.event_id = t10.event_id
;
    
    
DROP TABLE IF EXISTS dim_test_amt;
CREATE TABLE dim_test_amt AS
SELECT
		t1.event_id,
		t3.amt_sum,
		t4.amt_b1_sum,
		t5.amt_b24_sum,
		t6.amt_b168_sum,
		t7.amt_b648_sum,
		t8.amt_0h_sum,
		t9.amt_1h_sum,
		t10.amt_24h_sum,
		t4.amt_b1_sum / t3.amt_sum AS amt_b1_sum_ratio,
		t5.amt_b24_sum / t3.amt_sum AS amt_b24_sum_ratio,
		t6.amt_b168_sum / t3.amt_sum AS amt_b168_sum_ratio,
		t7.amt_b648_sum / t3.amt_sum AS amt_b648_sum_ratio,
		t8.amt_0h_sum / t3.amt_sum AS amt_0h_sum_ratio,
		t9.amt_1h_sum / t3.amt_sum AS amt_1h_sum_ratio,
		t10.amt_24h_sum / t3.amt_sum AS amt_24h_sum_ratio
FROM atec_test t1
	LEFT JOIN test_amt_sum_mapping t3 ON t1.event_id = t3.event_id
	LEFT JOIN test_amt_sum_b1_mapping t4 ON t1.event_id = t4.event_id
	LEFT JOIN test_amt_sum_b24_mapping t5 ON t1.event_id = t5.event_id
	LEFT JOIN test_amt_sum_b168_mapping t6 ON t1.event_id = t6.event_id
	LEFT JOIN test_amt_sum_b648_mapping t7 ON t1.event_id = t7.event_id
	LEFT JOIN test_amt_sum_0h_mapping t8 ON t1.event_id = t8.event_id
	LEFT JOIN test_amt_sum_1h_mapping t9 ON t1.event_id = t9.event_id
	LEFT JOIN test_amt_sum_24h_mapping t10 ON t1.event_id = t10.event_id
;