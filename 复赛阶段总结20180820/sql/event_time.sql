DROP TABLE IF EXISTS train_event_time_delta_mapping;
CREATE TABLE train_event_time_delta_mapping AS
SELECT event_id, ln(unix_timestamp(gmt_occur_dt) - unix_timestamp(max(gmt_occur_dt2))) as event_time_delta
FROM tmp_atec_train_join
WHERE gmt_occur_dt > gmt_occur_dt2
GROUP BY event_id, gmt_occur_dt;


DROP TABLE IF EXISTS test_event_time_delta_mapping;
CREATE TABLE test_event_time_delta_mapping AS
SELECT event_id, ln(unix_timestamp(gmt_occur_dt) - unix_timestamp(max(gmt_occur_dt2))) as event_time_delta
FROM tmp_atec_test_join
WHERE gmt_occur_dt > gmt_occur_dt2
GROUP BY event_id, gmt_occur_dt;


DROP TABLE IF EXISTS train_last_gmt_occur_mapping;
CREATE TABLE train_last_gmt_occur_mapping AS
SELECT event_id, user_id, max(gmt_occur_dt2) as last_gmt_occur
FROM tmp_atec_train_join
WHERE gmt_occur_dt > gmt_occur_dt2
GROUP BY event_id, user_id;


DROP TABLE IF EXISTS test_last_gmt_occur_mapping;
CREATE TABLE test_last_gmt_occur_mapping AS
SELECT event_id, user_id, max(gmt_occur_dt2) as last_gmt_occur
FROM tmp_atec_test_join
WHERE gmt_occur_dt > gmt_occur_dt2
GROUP BY event_id, user_id;


DROP TABLE IF EXISTS train_last_events_mapping;
CREATE TABLE train_last_events_mapping AS
SELECT t1.event_id, t1.user_id, t2.event_id as last_event_id
FROM train_last_gmt_occur_mapping t1
LEFT JOIN atec_train t2 ON t1.user_id = t2.user_id AND t1.last_gmt_occur = t2.gmt_occur_dt;

select * from train_ip_prov_residual_time_mapping

DROP TABLE IF EXISTS test_last_events_mapping;
CREATE TABLE test_last_events_mapping AS
SELECT t1.event_id, t1.user_id, t2.event_id as last_event_id
FROM test_last_gmt_occur_mapping t1
LEFT JOIN atec_test t2 ON t1.user_id = t2.user_id AND t1.last_gmt_occur = t2.gmt_occur_dt;


DROP TABLE IF EXISTS ip_prov_delta_time_mapping;
CREATE TABLE ip_prov_delta_time_mapping AS
SELECT ip_prov_from, ip_prov_to, AVG(ip_prov_delta_time) as mean_ip_prov_delta_time
FROM
(
    SELECT (unix_timestamp(t1.gmt_occur_dt) - unix_timestamp(t1.gmt_occur_dt2)) as ip_prov_delta_time, t1.ip_prov as ip_prov_to, t1.ip_prov2 as ip_prov_from
    FROM tmp_atec_train_join t1 LEFT JOIN train_last_gmt_occur_mapping t2 ON t1.event_id = t2.event_id
    WHERE is_fraud = 0 and is_fraud2 = 0 and t2.last_gmt_occur = t1.gmt_occur_dt2
) tmp
GROUP BY ip_prov_from, ip_prov_to
;

DROP TABLE IF EXISTS train_ip_prov_residual_time_mapping;
CREATE TABLE train_ip_prov_residual_time_mapping AS
SELECT event_id, avg(ip_prov_residual_time) as ip_prov_residual_time
FROM
(
    SELECT event_id, user_id, last_event_id, ip_prov_to, ip_prov_from,
    case 
    when abs(ip_prov_delta_time - mean_ip_prov_delta_time) is null then 0
    when abs(ip_prov_delta_time - mean_ip_prov_delta_time) = 0 then 0
    when abs(ip_prov_delta_time - mean_ip_prov_delta_time) > 0 then ln(abs(ip_prov_delta_time - mean_ip_prov_delta_time)) 
    end as ip_prov_residual_time
    FROM
        (
        SELECT
            t1.event_id, t1.user_id, t3.event_id as last_event_id, t1.ip_prov as ip_prov_to,
            t3.ip_prov as ip_prov_from, t4.mean_ip_prov_delta_time, (unix_timestamp(t1.gmt_occur_dt) - unix_timestamp(t3.gmt_occur_dt)) as ip_prov_delta_time
        FROM atec_train t1
        left join train_last_events_mapping t2 on t1.event_id = t2.event_id
        left join atec_train t3 on t2.last_event_id = t3.event_id
        left join ip_prov_delta_time_mapping t4 on t1.ip_prov = t4.ip_prov_to and t3.ip_prov = t4.ip_prov_from
        ) tmp
) tmp2
GROUP BY event_id;


DROP TABLE IF EXISTS test_ip_prov_residual_time_mapping;
CREATE TABLE test_ip_prov_residual_time_mapping AS
SELECT event_id, avg(ip_prov_residual_time) as ip_prov_residual_time
FROM
(
    SELECT event_id, user_id, last_event_id, ip_prov_to, ip_prov_from,
    case 
    when abs(ip_prov_delta_time - mean_ip_prov_delta_time) is null then 0
    when abs(ip_prov_delta_time - mean_ip_prov_delta_time) = 0 then 0
    when abs(ip_prov_delta_time - mean_ip_prov_delta_time) > 0 then ln(abs(ip_prov_delta_time - mean_ip_prov_delta_time)) 
    end as ip_prov_residual_time
    FROM
        (
        SELECT
            t1.event_id, t1.user_id, t3.event_id as last_event_id, t1.ip_prov as ip_prov_to,
            t3.ip_prov as ip_prov_from, t4.mean_ip_prov_delta_time, (unix_timestamp(t1.gmt_occur_dt) - unix_timestamp(t3.gmt_occur_dt)) as ip_prov_delta_time
        FROM atec_test t1
        left join test_last_events_mapping t2 on t1.event_id = t2.event_id
        left join atec_test t3 on t2.last_event_id = t3.event_id
        left join ip_prov_delta_time_mapping t4 on t1.ip_prov = t4.ip_prov_to and t3.ip_prov = t4.ip_prov_from
        ) tmp
) tmp2
GROUP BY event_id