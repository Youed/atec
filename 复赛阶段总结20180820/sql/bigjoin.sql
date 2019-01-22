DROP TABLE IF EXISTS tmp_atec_test_join;
CREATE TABLE tmp_atec_test_join AS
SELECT
		t1.event_id
	   ,t1.user_id
	   ,t1.gmt_occur_dt
	   ,t1.client_ip
	   ,t1.network
	   ,t1.device_sign
	   ,t1.info_1
	   ,t1.info_2
	   ,t1.ip_prov
	   ,t1.ip_city
	   ,t1.cert_prov
	   ,t1.cert_city
	   ,t1.is_one_people
	   ,t1.mobile_oper_platform
	   ,t1.operation_channel
	   ,t1.pay_scene
	   ,t1.version
	   ,t1.amt
	   ,t1.opposing_id
       ,t2.event_id AS event_id2
	   ,t2.user_id AS user_id2
	   ,t2.gmt_occur_dt AS gmt_occur_dt2
	   ,t2.client_ip AS client_ip2
	   ,t2.network AS network2
	   ,t2.device_sign AS device_sign2
	   ,t2.info_1 AS info_12
	   ,t2.info_2 AS info_22
	   ,t2.ip_prov AS ip_prov2
	   ,t2.ip_city AS ip_city2
	   ,t2.cert_prov AS cert_prov2
	   ,t2.cert_city AS cert_city2
	   ,t2.is_one_people AS is_one_people2
	   ,t2.mobile_oper_platform AS mobile_oper_platform2
	   ,t2.operation_channel AS operation_channel2
	   ,t2.pay_scene AS pay_scene2
	   ,t2.version AS version2
	   ,t2.amt AS amt2
	   ,t2.opposing_id AS opposing_id2
FROM atec_test t1
	LEFT JOIN atec_test t2 on t1.user_id = t2.user_id
WHERE t1.gmt_occur_dt >= t2.gmt_occur_dt;



DROP TABLE IF EXISTS tmp_atec_train_join;
CREATE TABLE tmp_atec_train_join AS
SELECT
		t1.event_id
	   ,t1.user_id
	   ,t1.gmt_occur_dt
	   ,t1.client_ip
	   ,t1.network
	   ,t1.device_sign
	   ,t1.info_1
	   ,t1.info_2
	   ,t1.ip_prov
	   ,t1.ip_city
	   ,t1.cert_prov
	   ,t1.cert_city
	   ,t1.is_one_people
	   ,t1.mobile_oper_platform
	   ,t1.operation_channel
	   ,t1.pay_scene
	   ,t1.version
	   ,t1.amt
	   ,t1.opposing_id
       ,t1.target
 	   ,t1.is_fraud
       ,t2.event_id AS event_id2
	   ,t2.user_id AS user_id2
	   ,t2.gmt_occur_dt AS gmt_occur_dt2
	   ,t2.client_ip AS client_ip2
	   ,t2.network AS network2
	   ,t2.device_sign AS device_sign2
	   ,t2.info_1 AS info_12
	   ,t2.info_2 AS info_22
	   ,t2.ip_prov AS ip_prov2
	   ,t2.ip_city AS ip_city2
	   ,t2.cert_prov AS cert_prov2
	   ,t2.cert_city AS cert_city2
	   ,t2.is_one_people AS is_one_people2
	   ,t2.mobile_oper_platform AS mobile_oper_platform2
	   ,t2.operation_channel AS operation_channel2
	   ,t2.pay_scene AS pay_scene2
	   ,t2.version AS version2
	   ,t2.amt AS amt2
	   ,t2.opposing_id AS opposing_id2
       ,t2.target AS target2
       ,t2.is_fraud AS is_fraud2
FROM atec_train t1
	LEFT JOIN atec_train t2 on t1.user_id = t2.user_id
WHERE t1.gmt_occur_dt >= t2.gmt_occur_dt;