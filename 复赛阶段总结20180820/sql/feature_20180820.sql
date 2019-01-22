-- 第一阶段

DROP TABLE IF EXISTS atec_train;
CREATE TABLE atec_train AS

SELECT  /*+ MAPJOIN(network_mapping, ip_prov_mapping, ip_city_mapping, cert_prov_mapping, cert_city_mapping, is_one_people_mapping, mobile_oper_platform_mapping, operation_channel_mapping) */
		a.event_id
	   ,a.user_id
       ,to_date(a.gmt_occur, 'yyyy-mm-dd HH') as gmt_occur_dt
	   ,a.client_ip
	   ,b.map_id AS network
	   ,a.device_sign
	   ,a.info_1
	   ,a.info_2
	   ,c.map_id AS ip_prov
	   ,d.map_id AS ip_city
	   ,e.map_id AS cert_prov
	   ,f.map_id AS cert_city
	   ,g.map_id AS is_one_people
	   ,h.map_id AS mobile_oper_platform
	   ,i.map_id AS operation_channel
	   ,j.map_id AS pay_scene
       ,k.map_id AS version
	   ,amt
	   ,opposing_id
	   ,CASE
            WHEN is_fraud = -1 THEN 1
            ELSE is_fraud
        END as target
       ,is_fraud
FROM atec_1000w_ins_data a
LEFT JOIN network_mapping b ON a.network = b.network
LEFT JOIN ip_prov_mapping c ON a.ip_prov = c.ip_prov
LEFT JOIN ip_city_mapping d ON a.ip_city = d.ip_city
LEFT JOIN cert_prov_mapping e ON a.cert_prov = e.cert_prov
LEFT JOIN cert_city_mapping f ON a.cert_city = f.cert_city
LEFT JOIN is_one_people_mapping g ON a.is_one_people = g.is_one_people
LEFT JOIN mobile_oper_platform_mapping h ON a.mobile_oper_platform = h.mobile_oper_platform
LEFT JOIN operation_channel_mapping i ON a.operation_channel = i.operation_channel
LEFT JOIN pay_scene_mapping j ON a.pay_scene = j.pay_scene
LEFT JOIN version_mapping k ON a.version = k.version
;


DROP TABLE IF EXISTS atec_test;
CREATE TABLE atec_test AS

SELECT  /*+ MAPJOIN(network_mapping, ip_prov_mapping, ip_city_mapping, cert_prov_mapping, cert_city_mapping, is_one_people_mapping, mobile_oper_platform_mapping, operation_channel_mapping) */
		a.event_id
	   ,a.user_id
       ,to_date(a.gmt_occur, 'yyyy-mm-dd HH') as gmt_occur_dt
	   ,a.client_ip
	   ,b.map_id AS network
	   ,a.device_sign
	   ,a.info_1
	   ,a.info_2
	   ,c.map_id AS ip_prov
	   ,d.map_id AS ip_city
	   ,e.map_id AS cert_prov
	   ,f.map_id AS cert_city
	   ,g.map_id AS is_one_people
	   ,h.map_id AS mobile_oper_platform
	   ,i.map_id AS operation_channel
	   ,j.map_id AS pay_scene
       ,k.map_id AS version
	   ,amt
	   ,opposing_id
FROM atec_1000w_ootb_data a
LEFT JOIN network_mapping b ON a.network = b.network
LEFT JOIN ip_prov_mapping c ON a.ip_prov = c.ip_prov
LEFT JOIN ip_city_mapping d ON a.ip_city = d.ip_city
LEFT JOIN cert_prov_mapping e ON a.cert_prov = e.cert_prov
LEFT JOIN cert_city_mapping f ON a.cert_city = f.cert_city
LEFT JOIN is_one_people_mapping g ON a.is_one_people = g.is_one_people
LEFT JOIN mobile_oper_platform_mapping h ON a.mobile_oper_platform = h.mobile_oper_platform
LEFT JOIN operation_channel_mapping i ON a.operation_channel = i.operation_channel
LEFT JOIN pay_scene_mapping j ON a.pay_scene = j.pay_scene
LEFT JOIN version_mapping k ON a.version = k.version;



DROP TABLE IF EXISTS atec_test_a;
CREATE TABLE atec_test_a AS

SELECT  /*+ MAPJOIN(network_mapping, ip_prov_mapping, ip_city_mapping, cert_prov_mapping, cert_city_mapping, is_one_people_mapping, mobile_oper_platform_mapping, operation_channel_mapping) */
		a.event_id
	   ,a.user_id
       ,to_date(a.gmt_occur, 'yyyy-mm-dd HH') as gmt_occur_dt
	   ,a.client_ip
	   ,b.map_id AS network
	   ,a.device_sign
	   ,a.info_1
	   ,a.info_2
	   ,c.map_id AS ip_prov
	   ,d.map_id AS ip_city
	   ,e.map_id AS cert_prov
	   ,f.map_id AS cert_city
	   ,g.map_id AS is_one_people
	   ,h.map_id AS mobile_oper_platform
	   ,i.map_id AS operation_channel
	   ,j.map_id AS pay_scene
       ,k.map_id AS version
	   ,amt
	   ,opposing_id
FROM atec_1000w_oota_data a
LEFT JOIN network_mapping b ON a.network = b.network
LEFT JOIN ip_prov_mapping c ON a.ip_prov = c.ip_prov
LEFT JOIN ip_city_mapping d ON a.ip_city = d.ip_city
LEFT JOIN cert_prov_mapping e ON a.cert_prov = e.cert_prov
LEFT JOIN cert_city_mapping f ON a.cert_city = f.cert_city
LEFT JOIN is_one_people_mapping g ON a.is_one_people = g.is_one_people
LEFT JOIN mobile_oper_platform_mapping h ON a.mobile_oper_platform = h.mobile_oper_platform
LEFT JOIN operation_channel_mapping i ON a.operation_channel = i.operation_channel
LEFT JOIN pay_scene_mapping j ON a.pay_scene = j.pay_scene
LEFT JOIN version_mapping k ON a.version = k.version;



-- 第二阶段


SET odps.sql.type.system.odps2 = true;

DROP TABLE IF EXISTS atec_train_featured;
CREATE TABLE atec_train_featured AS
SELECT
		t1.event_id
		,t1.user_id
		,t1.gmt_occur_dt
		,cast(dayofmonth(t1.gmt_occur_dt) as BIGINT) as dayofmonth
		,cast(weekday(t1.gmt_occur_dt) as BIGINT) as weekday
		,cast(hour(t1.gmt_occur_dt) as BIGINT) as hour
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
		,t2.`(event_id)?+.+`
		,t3.`(event_id)?+.+`
		,t4.`(event_id)?+.+`
		,t5.`(event_id)?+.+`
		,t6.`(event_id)?+.+`
		,t7.`(event_id)?+.+`
		,t8.`(event_id)?+.+`
		,t9.`(event_id)?+.+`
		,t10.`(event_id)?+.+`
		,t11.`(event_id)?+.+`
		,t12.`(event_id)?+.+`
		,t13.`(event_id)?+.+`
		,t14.event_time_delta
		,t15.ip_prov_residual_time
		,t1.is_fraud
		,t1.target
FROM atec_train t1
	LEFT JOIN dim_train_user_event_count t2 ON t1.event_id = t2.event_id
  LEFT JOIN dim_train_client_ip t3 ON t1.event_id = t3.event_id
  LEFT JOIN dim_train_device_sign t4 ON t1.event_id = t4.event_id
  LEFT JOIN dim_train_ip_prov t5 ON t1.event_id = t5.event_id
  LEFT JOIN dim_train_ip_city t6 ON t1.event_id = t6.event_id
  LEFT JOIN dim_train_info_1 t7 ON t1.event_id = t7.event_id
  LEFT JOIN dim_train_info_2 t8 ON t1.event_id = t8.event_id
  LEFT JOIN dim_train_pay_scene t9 ON t1.event_id = t9.event_id
  LEFT JOIN dim_train_opposing_id t10 ON t1.event_id = t10.event_id
  LEFT JOIN dim_train_amt t11 ON t1.event_id = t11.event_id
  LEFT JOIN dim_train_uniq_by_ip_prov t12 ON t1.event_id = t12.event_id
  LEFT JOIN dim_train_uniq_by_device_sign t13 ON t1.event_id = t13.event_id
  LEFT JOIN train_event_time_delta_mapping t14 ON t1.event_id = t14.event_id
  LEFT JOIN train_ip_prov_residual_time_mapping t15 ON t1.event_id = t15.event_id
;
    
    
DROP TABLE IF EXISTS atec_test_featured;
CREATE TABLE atec_test_featured AS
SELECT
		t1.event_id
		,t1.user_id
		,t1.gmt_occur_dt
		,cast(dayofmonth(t1.gmt_occur_dt) as BIGINT) as dayofmonth
		,cast(weekday(t1.gmt_occur_dt) as BIGINT) as weekday
		,cast(hour(t1.gmt_occur_dt) as BIGINT ) as hour
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
		,t2.`(event_id)?+.+`
		,t3.`(event_id)?+.+`
		,t4.`(event_id)?+.+`
		,t5.`(event_id)?+.+`
		,t6.`(event_id)?+.+`
		,t7.`(event_id)?+.+`
		,t8.`(event_id)?+.+`
		,t9.`(event_id)?+.+`
		,t10.`(event_id)?+.+`
		,t11.`(event_id)?+.+`
		,t12.`(event_id)?+.+`
		,t13.`(event_id)?+.+`
		,t14.event_time_delta
		,t15.ip_prov_residual_time
FROM atec_test t1
	LEFT JOIN dim_test_user_event_count t2 ON t1.event_id = t2.event_id
  LEFT JOIN dim_test_client_ip t3 ON t1.event_id = t3.event_id
  LEFT JOIN dim_test_device_sign t4 ON t1.event_id = t4.event_id
  LEFT JOIN dim_test_ip_prov t5 ON t1.event_id = t5.event_id
  LEFT JOIN dim_test_ip_city t6 ON t1.event_id = t6.event_id
  LEFT JOIN dim_test_info_1 t7 ON t1.event_id = t7.event_id
  LEFT JOIN dim_test_info_2 t8 ON t1.event_id = t8.event_id
  LEFT JOIN dim_test_pay_scene t9 ON t1.event_id = t9.event_id
  LEFT JOIN dim_test_opposing_id t10 ON t1.event_id = t10.event_id
  LEFT JOIN dim_test_amt t11 ON t1.event_id = t11.event_id
  LEFT JOIN dim_test_uniq_by_ip_prov t12 ON t1.event_id = t12.event_id
  LEFT JOIN dim_test_uniq_by_device_sign t13 ON t1.event_id = t13.event_id
  LEFT JOIN test_event_time_delta_mapping t14 ON t1.event_id = t14.event_id
  LEFT JOIN test_ip_prov_residual_time_mapping t15 ON t1.event_id = t15.event_id
;



-- 第三阶段



DROP TABLE IF EXISTS atec_train_featured_20180820;
CREATE TABLE atec_train_featured_20180820 AS
SELECT
		t1.event_id
		,user_id
		,t1.gmt_occur_dt
		,dayofmonth
		,hour
		,t1.client_ip
		,network
		,t1.device_sign
		,info_1
		,info_2
		,ip_prov
		,ip_city
		,cert_prov
		,cert_city
		,mobile_oper_platform
		,operation_channel
		,pay_scene
		,version
		,amt
		,opposing_id
		,if(floor(ln(event_count)) > 9, 9, floor(ln(event_count))) as event_count_level
		,if(floor(ln(user_event_count_1h)) > 6, 6, floor(ln(user_event_count_1h))) as user_event_count_1h_level

		,if(floor(ln(client_ip_b1_count)) > 9, 9, floor(ln(client_ip_b1_count))) as client_ip_b1_level
		,if(floor(ln(client_ip_b24_count)) > 9, 9, floor(ln(client_ip_b24_count))) as client_ip_b24_level
		,if(floor(ln(client_ip_0h_count)) > 6, 6, floor(ln(client_ip_0h_count))) as client_ip_0h_count_level
		,if(floor(ln(client_ip_1h_count)) > 6, 6, floor(ln(client_ip_1h_count))) as client_ip_1h_count_level
		,if(floor(ln(client_ip_24h_count)) > 7, 7, floor(ln(client_ip_24h_count))) as client_ip_24h_count_level
		,client_ip_b1_count_ratio
		,client_ip_b24_count_ratio
		,client_ip_0h_count_ratio
		,client_ip_1h_count_ratio
		,client_ip_24h_count_ratio

		,if(floor(ln(device_sign_count)) > 9, 9, floor(ln(device_sign_count))) as device_sign_count_level
		,if(floor(ln(device_sign_b1_count)) > 9, 9, floor(ln(device_sign_b1_count))) as device_sign_b1_count_level
		,if(floor(ln(device_sign_b24_count)) > 9, 9, floor(ln(device_sign_b24_count))) as device_sign_b24_count_level
		,if(floor(ln(device_sign_0h_count)) > 6, 6, floor(ln(device_sign_0h_count))) as device_sign_0h_count_level
		,if(floor(ln(device_sign_1h_count)) > 6, 6, floor(ln(device_sign_1h_count))) as device_sign_1h_count_level
		,if(floor(ln(device_sign_24h_count)) > 7, 7, floor(ln(device_sign_24h_count))) as device_sign_24h_count_level
		,device_sign_count_ratio
		,device_sign_b1_count_ratio
		,device_sign_b24_count_ratio
		,device_sign_0h_count_ratio
		,device_sign_1h_count_ratio
		,device_sign_24h_count_ratio

		,if(floor(ln(ip_prov_count)) > 9, 9, floor(ln(ip_prov_count))) as ip_prov_count_level
		,if(floor(ln(ip_prov_b1_count)) > 9, 9, floor(ln(ip_prov_b1_count))) as ip_prov_b1_count_level
		,if(floor(ln(ip_prov_b24_count)) > 9, 9, floor(ln(ip_prov_b24_count))) as ip_prov_b24_count_level
		,if(floor(ln(ip_prov_0h_count)) > 6, 6, floor(ln(ip_prov_0h_count))) as ip_prov_0h_count_level
		,if(floor(ln(ip_prov_1h_count)) > 6, 6, floor(ln(ip_prov_1h_count))) as ip_prov_1h_count_level
		,if(floor(ln(ip_prov_24h_count)) > 6, 6, floor(ln(ip_prov_24h_count))) as ip_prov_24h_count_level
		,ip_prov_count_ratio
		,ip_prov_b1_count_ratio
		,ip_prov_b24_count_ratio
		,ip_prov_0h_count_ratio
		,ip_prov_1h_count_ratio
		,ip_prov_24h_count_ratio

		,if(floor(ln(ip_city_count)) > 9, 9, floor(ln(ip_city_count))) as ip_city_count_level
		,if(floor(ln(ip_city_b1_count)) > 9, 9, floor(ln(ip_city_b1_count))) as ip_city_b1_count_level
		,if(floor(ln(ip_city_b24_count)) > 9, 9, floor(ln(ip_city_b24_count))) as ip_city_b24_count_level
		,if(floor(ln(ip_city_0h_count)) > 6, 6, floor(ln(ip_city_0h_count))) as ip_city_0h_count_level
		,if(floor(ln(ip_city_1h_count)) > 6, 6, floor(ln(ip_city_1h_count))) as ip_city_1h_count_level
		,if(floor(ln(ip_city_24h_count)) > 7, 7, floor(ln(ip_city_24h_count))) as ip_city_24h_count_level
		,ip_city_count_ratio
		,ip_city_b1_count_ratio
		,ip_city_b24_count_ratio
		,ip_city_0h_count_ratio
		,ip_city_1h_count_ratio
		,ip_city_24h_count_ratio

		,if(floor(ln(info_1_count)) > 8, 8, floor(ln(info_1_count))) as info_1_count_level
		,if(floor(ln(info_1_0h_count)) > 6, 6, floor(ln(info_1_0h_count))) as info_1_0h_count_level
		,if(floor(ln(info_1_1h_count)) > 6, 6, floor(ln(info_1_1h_count))) as info_1_1h_count_level
		,if(floor(ln(info_1_24h_count)) > 7, 7, floor(ln(info_1_24h_count))) as info_1_24h_count_level
		,info_1_count_ratio
		,info_1_0h_count_ratio
		,info_1_1h_count_ratio
		,info_1_24h_count_ratio

		,if(floor(ln(info_2_count)) > 8, 8, floor(ln(info_2_count))) as info_2_count_level
		,if(floor(ln(info_2_0h_count)) > 6, 6, floor(ln(info_2_0h_count))) as info_2_0h_count_level
		,if(floor(ln(info_2_1h_count)) > 6, 6, floor(ln(info_2_1h_count))) as info_2_1h_count_level
		,if(floor(ln(info_2_24h_count)) > 7, 7, floor(ln(info_2_24h_count))) as info_2_24h_count_level
		,info_2_count_ratio
		,info_2_0h_count_ratio
		,info_2_1h_count_ratio
		,info_2_24h_count_ratio

		,if(floor(ln(pay_scene_count)) > 9, 9, floor(ln(pay_scene_count))) as pay_scene_count_level
		,if(floor(ln(pay_scene_b24_count)) > 9, 9, floor(ln(pay_scene_b24_count))) as pay_scene_b24_count_level
		,if(floor(ln(pay_scene_0h_count)) > 6, 6, floor(ln(pay_scene_0h_count))) as pay_scene_0h_count_level
		,if(floor(ln(pay_scene_1h_count)) > 6, 6, floor(ln(pay_scene_1h_count))) as pay_scene_1h_count_level
		,if(floor(ln(pay_scene_24h_count)) > 7, 7, floor(ln(pay_scene_24h_count))) as pay_scene_24h_count_level
		,pay_scene_count_ratio
		,pay_scene_b24_count_ratio
		,pay_scene_0h_count_ratio
		,pay_scene_1h_count_ratio
		,pay_scene_24h_count_ratio

		,if(floor(ln(opposing_id_count)) > 9, 9, floor(ln(opposing_id_count))) as opposing_id_count_level
		,if(floor(ln(opposing_id_0h_count)) > 6, 6, floor(ln(opposing_id_0h_count))) as opposing_id_0h_count_level
		,if(floor(ln(opposing_id_1h_count)) > 6, 6, floor(ln(opposing_id_1h_count))) as opposing_id_1h_count_level
		,if(floor(ln(opposing_id_24h_count)) > 7, 7, floor(ln(opposing_id_24h_count))) as opposing_id_24h_count_level
		,if(floor(ln(opposing_id_b1_count)) > 9, 9, floor(ln(opposing_id_b1_count))) as opposing_id_b1_count_level
		,if(floor(ln(opposing_id_b24_count)) > 9, 9, floor(ln(opposing_id_b24_count))) as opposing_id_b24_count_level
		,opposing_id_count_ratio
		,opposing_id_0h_count_ratio
		,opposing_id_1h_count_ratio
		,opposing_id_24h_count_ratio
		,opposing_id_b1_count_ratio
		,opposing_id_b24_count_ratio

		,if(floor(ln(amt_sum)) > 16, 16, floor(ln(amt_sum))) as amt_sum_level
		,if(floor(ln(amt_b24_sum)) > 16, 16, floor(ln(amt_b24_sum))) as amt_b24_sum_level
		,if(floor(ln(amt_b168_sum)) > 16, 16, floor(ln(amt_b168_sum))) as amt_b168_sum_level
		,if(floor(ln(amt_0h_sum)) > 13, 13, floor(ln(amt_0h_sum))) as amt_0h_sum_level
		,if(floor(ln(amt_1h_sum)) > 13, 13, floor(ln(amt_1h_sum))) as amt_1h_sum_level
		,if(floor(ln(amt_24h_sum)) > 13, 13, floor(ln(amt_24h_sum))) as amt_24h_sum_level
		,amt_b24_sum_ratio
		,amt_b168_sum_ratio
		,amt_0h_sum_ratio
		,amt_1h_sum_ratio
		,amt_24h_sum_ratio

		,uniq_network_by_ip_prov
		,uniq_device_sign_by_ip_prov
		,uniq_event_by_ip_prov
		,uniq_pay_scene_by_device_sign
		,uniq_opposing_id_by_device_sign

		,floor(event_time_delta) as event_time_delta
		,floor(ip_prov_residual_time) as ip_prov_residual_time

		,t2.user_cnt as user_cnt_by_device_sign
		,t3.user_cnt as user_cnt_by_client_ip

		,is_fraud
		,target
FROM atec_train_featured t1
	LEFT JOIN train_user_count_by_device_sign_mapping t2 ON t1.event_id = t2.event_id
	LEFT JOIN train_user_count_by_client_ip_mapping t3 ON t1.event_id = t3.event_id
;
    
    
DROP TABLE IF EXISTS atec_test_featured_20180820;
CREATE TABLE atec_test_featured_20180820 AS
SELECT
		t1.event_id
		,user_id
		,t1.gmt_occur_dt
		,dayofmonth
		,hour
		,t1.client_ip
		,network
		,t1.device_sign
		,info_1
		,info_2
		,ip_prov
		,ip_city
		,cert_prov
		,cert_city
		,mobile_oper_platform
		,operation_channel
		,pay_scene
		,version
		,amt
		,opposing_id
		,if(floor(ln(event_count)) > 9, 9, floor(ln(event_count))) as event_count_level
		,if(floor(ln(user_event_count_1h)) > 6, 6, floor(ln(user_event_count_1h))) as user_event_count_1h_level

		,if(floor(ln(client_ip_b1_count)) > 9, 9, floor(ln(client_ip_b1_count))) as client_ip_b1_level
		,if(floor(ln(client_ip_b24_count)) > 9, 9, floor(ln(client_ip_b24_count))) as client_ip_b24_level
		,if(floor(ln(client_ip_0h_count)) > 6, 6, floor(ln(client_ip_0h_count))) as client_ip_0h_count_level
		,if(floor(ln(client_ip_1h_count)) > 6, 6, floor(ln(client_ip_1h_count))) as client_ip_1h_count_level
		,if(floor(ln(client_ip_24h_count)) > 7, 7, floor(ln(client_ip_24h_count))) as client_ip_24h_count_level
		,client_ip_b1_count_ratio
		,client_ip_b24_count_ratio
		,client_ip_0h_count_ratio
		,client_ip_1h_count_ratio
		,client_ip_24h_count_ratio

		,if(floor(ln(device_sign_count)) > 9, 9, floor(ln(device_sign_count))) as device_sign_count_level
		,if(floor(ln(device_sign_b1_count)) > 9, 9, floor(ln(device_sign_b1_count))) as device_sign_b1_count_level
		,if(floor(ln(device_sign_b24_count)) > 9, 9, floor(ln(device_sign_b24_count))) as device_sign_b24_count_level
		,if(floor(ln(device_sign_0h_count)) > 6, 6, floor(ln(device_sign_0h_count))) as device_sign_0h_count_level
		,if(floor(ln(device_sign_1h_count)) > 6, 6, floor(ln(device_sign_1h_count))) as device_sign_1h_count_level
		,if(floor(ln(device_sign_24h_count)) > 7, 7, floor(ln(device_sign_24h_count))) as device_sign_24h_count_level
		,device_sign_count_ratio
		,device_sign_b1_count_ratio
		,device_sign_b24_count_ratio
		,device_sign_0h_count_ratio
		,device_sign_1h_count_ratio
		,device_sign_24h_count_ratio

		,if(floor(ln(ip_prov_count)) > 9, 9, floor(ln(ip_prov_count))) as ip_prov_count_level
		,if(floor(ln(ip_prov_b1_count)) > 9, 9, floor(ln(ip_prov_b1_count))) as ip_prov_b1_count_level
		,if(floor(ln(ip_prov_b24_count)) > 9, 9, floor(ln(ip_prov_b24_count))) as ip_prov_b24_count_level
		,if(floor(ln(ip_prov_0h_count)) > 6, 6, floor(ln(ip_prov_0h_count))) as ip_prov_0h_count_level
		,if(floor(ln(ip_prov_1h_count)) > 6, 6, floor(ln(ip_prov_1h_count))) as ip_prov_1h_count_level
		,if(floor(ln(ip_prov_24h_count)) > 6, 6, floor(ln(ip_prov_24h_count))) as ip_prov_24h_count_level
		,ip_prov_count_ratio
		,ip_prov_b1_count_ratio
		,ip_prov_b24_count_ratio
		,ip_prov_0h_count_ratio
		,ip_prov_1h_count_ratio
		,ip_prov_24h_count_ratio

		,if(floor(ln(ip_city_count)) > 9, 9, floor(ln(ip_city_count))) as ip_city_count_level
		,if(floor(ln(ip_city_b1_count)) > 9, 9, floor(ln(ip_city_b1_count))) as ip_city_b1_count_level
		,if(floor(ln(ip_city_b24_count)) > 9, 9, floor(ln(ip_city_b24_count))) as ip_city_b24_count_level
		,if(floor(ln(ip_city_0h_count)) > 6, 6, floor(ln(ip_city_0h_count))) as ip_city_0h_count_level
		,if(floor(ln(ip_city_1h_count)) > 6, 6, floor(ln(ip_city_1h_count))) as ip_city_1h_count_level
		,if(floor(ln(ip_city_24h_count)) > 7, 7, floor(ln(ip_city_24h_count))) as ip_city_24h_count_level
		,ip_city_count_ratio
		,ip_city_b1_count_ratio
		,ip_city_b24_count_ratio
		,ip_city_0h_count_ratio
		,ip_city_1h_count_ratio
		,ip_city_24h_count_ratio

		,if(floor(ln(info_1_count)) > 8, 8, floor(ln(info_1_count))) as info_1_count_level
		,if(floor(ln(info_1_0h_count)) > 6, 6, floor(ln(info_1_0h_count))) as info_1_0h_count_level
		,if(floor(ln(info_1_1h_count)) > 6, 6, floor(ln(info_1_1h_count))) as info_1_1h_count_level
		,if(floor(ln(info_1_24h_count)) > 7, 7, floor(ln(info_1_24h_count))) as info_1_24h_count_level
		,info_1_count_ratio
		,info_1_0h_count_ratio
		,info_1_1h_count_ratio
		,info_1_24h_count_ratio

		,if(floor(ln(info_2_count)) > 8, 8, floor(ln(info_2_count))) as info_2_count_level
		,if(floor(ln(info_2_0h_count)) > 6, 6, floor(ln(info_2_0h_count))) as info_2_0h_count_level
		,if(floor(ln(info_2_1h_count)) > 6, 6, floor(ln(info_2_1h_count))) as info_2_1h_count_level
		,if(floor(ln(info_2_24h_count)) > 7, 7, floor(ln(info_2_24h_count))) as info_2_24h_count_level
		,info_2_count_ratio
		,info_2_0h_count_ratio
		,info_2_1h_count_ratio
		,info_2_24h_count_ratio

		,if(floor(ln(pay_scene_count)) > 9, 9, floor(ln(pay_scene_count))) as pay_scene_count_level
		,if(floor(ln(pay_scene_b24_count)) > 9, 9, floor(ln(pay_scene_b24_count))) as pay_scene_b24_count_level
		,if(floor(ln(pay_scene_0h_count)) > 6, 6, floor(ln(pay_scene_0h_count))) as pay_scene_0h_count_level
		,if(floor(ln(pay_scene_1h_count)) > 6, 6, floor(ln(pay_scene_1h_count))) as pay_scene_1h_count_level
		,if(floor(ln(pay_scene_24h_count)) > 7, 7, floor(ln(pay_scene_24h_count))) as pay_scene_24h_count_level
		,pay_scene_count_ratio
		,pay_scene_b24_count_ratio
		,pay_scene_0h_count_ratio
		,pay_scene_1h_count_ratio
		,pay_scene_24h_count_ratio

		,if(floor(ln(opposing_id_count)) > 9, 9, floor(ln(opposing_id_count))) as opposing_id_count_level
		,if(floor(ln(opposing_id_0h_count)) > 6, 6, floor(ln(opposing_id_0h_count))) as opposing_id_0h_count_level
		,if(floor(ln(opposing_id_1h_count)) > 6, 6, floor(ln(opposing_id_1h_count))) as opposing_id_1h_count_level
		,if(floor(ln(opposing_id_24h_count)) > 7, 7, floor(ln(opposing_id_24h_count))) as opposing_id_24h_count_level
		,if(floor(ln(opposing_id_b1_count)) > 9, 9, floor(ln(opposing_id_b1_count))) as opposing_id_b1_count_level
		,if(floor(ln(opposing_id_b24_count)) > 9, 9, floor(ln(opposing_id_b24_count))) as opposing_id_b24_count_level
		,opposing_id_count_ratio
		,opposing_id_0h_count_ratio
		,opposing_id_1h_count_ratio
		,opposing_id_24h_count_ratio
		,opposing_id_b1_count_ratio
		,opposing_id_b24_count_ratio

		,if(floor(ln(amt_sum)) > 16, 16, floor(ln(amt_sum))) as amt_sum_level
		,if(floor(ln(amt_b24_sum)) > 16, 16, floor(ln(amt_b24_sum))) as amt_b24_sum_level
		,if(floor(ln(amt_b168_sum)) > 16, 16, floor(ln(amt_b168_sum))) as amt_b168_sum_level
		,if(floor(ln(amt_0h_sum)) > 13, 13, floor(ln(amt_0h_sum))) as amt_0h_sum_level
		,if(floor(ln(amt_1h_sum)) > 13, 13, floor(ln(amt_1h_sum))) as amt_1h_sum_level
		,if(floor(ln(amt_24h_sum)) > 13, 13, floor(ln(amt_24h_sum))) as amt_24h_sum_level
		,amt_b24_sum_ratio
		,amt_b168_sum_ratio
		,amt_0h_sum_ratio
		,amt_1h_sum_ratio
		,amt_24h_sum_ratio

		,uniq_network_by_ip_prov
		,uniq_device_sign_by_ip_prov
		,uniq_event_by_ip_prov
		,uniq_pay_scene_by_device_sign
		,uniq_opposing_id_by_device_sign
		
		,floor(event_time_delta) as event_time_delta
		,floor(ip_prov_residual_time) as ip_prov_residual_time

		,t2.user_cnt as user_cnt_by_device_sign
		,t3.user_cnt as user_cnt_by_client_ip

FROM atec_test_featured t1
	LEFT JOIN test_user_count_by_device_sign_mapping t2 ON t1.event_id = t2.event_id
	LEFT JOIN test_user_count_by_client_ip_mapping t3 ON t1.event_id = t3.event_id
;



