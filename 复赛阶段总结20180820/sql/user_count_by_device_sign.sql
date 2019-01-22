drop table if exists train_user_count_by_device_sign_mapping;
create table train_user_count_by_device_sign_mapping as
select t1.event_id, t1.device_sign, t1.gmt_occur_dt, count(distinct t2.user_id) as user_cnt
from (select event_id, user_id, device_sign, gmt_occur_dt from atec_train where device_sign is not null) t1
	left join (select event_id, user_id, device_sign, gmt_occur_dt from atec_train where device_sign is not null) t2
    	on t1.device_sign = t2.device_sign
where t1.gmt_occur_dt >= t2.gmt_occur_dt
group by t1.event_id, t1.gmt_occur_dt, t1.device_sign;

drop table if exists test_user_count_by_device_sign_mapping;
create table test_user_count_by_device_sign_mapping as
select t1.event_id, t1.device_sign, t1.gmt_occur_dt, count(distinct t2.user_id) as user_cnt
from (select event_id, user_id, device_sign, gmt_occur_dt from atec_test where device_sign is not null) t1
	left join
		(
			select event_id, user_id, device_sign, gmt_occur_dt from atec_train where device_sign is not null
		 	union all
		 	select event_id, user_id, device_sign, gmt_occur_dt from atec_test where device_sign is not null
		) t2
  on t1.device_sign = t2.device_sign
where t1.gmt_occur_dt >= t2.gmt_occur_dt
group by t1.event_id, t1.gmt_occur_dt, t1.device_sign;



drop table if exists train_user_count_by_client_ip_mapping;
create table train_user_count_by_client_ip_mapping as
select t1.event_id, t1.client_ip, t1.gmt_occur_dt, count(distinct t2.user_id) as user_cnt
from (select event_id, user_id, client_ip, gmt_occur_dt from atec_train where client_ip is not null) t1
	left join (select event_id, user_id, client_ip, gmt_occur_dt from atec_train where client_ip is not null) t2
    	on t1.client_ip = t2.client_ip
where t1.gmt_occur_dt = t2.gmt_occur_dt
group by t1.event_id, t1.gmt_occur_dt, t1.client_ip;

drop table if exists test_user_count_by_client_ip_mapping;
create table test_user_count_by_client_ip_mapping as
select t1.event_id, t1.client_ip, t1.gmt_occur_dt, count(distinct t2.user_id) as user_cnt
from (select event_id, user_id, client_ip, gmt_occur_dt from atec_test where client_ip is not null) t1
	left join
		(
			select event_id, user_id, client_ip, gmt_occur_dt from atec_train where client_ip is not null
		 	union all
		 	select event_id, user_id, client_ip, gmt_occur_dt from atec_test where client_ip is not null
		) t2
  on t1.client_ip = t2.client_ip
where t1.gmt_occur_dt = t2.gmt_occur_dt
group by t1.event_id, t1.gmt_occur_dt, t1.client_ip;