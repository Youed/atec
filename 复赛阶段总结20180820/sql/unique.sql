drop table if exists dim_train_uniq_by_ip_prov;
create table dim_train_uniq_by_ip_prov as
select
	event_id,
    count(distinct network2) as uniq_network_by_ip_prov,
    count(distinct device_sign2) as uniq_device_sign_by_ip_prov,
    count(distinct operation_channel2) as uniq_operation_channel_by_ip_prov,
    count(1) as uniq_event_by_ip_prov
from tmp_atec_train_join
where ip_prov = ip_prov2 and gmt_occur_dt = gmt_occur_dt2
group by event_id;

drop table if exists dim_test_uniq_by_ip_prov;
create table dim_test_uniq_by_ip_prov as
select
	event_id,
    count(distinct network2) as uniq_network_by_ip_prov,
    count(distinct device_sign2) as uniq_device_sign_by_ip_prov,
    count(distinct operation_channel2) as uniq_operation_channel_by_ip_prov,
    count(1) as uniq_event_by_ip_prov
from tmp_atec_test_join
where ip_prov = ip_prov2 and gmt_occur_dt = gmt_occur_dt2
group by event_id;


drop table if exists dim_train_uniq_by_device_sign;
create table dim_train_uniq_by_device_sign as
select
    event_id,
    count(distinct pay_scene2) as uniq_pay_scene_by_device_sign,
    count(distinct amt2) as uniq_amt_by_device_sign,
    count(distinct opposing_id2) as uniq_opposing_id_by_device_sign
from tmp_atec_train_join
where gmt_occur_dt = gmt_occur_dt2 and device_sign = device_sign2
group by event_id;


drop table if exists dim_test_uniq_by_device_sign;
create table dim_test_uniq_by_device_sign as
select
    event_id,
    count(distinct pay_scene2) as uniq_pay_scene_by_device_sign,
    count(distinct amt2) as uniq_amt_by_device_sign,
    count(distinct opposing_id2) as uniq_opposing_id_by_device_sign
from tmp_atec_test_join
where gmt_occur_dt = gmt_occur_dt2 and device_sign = device_sign2
group by event_id;