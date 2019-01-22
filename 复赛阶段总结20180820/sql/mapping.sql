DROP TABLE IF EXISTS network_mapping;
CREATE TABLE network_mapping
AS
SELECT row_number() over(order by network) map_id, network FROM
(
    SELECT DISTINCT network as network FROM
    (
        SELECT DISTINCT network FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT network FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT network FROM atec_1000w_ootb_data
    ) UT
) DN;



DROP TABLE IF EXISTS ip_prov_mapping;
CREATE TABLE ip_prov_mapping
AS
SELECT row_number() over(order by ip_prov) map_id, ip_prov FROM
(
    SELECT DISTINCT ip_prov as ip_prov FROM
    (
        SELECT DISTINCT ip_prov FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT ip_prov FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT ip_prov FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS ip_city_mapping;
CREATE TABLE ip_city_mapping
AS
SELECT row_number() over(order by ip_city) map_id, ip_city FROM
(
    SELECT DISTINCT ip_city as ip_city FROM
    (
        SELECT DISTINCT ip_city FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT ip_city FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT ip_city FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS cert_prov_mapping;
CREATE TABLE cert_prov_mapping
AS
SELECT row_number() over(order by cert_prov) map_id, cert_prov FROM
(
    SELECT DISTINCT cert_prov as cert_prov FROM
    (
        SELECT DISTINCT cert_prov FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT cert_prov FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT cert_prov FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS cert_city_mapping;
CREATE TABLE cert_city_mapping
AS
SELECT row_number() over(order by cert_city) map_id, cert_city FROM
(
    SELECT DISTINCT cert_city as cert_city FROM
    (
        SELECT DISTINCT cert_city FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT cert_city FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT cert_city FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS is_one_people_mapping;
CREATE TABLE is_one_people_mapping
AS
SELECT row_number() over(order by is_one_people) map_id, is_one_people FROM
(
    SELECT DISTINCT is_one_people as is_one_people FROM
    (
        SELECT DISTINCT is_one_people FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT is_one_people FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT is_one_people FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS mobile_oper_platform_mapping;
CREATE TABLE mobile_oper_platform_mapping
AS
SELECT row_number() over(order by mobile_oper_platform) map_id, mobile_oper_platform FROM
(
    SELECT DISTINCT mobile_oper_platform as mobile_oper_platform FROM
    (
        SELECT DISTINCT mobile_oper_platform FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT mobile_oper_platform FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT mobile_oper_platform FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS operation_channel_mapping;
CREATE TABLE operation_channel_mapping
AS
SELECT row_number() over(order by operation_channel) map_id, operation_channel FROM
(
    SELECT DISTINCT operation_channel as operation_channel FROM
    (
        SELECT DISTINCT operation_channel FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT operation_channel FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT operation_channel FROM atec_1000w_ootb_data
    ) UT
) DN;


DROP TABLE IF EXISTS pay_scene_mapping;
CREATE TABLE pay_scene_mapping
AS
SELECT row_number() over(order by pay_scene) map_id, pay_scene FROM
(
    SELECT DISTINCT pay_scene as pay_scene FROM
    (
        SELECT DISTINCT pay_scene FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT pay_scene FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT pay_scene FROM atec_1000w_ootb_data
    ) UT
) DN;

DROP TABLE IF EXISTS version_mapping;
CREATE TABLE version_mapping
AS
SELECT row_number() over(order by version) map_id, version FROM
(
    SELECT DISTINCT version as version FROM
    (
        SELECT DISTINCT version FROM atec_1000w_ins_data
        UNION ALL
        SELECT DISTINCT version FROM atec_1000w_oota_data
        UNION ALL
        SELECT DISTINCT version FROM atec_1000w_ootb_data
    ) UT
) DN;
