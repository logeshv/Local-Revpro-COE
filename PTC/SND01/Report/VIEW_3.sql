CREATE OR REPLACE VIEW RPRO_RI_CUST_INSIGHT_V
AS
SELECT  '#1' c
FROM  rpro_rc_line_g        rrl
     ,rpro_cust_acct_summ_v CUST_RV_INSGT
WHERE 1         = 1
AND   rrl.id    = CUST_RV_INSGT.root_line_id
AND   rrl.rc_id = CUST_RV_INSGT.rc_id;

/