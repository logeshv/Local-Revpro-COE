CREATE OR REPLACE VIEW RPRO_RI_CUST_INSIGHT_V
AS
SELECT  '#1' c
FROM  rpro_rc_line_cbill_v        rrlc
     ,rpro_cust_acct_summ_v       cust_rv_insgt
WHERE 1          = 1
AND   rrlc.id     = cust_rv_insgt.root_line_id
AND   rrlc.rc_id  = cust_rv_insgt.rc_id;

/