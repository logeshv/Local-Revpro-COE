CREATE OR REPLACE VIEW rpro_rc_line_cbill_v
AS
WITH bill AS( SELECT  rc_id
                    , line_id
                    , SUM(ext_sll_prc) AS cumm_bill_amt
                    , SUM(ext_sll_prc*f_ex_rate) AS f_cumm_bill_amt 
              FROM    rpro_rc_bill_g bill_g
              WHERE   type  != 'RORD'
              AND     rc_id != 0
              GROUP BY rc_id
                      ,line_id )
SELECT /* LEADING(rrl,rrb)*/
        rrl.id 
       ,rrl.doc_date                  sales_order_date 
       ,rrl.doc_num                   sales_order_num 
       ,rrl.rc_id                     
       ,rrl.item_num                  item_number 
       ,rrl.atr9                      project_number 
       ,rrl.doc_line_id               sales_order_line_id 
       ,rrl.atr1 mson 
       ,rrl.end_date 
       ,rrl.start_date 
       ,rrl.business_unit 
       ,rrl.atr3                      subscription_number 
       ,rrl.net_sll_prc               t_transaction_price 
       ,rrl.net_sll_prc*rrl.f_ex_rate f_transaction_price 
       ,rrl.net_sll_prc*rrl.g_ex_rate r_transaction_price 
FROM rpro_rc_line_g rrl ,
     bill rrb
WHERE rrb.line_id(+) = rrl.id
AND   rrb.rc_id(+)   = rrl.rc_id
AND   rrl.rc_id     != 0;

/