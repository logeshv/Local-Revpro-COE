
  CREATE TABLE rpro_cust_rc_schd 
   (	
   id            NUMBER, 
   rel_id        NUMBER, 
   rc_id         NUMBER, 
   rc_ver        NUMBER, 
   line_id       NUMBER, 
   pob_id        NUMBER, 
   curr          VARCHAR2(10 ), 
   amount        NUMBER, 
   rel_pct       NUMBER, 
   indicators    VARCHAR2(40 ), 
   post_date     DATE, 
   prd_id        NUMBER, 
   post_prd_id   NUMBER, 
   post_batch_id NUMBER, 
   dr_segments   VARCHAR2(260 ), 
   cr_segments   VARCHAR2(260 ), 
   f_ex_rate     NUMBER, 
   g_ex_rate     NUMBER, 
   ex_rate_date  DATE, 
   atr1          VARCHAR2(257 ), 
   atr2          VARCHAR2(257 ), 
   atr3          VARCHAR2(257 ), 
   atr4          VARCHAR2(257 ), 
   atr5          VARCHAR2(257 ), 
   client_id     NUMBER, 
   crtd_prd_id   NUMBER, 
   sec_atr_val   VARCHAR2(257 ), 
   crtd_by       VARCHAR2(257 ), 
   crtd_dt       DATE, 
   updt_by       VARCHAR2(257 ), 
   updt_dt       DATE, 
   dist_id       NUMBER, 
   ref_bill_id   NUMBER, 
   root_line_id  NUMBER, 
   book_id       NUMBER, 
   bld_fx_dt     DATE, 
   bld_fx_rate   NUMBER, 
   rord_inv_ref  NUMBER, 
   orig_line_id  NUMBER, 
   dr_link_id    NUMBER, 
   cr_link_id    NUMBER, 
   model_id      NUMBER, 
   je_batch_id   NUMBER, 
   je_batch_name VARCHAR2(257 ), 
   pp_amt        NUMBER, 
   pq_amt        NUMBER, 
   py_amt        NUMBER, 
   updt_prd_id   NUMBER
   );