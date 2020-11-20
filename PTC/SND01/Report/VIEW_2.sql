CREATE OR REPLACE VIEW rpro_cust_acct_summ_v
AS
SELECT summ.period_name
      ,summ.book_id
      ,summ.rc_id
      ,summ.client_id
      ,summ.line_id
      ,summ.root_line_id
      ,acg.NAME AS acctg_type
      ,summ.acct_segs
      ,summ.vc_flag
      ,summ.cost_flag
      ,DECODE(summ.schd_type,'M','Y','T','Y','N') AS mje_flag,
      --------------------------------------------------------------Revenue Insight columns Transactional
       SUM (
       CASE
         WHEN acg.acct_group = 'EREV'
         AND acg.pl_flag     ='Y'
         THEN summ.t_ar_at
         ELSE 0
       END) AS T_Adj_Revenue_Activity, ----T Adj Revenue Activity
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.t_tl_at
         ELSE 0
       END) AS T_Total_Revenue_Activity,--T Total Revenue Activity
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.t_tl_at_qt
         ELSE 0
       END) AS T_Total_Revenue_Activity_QTD, -- T Total Revenue Activity QTD
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.t_tl_at_yt
         ELSE 0
       END) AS T_Total_Revenue_Activity_YTD, -- T Total Revenue Activity YTD
       ----------------------------------------------------------------------Revenue Insight columns Functional
       SUM (
       CASE
         WHEN acg.acct_group = 'EREV'
         AND acg.pl_flag     ='Y'
         THEN summ.f_ar_at
         ELSE 0
       END) AS F_Adj_Revenue_Activity, --F Adj Revenue Activity
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.f_tl_at
         ELSE 0
       END) AS F_Total_Revenue_Activity, -- F Total Revenue Activity
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.f_tl_at_qt
         ELSE 0
       END) AS F_Total_Revenue_Activity_QTD, -- F Total Revenue Activity  QTD
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.f_tl_at_yt
         ELSE 0
       END) AS F_Total_Revenue_Activity_YTD, -- F Total Revenue Activity YTD
       -------------------------------------------------------------------------------Revenue Insight columns Reporting
       SUM (
       CASE
         WHEN acg.acct_group = 'EREV'
         AND acg.pl_flag     ='Y'
         THEN summ.r_ar_at
         ELSE 0
       END) AS R_Adj_Revenue_Activity, --R Adj Revenue Activity
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.r_tl_at
         ELSE 0
       END) AS R_Total_Revenue_Activity, -- R Total Revenue Activity
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.r_tl_at_qt
         ELSE 0
       END) AS R_Total_Revenue_Activity_QTD, -- R Total Revenue Activity QTD
       SUM (
       CASE
         WHEN acg.acct_group IN ('REV', 'EREV')
         AND acg.pl_flag      ='Y'
         THEN summ.r_tl_at_yt
         ELSE 0
       END) AS R_Total_Revenue_Activity_YTD, -- R Total Revenue Activity YTD
       -----------------------------------------------------------Rc rollforward columns Total Ending Balance Transactional
       SUM (
       CASE
         WHEN summ.acct_group        IN ('DREV', 'EDREV')
         AND summ.incl_netting_flag   = 'Y' --20170103
         AND summ.payables_acct_flag <> 'Y'
         THEN summ.t_tl_eb
         ELSE 0
       END) AS T_Total_Ending_Balance ,
       SUM (                                                     --Rc rollforward columns Total Ending Balance Functional
       CASE
         WHEN summ.acct_group        IN ('DREV', 'EDREV')
         AND summ.incl_netting_flag   = 'Y' --20170103
         AND summ.payables_acct_flag <> 'Y'
         THEN summ.f_tl_eb
         ELSE 0
       END) AS F_Total_Ending_Balance ,
       SUM (                                                     ----Rc rollforward columns Total Ending Balance Reporting
       CASE
         WHEN summ.acct_group        IN ('DREV', 'EDREV')
         AND summ.incl_netting_flag   = 'Y' --20170103
         AND summ.payables_acct_flag <> 'Y'
         THEN summ.r_tl_eb
         ELSE 0
       END) AS R_Total_Ending_Balance ,
       ---------------------------------------------------------------Rc rollforward columns Alloc Ending Balance Transactional
       SUM (
       CASE
         WHEN summ.acct_group        IN ('DREV', 'EDREV')
         AND summ.incl_netting_flag   = 'Y' --20170103
         AND summ.payables_acct_flag <> 'Y'
         THEN summ.t_al_eb
         ELSE 0
       END) AS T_Alloc_Ending_Balance , --T Alloc Ending Balance
       SUM (                                                         --Rc rollforward columns Alloc Ending Balance Functional
       CASE
         WHEN summ.acct_group        IN ('DREV', 'EDREV')
         AND summ.incl_netting_flag   = 'Y' --20170103
         AND summ.payables_acct_flag <> 'Y'
         THEN summ.f_al_eb
         ELSE 0
       END) AS F_Alloc_Ending_Balance , --F Alloc Ending Balance
       SUM (                                                         --Rc rollforward columns Alloc Ending Balance Reporting
       CASE
         WHEN summ.acct_group        IN ('DREV', 'EDREV')
         AND summ.incl_netting_flag   = 'Y' --20170103
         AND summ.payables_acct_flag <> 'Y'
         THEN summ.r_al_eb
         ELSE 0
       END) AS R_Alloc_Ending_Balance ,--R Alloc Ending Balance
       -------------------------------------------------------------------------------------Unbill Balance from unbill rollforward report
       SUM (
       CASE
         WHEN acg.ID IN ('Z','U')
         THEN summ.T_UNBL_AT_UT
         ELSE 0
       END) AS T_unbill_ending_balance ,--T unbill ending balance
       SUM (
       CASE
         WHEN acg.ID IN ('Z','U')
         THEN summ.F_UNBL_AT_UT
         ELSE 0
       END) AS F_unbill_ending_balance ,--F unbill ending balance
       SUM (
       CASE
         WHEN acg.ID IN ('Z','U')
         THEN summ.R_UNBL_AT_UT
         ELSE 0
       END) AS R_unbill_ending_balance ,--R unbill ending balance
       ------------------------------------------------------------------------------------Contra Schedule
       SUM (
       CASE
         WHEN acg.ID = ('T')
         THEN summ.T_CONTRA_RL_UT
         ELSE 0
       END) AS T_Contra_AR_balance ,--T Contra AR balance
       SUM (
       CASE
         WHEN acg.ID = ('T')
         THEN summ.F_CONTRA_RL_UT
         ELSE 0
       END) AS F_Contra_AR_balance ,--T Contra AR balance
       SUM (
       CASE
         WHEN acg.ID = ('T')
         THEN summ.R_CONTRA_RL_UT
         ELSE 0
       END) AS R_Contra_AR_balance --T Contra AR balance
       ------------------------------------------------------------------------------------
FROM rpro_cust_ri_ln_acct_summ_v summ
    ,rpro_ri_acct_grp_v     acg
WHERE summ.acct_type_id   = acg.ID
AND   summ.client_id      = acg.client_id
GROUP BY summ.period_name
        ,summ.book_id
        ,summ.rc_id
        ,summ.client_id
        ,summ.line_id
        ,summ.root_line_id
        ,acg.NAME
        ,summ.acct_segs
        ,summ.vc_flag
        ,summ.cost_flag
        ,DECODE(summ.schd_type,'M','Y','T','Y','N');
