CREATE OR REPLACE VIEW rpro_cust_acct_summ_v
AS
  SELECT summ.period_name ,
    summ.book_id ,
    summ.rc_id ,
    summ.client_id ,
    summ.line_id ,
    summ.root_line_id ,
    acg.NAME AS acctg_type ,
    summ.acct_segs ,
    summ.vc_flag ,
    summ.cost_flag ,
    DECODE(summ.schd_type,'M','Y','T','Y','N') AS mje_flag,
    SUM (
    CASE
      WHEN acg.acct_group IN ('EREV','REV')
      AND acg.pl_flag      ='Y'
      THEN summ.t_tl_at_ut
      ELSE 0
    END) AS T_Cum_Revenue_UTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.t_ar_at_ut
      ELSE 0
    END) AS T_Cum_Adj_Revenue_UTD,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.t_ar_at
      ELSE 0
    END) AS T_Adj_MTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.t_ar_at_qt
      ELSE 0
    END) AS T_Adj_QTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.t_ar_at_yt
      ELSE 0
    END) AS T_Adj_YTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.t_tl_at
      ELSE 0
    END) AS T_Total_Revenue_Activity,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.t_tl_at_qt
      ELSE 0
    END) AS T_Total_Revenue_Activity_QTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.t_tl_at_yt
      ELSE 0
    END) AS T_Total_Revenue_Activity_YTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('EREV','REV')
      AND acg.pl_flag      ='Y'
      THEN summ.f_tl_at_ut
      ELSE 0
    END) AS F_Cum_Revenue_UTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.f_ar_at_ut
      ELSE 0
    END) AS f_Cum_Adj_Revenue_UTD,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.f_ar_at
      ELSE 0
    END) AS F_Adj_MTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.f_ar_at_qt
      ELSE 0
    END) AS F_Adj_QTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.f_ar_at_yt
      ELSE 0
    END) AS F_Adj_YTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.f_tl_at
      ELSE 0
    END) AS F_Total_Revenue_Activity,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.f_tl_at_qt
      ELSE 0
    END) AS F_Total_Revenue_Activity_QTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.f_tl_at_yt
      ELSE 0
    END) AS F_Total_Revenue_Activity_YTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('EREV','REV')
      AND acg.pl_flag      ='Y'
      THEN summ.r_tl_at_ut
      ELSE 0
    END) AS R_Cum_Revenue_UTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.r_ar_at_ut
      ELSE 0
    END) AS R_Cum_Adj_Revenue_UTD,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.r_ar_at
      ELSE 0
    END) AS R_Adj_MTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.r_ar_at_qt
      ELSE 0
    END) AS R_Adj_QTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group = 'EREV'
      AND acg.pl_flag     ='Y'
      THEN summ.r_ar_at_yt
      ELSE 0
    END) AS R_Adj_YTD_Revenue,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.r_tl_at
      ELSE 0
    END) AS R_Total_Revenue_Activity,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.r_tl_at_qt
      ELSE 0
    END) AS R_Total_Revenue_Activity_QTD,
    SUM (
    CASE
      WHEN acg.acct_group IN ('REV', 'EREV')
      AND acg.pl_flag      ='Y'
      THEN summ.r_tl_at_yt
      ELSE 0
    END) AS R_Total_Revenue_Activity_YTD,
    SUM (
    CASE
      WHEN summ.acct_group        IN ('DREV', 'EDREV')
      AND summ.incl_netting_flag   = 'Y'
      AND summ.payables_acct_flag <> 'Y'
      THEN summ.t_tl_eb
      ELSE 0
    END) AS T_Total_Ending_Balance ,
    SUM (
    CASE
      WHEN summ.acct_group        IN ('DREV', 'EDREV')
      AND summ.incl_netting_flag   = 'Y'
      AND summ.payables_acct_flag <> 'Y'
      THEN summ.f_tl_eb
      ELSE 0
    END) AS F_Total_Ending_Balance ,
    SUM (
    CASE
      WHEN summ.acct_group        IN ('DREV', 'EDREV')
      AND summ.incl_netting_flag   = 'Y'
      AND summ.payables_acct_flag <> 'Y'
      THEN summ.r_tl_eb
      ELSE 0
    END) AS R_Total_Ending_Balance ,
    SUM (
    CASE
      WHEN summ.acct_group        IN ('DREV', 'EDREV')
      AND summ.incl_netting_flag   = 'Y'
      AND summ.payables_acct_flag <> 'Y'
      THEN summ.t_al_eb
      ELSE 0
    END) AS T_Alloc_Ending_Balance ,
    SUM (
    CASE
      WHEN summ.acct_group        IN ('DREV', 'EDREV')
      AND summ.incl_netting_flag   = 'Y'
      AND summ.payables_acct_flag <> 'Y'
      THEN summ.f_al_eb
      ELSE 0
    END) AS F_Alloc_Ending_Balance ,
    SUM (
    CASE
      WHEN summ.acct_group        IN ('DREV', 'EDREV')
      AND summ.incl_netting_flag   = 'Y'
      AND summ.payables_acct_flag <> 'Y'
      THEN summ.r_al_eb
      ELSE 0
    END) AS R_Alloc_Ending_Balance ,
    SUM (
    CASE
      WHEN acg.ID IN ('Z','U')
      THEN summ.T_UNBL_AT_UT
      ELSE 0
    END) AS T_unbill_ending_balance ,
    SUM (
    CASE
      WHEN acg.ID IN ('Z','U')
      THEN summ.F_UNBL_AT_UT
      ELSE 0
    END) AS F_unbill_ending_balance ,
    SUM (
    CASE
      WHEN acg.ID IN ('Z','U')
      THEN summ.R_UNBL_AT_UT
      ELSE 0
    END) AS R_unbill_ending_balance ,
    SUM (
    CASE
      WHEN acg.ID = ('T')
      THEN summ.T_CONTRA_RL_UT
      ELSE 0
    END) AS T_Contra_AR_balance ,
    SUM (
    CASE
      WHEN acg.ID = ('T')
      THEN summ.F_CONTRA_RL_UT
      ELSE 0
    END) AS F_Contra_AR_balance ,
    SUM (
    CASE
      WHEN acg.ID = ('T')
      THEN summ.R_CONTRA_RL_UT
      ELSE 0
    END) AS R_Contra_AR_balance
  FROM rpro_cust_ri_ln_acct_summ_v summ ,
    rpro_ri_acct_grp_v acg
  WHERE summ.acct_type_id = acg.ID
  AND summ.client_id      = acg.client_id
  GROUP BY summ.period_name ,
    summ.book_id ,
    summ.rc_id ,
    summ.client_id ,
    summ.line_id ,
    summ.root_line_id ,
    acg.NAME ,
    summ.acct_segs ,
    summ.vc_flag ,
    summ.cost_flag ,
    DECODE(summ.schd_type,'M','Y','T','Y','N');
  /