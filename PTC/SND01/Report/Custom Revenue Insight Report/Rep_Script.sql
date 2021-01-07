
DECLARE
      CURSOR c_col
      IS
      SELECT column_name ,
             CASE WHEN column_name = 'PERIOD_NAME'
                  THEN 'VARCHAR2'
                  ELSE 'AMOUNT'
             END data_type ,
             column_name derived_column,
             'RPRO_CUST_ACCT_SUMM_V' tbl_name
      FROM  all_tab_columns
      WHERE table_name     = 'RPRO_CUST_ACCT_SUMM_V'
      AND   column_name NOT IN ( SELECT col_name
                                 FROM rpro_label_g
                                 WHERE tbl_name = 'RPRO_CUST_ACCT_SUMM_V'
                                )
      AND   column_name IN ('PERIOD_NAME'
                           ,'T_CUM_REVENUE_UTD'
                           ,'T_TOTAL_REVENUE_ACTIVITY'
                           ,'T_TOTAL_REVENUE_ACTIVITY_QTD'
                           ,'T_TOTAL_REVENUE_ACTIVITY_YTD'
                           ,'F_CUM_REVENUE_UTD'
                           ,'F_TOTAL_REVENUE_ACTIVITY'
                           ,'F_TOTAL_REVENUE_ACTIVITY_QTD'
                           ,'F_TOTAL_REVENUE_ACTIVITY_YTD'
                           ,'R_CUM_REVENUE_UTD'
                           ,'R_TOTAL_REVENUE_ACTIVITY'
                           ,'R_TOTAL_REVENUE_ACTIVITY_QTD'
                           ,'R_TOTAL_REVENUE_ACTIVITY_YTD'
                           ,'T_TOTAL_ENDING_BALANCE'
                           ,'F_TOTAL_ENDING_BALANCE'
                           ,'R_TOTAL_ENDING_BALANCE'
                           ,'T_ALLOC_ENDING_BALANCE'
                           ,'F_ALLOC_ENDING_BALANCE'
                           ,'R_ALLOC_ENDING_BALANCE'
                           ,'T_UNBILL_ENDING_BALANCE'
                           ,'F_UNBILL_ENDING_BALANCE'
                           ,'R_UNBILL_ENDING_BALANCE'
                           ,'T_CONTRA_AR_BALANCE'
                           ,'F_CONTRA_AR_BALANCE'
                           ,'R_CONTRA_AR_BALANCE'
                           ,'T_ADJ_MTD_REVENUE'    --20201121
                           ,'T_ADJ_QTD_REVENUE'    --20201121
                           ,'T_ADJ_YTD_REVENUE'    --20201121
                           ,'F_ADJ_MTD_REVENUE'    --20201121
                           ,'F_ADJ_QTD_REVENUE'    --20201121
                           ,'F_ADJ_YTD_REVENUE'    --20201121
                           ,'R_ADJ_MTD_REVENUE'    --20201121
                           ,'R_ADJ_QTD_REVENUE'    --20201121
                           ,'R_ADJ_YTD_REVENUE'    --20201121
                           ,'T_CUM_ADJ_REVENUE_UTD'--20201201
                           ,'F_CUM_ADJ_REVENUE_UTD'--20201201
                           ,'R_CUM_ADJ_REVENUE_UTD'--20201201
                           ,'T_CUM_BILLING_UTD'
                           ,'F_CUM_BILLING_UTD')
      UNION
      SELECT column_name ,
             data_type ,
             column_name derived_column,
             'RPRO_RC_LINE_CBILL_V' tbl_name
      FROM  all_tab_columns
      WHERE table_name     = 'RPRO_RC_LINE_CBILL_V'
      AND   column_name NOT IN ( SELECT col_name
                                 FROM rpro_label_g
                                 WHERE tbl_name = 'RPRO_RC_LINE_CBILL_V'
                                );

      
      --Please use different name for each report. Re-use the existing transaction lables where ever possible.
      CURSOR c1
      IS 
         SELECT * FROM (
         SELECT rl.*,'CUST_RV_INSGT' alias
         FROM  rpro_label_g rl
         WHERE tbl_name    = 'RPRO_CUST_ACCT_SUMM_V'      
         UNION ALL
         SELECT rl.*,'rrlc' alias
         FROM  rpro_label_g rl
         WHERE tbl_name    = 'RPRO_RC_LINE_CBILL_V' )
         ORDER BY tbl_name desc;
   
      CURSOR c2
      IS
         SELECT *
         FROM   rpro_rep_field_g
         WHERE  1 = 1
         AND    rep_id           = ( SELECT id FROM rpro_rep_g WHERE rep_name = 'Custom Revenue Insight Report') order by id;
   
      l_period_id            NUMBER;
      l_rep_seq              NUMBER ;
      l_lay_seq              NUMBER ;
      l_cnt                  NUMBER := 0;
      l_rpro_rep_g           rpro_rep_g%ROWTYPE;
      l_rpro_label_g         rpro_label_g%ROWTYPE;
      l_rpro_rep_field_g     rpro_rep_field_g%ROWTYPE;
      l_rpro_rp_layout_g     rpro_rp_layout_g%ROWTYPE;
      l_rpro_layout_field_g  rpro_layout_field_g%ROWTYPE;
      l_rpro_rep_filter_g    rpro_rep_filter_g%ROWTYPE;
      l_tbl_name             VARCHAR2(240):= 'RPRO_CUST_INSGT';
      l_rep_name             VARCHAR2(240):= 'Custom Revenue Insight Report';
      l_alias                VARCHAR2(240):= 'CUST_RV_INSGT';
      l_report_id            NUMBER;
      l_rep_layout_id        NUMBER;
      
   BEGIN
      rpro_utility_pkg.set_revpro_context;
    
      BEGIN
         SELECT id
         INTO   l_report_id
         FROM   rpro_rep_g
         WHERE  rep_name   = l_rep_name;
         
         DELETE FROM rpro_layout_field_g
         WHERE       layout_id IN (SELECT id
                                   FROM   rpro_rp_layout_g
                                   WHERE  rep_id = l_report_id);
         
         DELETE FROM rpro_rep_filter_g
         WHERE       layout_id IN (SELECT id
                                   FROM   rpro_rp_layout_g
                                   WHERE  rep_id = l_report_id);
                                   
         DELETE FROM rpro_rep_field_g
         WHERE       rep_id = l_report_id;
         
         DELETE FROM rpro_rp_layout_g
         WHERE       rep_id = l_report_id;
         
         DELETE FROM rpro_rep_access_g
         WHERE       rep_id = l_report_id;
         
         DELETE FROM rpro_rep_g
         WHERE       id = l_report_id;
         
         DELETE FROM rpro_label_g
         WHERE       tbl_name    IN ('RPRO_CUST_ACCT_SUMM_V','RPRO_RC_LINE_CBILL_V');
         
         COMMIT;
      EXCEPTION WHEN OTHERS
      THEN
         NULL;
      END;
      
      --Generate Report and Layout id.
      l_rep_seq      := rpro_utility_pkg.generate_id('RPRO_REP_ID_S',rpro_utility_pkg.g_client_id);
      l_lay_seq      := rpro_utility_pkg.generate_id('RPRO_RP_LAYOUT_ID_S',rpro_utility_pkg.g_client_id);
      
      --Report Creation Period
      BEGIN
         SELECT id
         INTO   l_period_id
         FROM   rpro_period_g
         WHERE  client_id = rpro_utility_pkg.g_client_id; 
      EXCEPTION
      WHEN others THEN 
         l_period_id := 201501;
      END;
      
      ---Inserting in The RPRO_REP_G table
      BEGIN
         
         l_rpro_rep_g.id          := l_rep_seq;
         l_rpro_rep_g.rep_name    := l_rep_name;
         l_rpro_rep_g.rep_desc    := l_rep_name;
         l_rpro_rep_g.category    := 'Custom';
         l_rpro_rep_g.view_name   := 'RPRO_RI_CUST_INSIGHT_V';
         l_rpro_rep_g.indicators  := rpro_rep_pkg.g_set_default_ind;
         l_rpro_rep_g.crtd_by     := rpro_utility_pkg.g_user;
         l_rpro_rep_g.crtd_dt     := SYSDATE;
         l_rpro_rep_g.updt_by     := rpro_utility_pkg.g_user;
         l_rpro_rep_g.updt_dt     := SYSDATE;
         l_rpro_rep_g.client_id   := rpro_utility_pkg.g_client_id;
         l_rpro_rep_g.crtd_prd_id := l_period_id;
         
         l_rpro_rep_g.indicators  := rpro_rep_pkg.set_enabled_flag (l_rpro_rep_g.indicators, 'Y');
         l_rpro_rep_g.indicators  := rpro_rep_pkg.set_long_running_flag (l_rpro_rep_g.indicators, 'Y');
         
         INSERT INTO rpro_rep_g
         VALUES l_rpro_rep_g;
         
         ---Inserting in The Report access table
         
         INSERT INTO rpro_rep_access_g (rep_id
                                       ,role_id
                                       ,crtd_dt
                                       ,crtd_by
                                       ,updt_dt
                                       ,updt_by
                                       ,client_id)
         SELECT                         l_rep_seq
                                       ,id
                                       ,SYSDATE
                                       ,rpro_utility_pkg.g_user
                                       ,SYSDATE
                                       ,rpro_utility_pkg.g_user
                                       ,rpro_utility_pkg.g_client_id
         FROM                           rpro_role_g
         WHERE                          1 = 1;
   
         COMMIT;
      END;
      

      BEGIN
         FOR rec_col IN c_col
         LOOP
            l_rpro_label_g.id             := rpro_utility_pkg.generate_id('RPRO_LABEL_ID_S',rpro_utility_pkg.g_client_id);
            l_rpro_label_g.col_name       := rec_col.column_name;
            l_rpro_label_g.tbl_name       := rec_col.tbl_name;
            --l_rpro_label_g.alias          := 'CUST_RV_INSGT';
            l_rpro_label_g.label          := REPLACE (INITCAP (rec_col.column_name), '_', ' ');
            l_rpro_label_g.indicators     := rpro_label_pkg.g_set_default_ind;
            l_rpro_label_g.col_type       := rec_col.data_type;
            l_rpro_label_g.crtd_dt        := SYSDATE;
            l_rpro_label_g.crtd_by        := rpro_utility_pkg.g_user;
            l_rpro_label_g.updt_dt        := SYSDATE;
            l_rpro_label_g.updt_by        := rpro_utility_pkg.g_user;
            l_rpro_label_g.client_id      := rpro_utility_pkg.g_client_id;
            l_rpro_label_g.crtd_prd_id    := l_period_id;
            l_rpro_label_g.report_label   := REPLACE (INITCAP (rec_col.column_name), '_', ' ');
            l_rpro_label_g.derived_column := rec_col.derived_column;
            l_rpro_label_g.indicators     := rpro_label_pkg.set_manual_flag (l_rpro_label_g.indicators, 'N');
            l_rpro_label_g.category       := 'All';
            
            INSERT INTO rpro_label_g
            VALUES l_rpro_label_g;
         END LOOP;
      END;
     
         FOR rec IN c1
         LOOP
            l_rpro_rep_field_g.id           := rpro_utility_pkg.generate_id('RPRO_REP_FIELD_ID_S',rpro_utility_pkg.g_client_id);
            l_rpro_rep_field_g.rep_id       := l_rep_seq;
            l_rpro_rep_field_g.label_id     := rec.id;
            
            l_rpro_rep_field_g.fld_name     := rec.derived_column;
           
            l_rpro_rep_field_g.alias        := rec.alias;
            l_rpro_rep_field_g.type         := CASE WHEN rec.col_name = 'PERIOD_NAME' THEN 'LOV' ELSE NULL END;
            l_rpro_rep_field_g.crtd_by      := rpro_utility_pkg.g_user;
            l_rpro_rep_field_g.crtd_dt      := SYSDATE;
            l_rpro_rep_field_g.updt_by      := rpro_utility_pkg.g_user;
            l_rpro_rep_field_g.updt_dt      := SYSDATE;
            l_rpro_rep_field_g.indicators   := rpro_rep_field_pkg.g_set_default_ind;
            l_rpro_rep_field_g.client_id    := rpro_utility_pkg.g_client_id;
            l_rpro_rep_field_g.crtd_prd_id  := l_period_id;
            
            IF  l_rpro_rep_field_g.fld_name IN ('PERIOD_NAME') 
            --AND l_rpro_rep_field_g.alias = 'RCR'
            THEN
               l_rpro_rep_field_g.indicators   := rpro_rep_field_pkg.set_mandatory_filter_flag(l_rpro_rep_field_g.indicators,'Y');
               l_rpro_rep_field_g.indicators   := rpro_rep_field_pkg.set_required_filter(l_rpro_rep_field_g.indicators,'Y');
               l_rpro_rep_field_g.indicators := rpro_rep_field_pkg.set_filter_flag (l_rpro_rep_field_g.indicators,'Y');
               
            END IF;
            
            INSERT INTO rpro_rep_field_g
            VALUES l_rpro_rep_field_g;
            COMMIT;
            
         END LOOP;
         
      
      
      BEGIN
         l_rpro_rp_layout_g.id           := l_lay_seq;
         l_rpro_rp_layout_g.layout_name  := l_rep_name;
         l_rpro_rp_layout_g.layout_desc  := l_rep_name;
         l_rpro_rp_layout_g.rep_id       := l_rep_seq;
         l_rpro_rp_layout_g.crtd_by      := rpro_utility_pkg.g_user;
         l_rpro_rp_layout_g.crtd_dt      := SYSDATE;
         l_rpro_rp_layout_g.updt_dt      := SYSDATE;
         l_rpro_rp_layout_g.updt_by      := rpro_utility_pkg.g_user;
         l_rpro_rp_layout_g.indicators   := rpro_rp_layout_pkg.g_set_default_ind;
         l_rpro_rp_layout_g.client_id    := rpro_utility_pkg.g_client_id;
         l_rpro_rp_layout_g.crtd_prd_id  := l_period_id;
         l_rpro_rp_layout_g.indicators   := rpro_rp_layout_pkg.set_enabled_flag (l_rpro_rp_layout_g.indicators, 'Y');
         l_rpro_rp_layout_g.indicators   := rpro_rp_layout_pkg.set_enabled_totals_flag (l_rpro_rp_layout_g.indicators, 'N');
         l_rpro_rp_layout_g.indicators   := rpro_rp_layout_pkg.set_is_summary_flag (l_rpro_rp_layout_g.indicators, 'Y');
         
         INSERT INTO rpro_rp_layout_g
         VALUES l_rpro_rp_layout_g;
         COMMIT;
      END;
      
      ---Inserting in The rpro_layout_field_g table
      BEGIN
         FOR i IN c2
         LOOP
            l_cnt := l_cnt+1;
            
            l_rpro_layout_field_g.id          := rpro_utility_pkg.generate_id('RPRO_LAYOUT_FIELD_ID_S',rpro_utility_pkg.g_client_id);
            l_rpro_layout_field_g.layout_id   := l_lay_seq;
            l_rpro_layout_field_g.field_id    := i.id;
            l_rpro_layout_field_g.seq         := l_cnt;
            l_rpro_layout_field_g.crtd_dt     := SYSDATE;
            l_rpro_layout_field_g.crtd_by     := rpro_utility_pkg.g_user;
            l_rpro_layout_field_g.updt_dt     := SYSDATE;
            l_rpro_layout_field_g.updt_by     := rpro_utility_pkg.g_user;
            l_rpro_layout_field_g.indicators  := rpro_layout_field_pkg.g_set_default_ind;
            l_rpro_layout_field_g.client_id   := rpro_utility_pkg.g_client_id;
            l_rpro_layout_field_g.crtd_prd_id := l_period_id;
            
            INSERT INTO rpro_layout_field_g
            VALUES l_rpro_layout_field_g;
            COMMIT;
            
            IF rpro_rep_field_pkg.get_filter_flag (i.indicators) ='Y'
            THEN
               l_rpro_rep_filter_g.id         := rpro_utility_pkg.generate_id('RPRO_REP_FILTER_ID_S',rpro_utility_pkg.g_client_id);
               l_rpro_rep_filter_g.layout_id  := l_lay_seq;
               l_rpro_rep_filter_g.field_id   := i.id;
               l_rpro_rep_filter_g.type       := 'LOV';
               l_rpro_rep_filter_g.val_frm    := NULL;
               l_rpro_rep_filter_g.val_to     := NULL;
               l_rpro_rep_filter_g.crtd_dt    := SYSDATE;
               l_rpro_rep_filter_g.crtd_by    := rpro_utility_pkg.g_user;
               l_rpro_rep_filter_g.updt_dt    := SYSDATE;
               l_rpro_rep_filter_g.updt_by    := rpro_utility_pkg.g_user;
               l_rpro_rep_filter_g.client_id  := rpro_utility_pkg.g_client_id;
               l_rpro_rep_filter_g.indicators := rpro_rep_filter_pkg.g_set_default_ind;
            
               INSERT INTO rpro_rep_filter_g
               VALUES l_rpro_rep_filter_g;
               COMMIT;
            END IF;
         END LOOP;
      END;
      ---To Enable Report filter Condition in the Angular UI
     BEGIN
        rpro_run_rep_ws_pkg.gen_rp_lyt_json(l_rep_seq,l_lay_seq);
     END;
      
      COMMIT;
   END;
/