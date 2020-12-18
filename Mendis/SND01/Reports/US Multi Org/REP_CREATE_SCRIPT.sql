DECLARE
   CURSOR c_col
   IS
   
   SELECT column_name
         ,data_type 
   FROM(
      SELECT column_name, data_type
      FROM   all_tab_columns--user_tab_columns
      WHERE  table_name = 'RPRO_CUST_WF_TBL' 
      AND    column_name NOT IN (SELECT col_name
                                 FROM   rpro_label_g
                                 WHERE  tbl_name = 'siem_wf_sumy')
	UNION ALL
	SELECT 'CURRENCY_TYPE','VARCHAR2' FROM DUAL
	UNION ALL
	SELECT 'PERIOD_NAME','VARCHAR2' FROM DUAL);

   --Please use different name for each report. Re-use the existing transaction lables where ever possible.
   CURSOR c1
   IS
   SELECT *
   FROM RPRO_LABEL_G
   WHERE TBL_NAME = 'siem_wf_sumy';


   CURSOR c2 (l_rep_id VARCHAR2)
   IS
    SELECT *
    FROM  RPRO_REP_FIELD_G
    WHERE ALIAS = 'SIEM_WF'
	AND   fld_name IN ( 'BOOK_ID'
                      , 'SEC_ATR_VAL'
                      , 'RC_ID'
                      , 'LINE_ID'
                      , 'RC_POB_ID'
                      , 'EXT_SLL_PRC'
                      , 'EXT_LST_PRC'
					  , 'DOC_LINE_ID'
					  , 'CV_AMT'
					  , 'CUM_CV_AMT'
					  , 'CUM_ALCTD_AMT'
					  )
	AND rep_id = l_rep_id;
					   
   l_period_id   NUMBER                                ;
   l_rep_seq     NUMBER :=rpro_rep_id_s_1.NEXTVAL      ;
   l_lay_seq     NUMBER :=rpro_rp_layout_id_s_1.NEXTVAL;
   l_cnt         NUMBER :=0                            ;
BEGIN
   BEGIN
      SELECT id 
	  INTO   l_period_id 
	  FROM   rpro_period_g;            
   EXCEPTION
     WHEN OTHERS THEN
   SELECT to_number(to_char(sysdate,'yyyymm')) into l_period_id  FROM dual;
   END;

   BEGIN
      INSERT INTO RPRO_REP_G (ID,
                                     REP_NAME,
                                     REP_DESC,
                                     CATEGORY,
                                     FUNC_NAME,
                                     INDICATORS,
                                     CRTD_BY,
                                     CRTD_DT,
                                     UPDT_BY,
                                     UPDT_DT,
                                     CLIENT_ID,
                                     CRTD_PRD_ID)
           VALUES (l_rep_seq,--RPRO_REP_ID_S_1.NEXTVAL,
                   'Mendix Waterfall Report',
                   'Mendix Waterfall Report',
                   'Custom',
                   'rpro_siemens_custom_pkg.siemens_waterfall_rep',
                   'YNRNNNNNNNNNNNNNNNNN',
                   'SYSADMIN',
                   SYSDATE,
                   'SYSADMIN',
                   SYSDATE,
                   1,
                   l_period_id);

      UPDATE RPRO_REP_G
         SET indicators =
                RPRO_REP_PKG.set_enabled_flag (indicators, 'Y')
       WHERE id = l_rep_seq;--RPRO_REP_ID_S_1.CURRVAL;         --Added by gokul

      UPDATE RPRO_REP_G
         SET indicators =
                RPRO_REP_PKG.set_long_running_flag (indicators, 'N')
       WHERE id = l_rep_seq;--RPRO_REP_ID_S_1.CURRVAL;         --Added by gokul

      INSERT INTO RPRO_REP_ACCESS_G (REP_ID,
                                            ROLE_ID,
                                            CRTD_DT,
                                            CRTD_BY,
                                            UPDT_DT,
                                            UPDT_BY,
                                            CLIENT_ID)
         SELECT l_rep_seq,--RPRO_REP_ID_S_1.CURRVAL,
                GROUP_ID,
                SYSDATE,
                'SYSADMIN',
                SYSDATE,
                'SYSADMIN',
                1
           FROM rpro_groups
          WHERE 1 = 1;

      COMMIT;
   END;

   BEGIN
      FOR rec_col IN c_col
      LOOP
         INSERT INTO RPRO_LABEL_G (ID,
                                          COL_NAME,
                                          TBL_NAME,
                                          LABEL,
                                          INDICATORS,
                                          COL_TYPE,
                                          --is_manual,
                                          CRTD_DT,
                                          CRTD_BY,
                                          UPDT_DT,
                                          UPDT_BY,
                                          CLIENT_ID,
                                          CRTD_PRD_ID,
                      REPORT_LABEL
					  ,category)
              VALUES (RPRO_LABEL_ID_S_1.NEXTVAL,
                      rec_col.column_name,
                      'siem_wf_sumy',
                      REPLACE (INITCAP (rec_col.column_name), '_', ' '),
                      'NNNNNNNNNNNNNNNNNBNN',
                      rec_col.DATA_TYPE,
                      --'N',
                      SYSDATE,
                      'SYSADMIN',
                      SYSDATE,
                      'SYSADMIN',
                      1,
                      l_period_id,
                      REPLACE (INITCAP (rec_col.column_name), '_', ' ')
					  ,'ALL');

         COMMIT;
      END LOOP;

      UPDATE RPRO_LABEL_G
         SET indicators =
                RPRO_LABEL_PKG.set_manual_flag (indicators, 'N')
       WHERE TBL_NAME = 'siem_wf_sumy';  /*Added where condition by Gokul*/
   END;

   BEGIN
      FOR rec IN c1
      LOOP
         INSERT INTO RPRO_REP_FIELD_G (ID,
                                              REP_ID,
                                              LABEL_ID,
                                              FLD_NAME,
                                              --FILTER_FLAG,
                                              ALIAS,
                                              --REQUIRED_FILTER,
                                              TYPE, -- Add filter type for LOV fields.
                                              CRTD_BY,
                                              CRTD_DT,
                                              UPDT_BY,
                                              UPDT_DT,
                                              INDICATORS,
                                              CLIENT_ID,
                                              CRTD_PRD_ID)
            SELECT RPRO_REP_FIELD_ID_S_1.NEXTVAL,
                   l_rep_seq,--RPRO_REP_ID_S_1.CURRVAL,--Added by gokul
                   ID,
                   COL_NAME,
                   --'N',
                   'SIEM_WF',
                   --- 'N', ----------   Give Y for parameter column. You can use decode for these based on column_name
                   '', --'LOV',/*For LOV fields the filter type should  be 'LOV'-lOV SHOULD NOT BE GIVEN TO ALL THE COLUMNS lOV SHOLD USE ONLY FOR STANDARD COLUMNS*/
                   'SYSADMIN',
                   SYSDATE,
                   'SYSADMIN',
                   SYSDATE,
                   'NNNYNNNNNNNNNNNNNNNN',
                   1,
                   l_period_id
              FROM RPRO_LABEL_G
             WHERE COL_NAME = rec.COL_NAME AND TBL_NAME = 'siem_wf_sumy';

         COMMIT;

         /*FILTER CAN BE ADDED */
         /*UPDATE RPRO_REP_FIELD_G
         SET indicators = RPRO_REP_FIELD_PKG.set_filter_flag (indicators, 'N');*/
         --Commited by Gokul--This update will remove all the filter since there is no where clause
         UPDATE RPRO_REP_FIELD_G
            SET indicators =
                   RPRO_REP_FIELD_PKG.SET_REQUIRED_FILTER (indicators,
                                                                  'Y')
          WHERE ALIAS = 'SIEM_WF' AND fld_name IN ('PERIOD_NAME','CURRENCY_TYPE','SEC_ATR_VAL'); --Added by Gokul
         
		 UPDATE RPRO_REP_FIELD_G
            SET type =
                   'LOV'
          WHERE ALIAS = 'SIEM_WF' AND fld_name ='SEC_ATR_VAL'; /*Give the column name need to added as filter*/
		  
         UPDATE RPRO_REP_FIELD_G
            SET indicators =
                   RPRO_REP_FIELD_PKG.set_filter_flag (indicators,
                                                              'Y')
          WHERE ALIAS = 'SIEM_WF' AND fld_name IN ('PERIOD_NAME','CURRENCY_TYPE','SEC_ATR_VAL'); /*Give the column name need to added as filter*/
		  
		 

         COMMIT;
      END LOOP;
   END;

   BEGIN
      INSERT INTO RPRO_RP_LAYOUT_G (ID,
                                           LAYOUT_NAME,
                                           LAYOUT_DESC,
                                           REP_ID,
                                           CRTD_BY,
                                           CRTD_DT,
                                           UPDT_DT,
                                           UPDT_BY,
                                           INDICATORS,
                                           CLIENT_ID,
                                           CRTD_PRD_ID)
           VALUES (l_lay_seq,
                   'Mendix Waterfall Report',
                   'Mendix Waterfall Report',
                   l_rep_seq,
                   'SYSADMIN',
                   SYSDATE,
                   SYSDATE,
                   'SYSADMIN',
                   rpro_rp_layout_pkg.g_set_default_ind,
                   1,
                   l_period_id);

      UPDATE RPRO_RP_LAYOUT_G
         SET indicators =
                RPRO_RP_LAYOUT_PKG.SET_ENABLED_FLAG (indicators, 'Y')
       WHERE id = l_lay_seq;

      UPDATE RPRO_RP_LAYOUT_G
         SET indicators =
                RPRO_RP_LAYOUT_PKG.SET_ENABLED_TOTALS_FLAG (
                   indicators,
                   'N')
       WHERE id = l_lay_seq;


      UPDATE RPRO_RP_LAYOUT_G
         SET indicators =
                RPRO_RP_LAYOUT_PKG.set_is_summary_flag (indicators,
                                                               'N')
       WHERE id = l_lay_seq;

      UPDATE RPRO_RP_LAYOUT_G
         SET indicators =
                RPRO_RP_LAYOUT_PKG.set_aggregate_rep_flag (indicators,'N')
       WHERE id = l_lay_seq;
	   
      COMMIT;
   END;

   BEGIN
      FOR i IN c2(l_rep_seq)
      LOOP
      L_CNT:=L_CNT+1;
         INSERT INTO RPRO_LAYOUT_FIELD_G (ID,
                                                 LAYOUT_ID,
                                                 FIELD_ID,
                                                 SEQ,
                                                 CRTD_DT,
                                                 CRTD_BY,
                                                 UPDT_DT,
                                                 UPDT_BY,
                                                 INDICATORS,
                                                 CLIENT_ID,
                                                 CRTD_PRD_ID)
            SELECT RPRO_LAYOUT_FIELD_ID_S_1.NEXTVAL,
                   l_lay_seq,--RPRO_RP_LAYOUT_ID_S_1.NEXTVAL,
                   ID,
                   L_CNT,
                   SYSDATE,
                   'SYSADMIN',
                   SYSDATE,
                   'SYSADMIN',
                   'NNNNNNNNNNNNNNNNNNNN',
                   1,
                   l_period_id--Added by gokul
              FROM RPRO_REP_FIELD_G
             WHERE FLD_NAME = i.FLD_NAME AND ALIAS = 'SIEM_WF' AND  rep_id = l_rep_seq;
         COMMIT;
      END LOOP;

   END;
   
   INSERT INTO RPRO_REP_FILTER_G (ID,
                                  LAYOUT_ID,
                                  FIELD_ID,
                                  TYPE,
                                  VAL_FRM,
                                  VAL_TO,
                                  CRTD_DT,
                                  CRTD_BY,
                                  UPDT_DT,
                                  UPDT_BY,
                                  CLIENT_ID,
                                  INDICATORS)
    SELECT RPRO_REP_FILTER_ID_S_1.NEXTVAL,
           l_lay_seq,--RPRO_RP_LAYOUT_ID_S_1.NEXTVAL,--Added by gokul
           ID,--Added by gokul
           'LOV',
           NULL,
           NULL,
           SYSDATE,
           'SYSADMIN',
           SYSDATE,
           'SYSADMIN',
           1,
           'YNNNNNNNNNNNNNNNNNNN'
      FROM RPRO_REP_FIELD_G
     WHERE RPRO_REP_FIELD_PKG.get_filter_flag (indicators) =
              'Y'
           AND ALIAS = 'SIEM_WF'--Added by gokul
             and fld_name in ('CURRENCY_TYPE') AND  rep_id = l_rep_seq;
			 
    INSERT INTO RPRO_REP_FILTER_G (ID,
                                  LAYOUT_ID,
                                  FIELD_ID,
                                  TYPE,
                                  VAL_FRM,
                                  VAL_TO,
                                  CRTD_DT,
                                  CRTD_BY,
                                  UPDT_DT,
                                  UPDT_BY,
                                  CLIENT_ID,
                                  INDICATORS)
    SELECT RPRO_REP_FILTER_ID_S_1.NEXTVAL,
           l_lay_seq,--RPRO_RP_LAYOUT_ID_S_1.NEXTVAL,--Added by gokul
           ID,--Added by gokul
           'LOV',
           NULL,
           NULL,
           SYSDATE,
           'SYSADMIN',
           SYSDATE,
           'SYSADMIN',
           1,
           'YNNNNNNNNNNNNNNNNNNN'
      FROM RPRO_REP_FIELD_G
     WHERE RPRO_REP_FIELD_PKG.get_filter_flag (indicators) =
              'Y'
           AND ALIAS = 'SIEM_WF'--Added by gokul
             and fld_name in ('PERIOD_NAME','SEC_ATR_VAL')  AND  rep_id = l_rep_seq;
			 
			 
      UPDATE RPRO_LABEL_G SET COL_TYPE='AMOUNT' WHERE COL_NAME IN ('EXT_SLL_PRC',
                                                                   'DEF_AMT',
                                                                   'REC_AMT',
                                                                   'EXT_LST_PRC',
                                                                   'UNIT_SELL_PRC',
                                                                   'UNIT_LIST_PRC')
                                                                   AND TBL_NAME ='siem_wf_sumy';
  
   COMMIT;
   rpro_utility_pkg.set_revpro_context;
   rpro_run_rep_ws_pkg.gen_rp_lyt_json(l_rep_seq,l_lay_seq);
END;

/