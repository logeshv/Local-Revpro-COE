SET DEFINE OFF
 DECLARE 
 l_hierarchy_id      NUMBER; 
 l_prev_hierarchy_id NUMBER; 
 l_rc_row_cnt        NUMBER; 
 l_rc_tmpl_id        NUMBER; 
 l_fv_row_cnt        NUMBER; 
 l_fv_tmpl_id        NUMBER; 
 l_vc_row_cnt        NUMBER; 
 l_vc_tmpl_id        NUMBER; 
 l_vc_type_cnt       NUMBER; 
 l_vc_type_id        NUMBER; 
 l_segment_id        NUMBER; 
 l_acct_id           NUMBER; 
 l_gl_map_id         NUMBER; 
 l_lkp_id            NUMBER; 
 l_lkp_val_id        NUMBER; 
 l_upl_id            NUMBER; 
 l_upl_map_id        NUMBER; 
 l_cust_ui_cnt       NUMBER; 
 l_appr_rule_cnt     NUMBER; 
 l_appr_rule_id      NUMBER; 
 l_hold_cnt          NUMBER; 
 l_hold_id           NUMBER; 
 l_currency_cnt      NUMBER; 
 l_currency_id       NUMBER; 
 l_event_cnt         NUMBER; 
 l_event_id          NUMBER; 
 l_processor_cnt     NUMBER; 
 l_processor_id      NUMBER; 
 l_proc_stage_id     NUMBER; 
 l_org_cnt           NUMBER; 
 l_org_id            NUMBER; 
 l_book_cnt          NUMBER; 
 l_book_id           NUMBER; 
 l_function_cnt      NUMBER; 
 l_function_id       NUMBER; 
 l_user_cnt          NUMBER; 
 l_user_id           NUMBER; 
 l_role_func_id      NUMBER; 
 l_role_user_id      NUMBER; 
 l_role_book_id      NUMBER; 
 l_role_org_id       NUMBER; 
 l_role_cnt          NUMBER; 
 l_role_id           NUMBER; 
 l_pob_cond_line_hold_id NUMBER;
 l_pob_cond_line_cnt NUMBER; 
 l_pob_cond_line_id  NUMBER; 
 l_pob_rule_cond_cnt NUMBER; 
 l_pob_rule_cond_id  NUMBER; 
 l_pob_tmpl_cnt      NUMBER; 
 l_pob_tmpl_id       NUMBER; 
 l_pob_rule_cnt      NUMBER; 
 l_pob_rule_id       NUMBER; 
 l_label_id          NUMBER; 
 l_prfl_id           NUMBER; 
 l_prfl_val_id       NUMBER; 
 l_job_grp_id         NUMBER;
 l_job_grp_dtl_id     NUMBER;
l_program_cnt         NUMBER;
l_prog_detail_id      NUMBER;
l_intgrn_mpg_id       NUMBER;
l_version             NUMBER;
l_fv_version          NUMBER;
l_cost_pob_cnt        NUMBER;
l_cost_pob_type_id    NUMBER;
l_cost_id             NUMBER;
l_vc_pob_cnt          NUMBER;
l_vc_pob_type_id      NUMBER;
l_vc_id               NUMBER;
l_prnt_pob_cnt        NUMBER;
p_id_fr_chld          NUMBER;
l_pob_pr_id           NUMBER;
l_rule_id             NUMBER;
l_rule_cond_id        NUMBER;
l_cri_cnt             NUMBER;
l_cri_id              NUMBER;
l_con_hold_cnt        NUMBER;
l_attr_cnt            NUMBER;
l_pob_cond_strat_id   NUMBER;
l_item_cnt            NUMBER;
l_pob_itm_strat_id    NUMBER;
l_pob_cond_item_id    NUMBER;
l_obj_id              NUMBER;
l_ext_vc_id           NUMBER;
l_cost_cnt            NUMBER;
l_acct_tp_id          VARCHAR2(1);
l_cnt       NUMBER;
l_tsk_id      NUMBER;
l_tmpl_id     NUMBER;
l_grp_id    NUMBER;
l_tmpl_tsk_id   NUMBER;
l_var_for            NUMBER;
l_fcst_hier_cnt      NUMBER;
l_fc_hier_id     NUMBER;
l_fcst_tmpl_cnt      NUMBER;
l_fc_tmp_id      NUMBER;
l_fc_hier_schd_id     NUMBER;
l_rpt_stup_cnt      NUMBER;
l_rpt_stup_id      NUMBER;
l_rpt_fld_cnt      NUMBER;
l_rpt_fld_id      NUMBER;
l_rpt_lay_cnt      NUMBER;
l_rpt_lay_id      NUMBER;
l_rpt_lay_fld_cnt     NUMBER;
l_rpt_lay_fld_id      NUMBER;
l_rpt_fltr_cnt      NUMBER;
l_rpt_fltr_id      NUMBER;
l_rpt_acc_cnt      NUMBER;
l_fld_id      NUMBER;
l_rpt_role_id      NUMBER;
l_vc_for_id        NUMBER;
l_vc_rule_id       NUMBER;
l_vc_field_id      NUMBER;
l_ext_event_id     NUMBER;
l_formula_id       NUMBER;
l_hld_cri_id       NUMBER;
l_hier_version    NUMBER;
l_pob_version    NUMBER;
l_pob_id    NUMBER;
l_ext_cost_id  NUMBER;
l_criteria_id NUMBER;
l_aug_rl_cnt NUMBER;
l_prnt_id NUMBER;
l_fcst_id NUMBER;
l_ext_fcst_id NUMBER;
l_prnt_ver NUMBER;
l_vc_cri_id NUMBER;
l_stg_rl_ln_id  NUMBER;
l_map_id   NUMBER;
l_book_name VARCHAR2(250);
l_curr_seq   NUMBER;
l_seq   NUMBER;
l_t_form_id NUMBER;
l_cost_type_id    NUMBER;
l_cost_type_cnt   NUMBER;
l_cost_field_id   NUMBER;
l_cost_rule_id    NUMBER;
l_cost_cri_id     NUMBER;
l_sub_id          NUMBER;
l_rc_id           NUMBER;
l_id              NUMBER;
l_s_id            NUMBER;
l_sub             NUMBER;
l_exec            VARCHAR2(240);
l_dis_agg_id  NUMBER;
l_lbl_cnt        NUMBER;
l_text           VARCHAR2(2400);
l_program_id     NUMBER;
l_incompatbl_program_id  NUMBER;
l_incompatbl_id      NUMBER;
l_current_schema   VARCHAR2(100);
l_err_count     NUMBER;  
l_error_id      NUMBER;  
l_error_code    VARCHAR2(2400);  
l_rectode      NUMBER;  
l_errbuf    VARCHAR2(2400);  
l_stmt VARCHAR2(2400);
 BEGIN 
 rpro_utility_pkg.set_revpro_context; 
l_text:=NULL;
SELECT id INTO l_processor_id FROM rpro_proc_head WHERE name = q'{RC COLLECT}' ;
IF l_processor_id IS NOT NULL THEN 
 DELETE FROM RPRO_PROC_STAGE WHERE name = 'RPRO_ATL_CUSTOM_PKG.rpro_cmod_link_delink';
 l_proc_stage_id := rpro_utility_pkg.generate_id('RPRO_PROC_STAGE_ID_S', rpro_utility_pkg.g_client_id);
 INSERT INTO RPRO_PROC_STAGE (ID,PROCESSOR_ID,TYPE,NAME,SEQ,STAGE_NAME,INDICATORS,CLIENT_ID,CRTD_PRD_ID,CRTD_BY,CRTD_DT,UPDT_BY,UPDT_DT) VALUES (l_proc_stage_id,l_processor_id,'PROCEDURE','RPRO_ATL_CUSTOM_PKG.rpro_cmod_link_delink',26,'Before Collection End','YNNNNNNNNNNNNNNNNNNN',1,NULL,'MIGRATION',SYSDATE,'MIGRATION',SYSDATE);

END IF; 
COMMIT;
EXCEPTION 
WHEN NO_DATA_FOUND THEN 
ROLLBACK;
rpro_utility_pkg.record_log_act (p_type   =>'MIGRATION_ERROR'
                                      ,p_text   =>'Pre-requisite: '||l_text||'-setup is missing'
                                      ,p_log_level => 3
                                      );
raise_application_error(-20000,'Pre-requisite :'||l_text||'-setup is missing');
WHEN OTHERS THEN 
ROLLBACK;
raise_application_error(-20000,SQLERRM);
END;
/
