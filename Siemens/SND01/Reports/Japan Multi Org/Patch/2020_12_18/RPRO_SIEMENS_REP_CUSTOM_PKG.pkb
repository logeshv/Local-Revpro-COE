CREATE OR REPLACE PACKAGE BODY RPRO_SIEMENS_REP_CUSTOM_PKG
AS
   /*===================================================================================================+
   |                              ZUORA, San Jose, California                                           |
   +====================================================================================================+
   |                                                                                                    |
   |    File Name:         RPRO_SIEMENS_REP_CUSTOM_PKG.pkb                                              |
   |    Object Name:       RPRO_SIEMENS_REP_CUSTOM_PKG                                                  |
   |                                                                                                    |
   |    Pre-reqs:                                                                                       |
   |                                                                                                    |
   |                                                                                                    |
   |    Revision History:                                                                               |
   |    Date       Name              Revision Description                                               |
   |    =========  ===============   ======== ==========================================================|
   |    16-JUL-19  Logesh Varathan   1.0      Initial                                                   |
   |    29-JUL-19  Logesh Varathan   1.1      Duplication of OOH lines fixed                 20190729   |
   |    30-JUL-19  Logesh Varathan   1.2      Added prior amount                             20190730   |
   |    31-JUL-19  Logesh Varathan   1.3      Enhancements                                   20190731   |
   |    02-AUG-19  Logesh Varathan   1.4      Enhancements to filters and NO schd check      20190802   |
   |                                          Formatted std API's                                       |
   |    06-AUG-19  Logesh Varathan   1.5      Handled change in cum_alctd_amt to rewrite the schedules  |
   |    08-AUG-19  Logesh Varathan   1.6      Handled the updated SO on initial period       20190808   |
   |                                          Added data augmentation                                   |
   |    09-AUG-19  Logesh Varathan   1.7      Enabled custom prospective calculation 20190809           |
   |    10-AUG-19  Logesh Varathan   1.7      Added LAST_DAY                                 20190810   |
   |    12-AUG-19  Sreeni            1.8      Org based changes,gc_limit,Handled exception on NO lines, |
   |                                          f_curr display                                 20190812   |
   |    13-AUG-19  Logesh Varathan   1.9      Ord type to Rev type, Run rep only if NO lines available  |
   |                                          ,OOH zero dollar amount inserted,NVL handled   20190813   |
   |    16-AUG-19  Sreeni B          2.0      NO schedules PRD_ID Null Fix and Contract Prospective     |
   |                                          Duplicate schedule fix                         20190819   |
   |    27-AUG-19  Logesh Varathan   2.1      l_open_prd_id code commented                   20190827   |
   |    28-AUG-19  Logesh Varathan   2.2      OOH prior prd catch up to current prd          20190828   |
   |    04-SEP-19  Logesh Varathan   2.3      Changed NO start date from start date to date3 20190904   |
   |    06-SEP-19  Logesh Varathan   2.4      Added date3 is not null                        20190906   |
   |    13-SEP-19  Logesh Varathan   2.5      Swapped start date to date3 for no_end_date    20190913   |
   |    17-SEP-19  Logesh Varathan   2.6      Fixed data when both no_st_date and no_end_date are same  |
   |                                          20190917                                                  |
   |    27-SEP-19  Sera/Sreeni       2.7      Changed NVL of 0 to -999  20190927                        |
   |    10-OCT-19  Logesh            2.8      Handled suspend and resume 20191011                       |
   |    14-OCT-19  Sreeni            2.9      Added extra condition for suspend-Resume 20191014         |
   |    15-OCT-19  Soundarya         3.0      20191015 fix on OOH date issue and NO schedule 0 dollar   |
   |    07-DEC-20  Manisundaram      3.1      Multi Org Changes MS20201207                              |
   |    11-DEC-20  Logesh            3.2      Logic to handle jpn current qtr change 20201211           |
   |    15-MAR-21  Logesh            3.3      Fix to qtr waterfall rep for old periods  LV20210315      |
   |    18-MAR-21  Logesh            3.4      Commented RC_ID for link/delink LV20210318                |
   |                                          Added no_schd_gen from US report LV20210318               |
   +===================================================================================================*/

   -------------------------
   ---  Global Variables ---
   -------------------------
   gc_module         CONSTANT  VARCHAR2(150)     := 'RPRO_SIEMENS_REP_CUSTOM_PKG';
   gc_log_level      CONSTANT  NUMBER            :=  2;
   g_rc_id                     rpro_rc_head.ID%TYPE;
   g_crtd_prd_id               NUMBER;
   gc_limit          CONSTANT  NUMBER          := 10;                  --20190812
   g_limit                     NUMBER          := 10000;
   g_book_id                   NUMBER          := 1;
   g_user                      VARCHAR2(200)   := rpro_utility_pkg.g_user;
   g_client_id                 NUMBER          := rpro_utility_pkg.g_client_id;
   PROCEDURE write_log(p_message  IN  VARCHAR2)
   IS
   BEGIN
      rpro_utility_pkg.record_log_act
         (p_type        => gc_module
         ,p_text        => p_message
         ,p_log_level   => gc_log_level
         ,p_line_id     => NULL
         ,p_rc_id       => g_rc_id
         ,p_pob_id      => NULL
         ,p_clob_text   => NULL);

   EXCEPTION
   WHEN OTHERS THEN
      dbms_output.put_line(' Unable to write log : '||sqlerrm);
   END write_log;
   -----------------------------------------------------------------------------
   PROCEDURE write_error(p_message  IN  VARCHAR2)
   IS
   BEGIN
      rpro_utility_pkg.record_err_act
         (p_type       =>   gc_module
         ,p_text       =>   p_message
         ,p_line_id    =>   NULL
         ,p_rc_id      =>   g_rc_id
         ,p_pob_id     =>   NULL
         ,p_clob_text  =>   NULL);

   EXCEPTION
   WHEN OTHERS THEN
      dbms_output.put_line(' Unable to write error : '||sqlerrm);
   END write_error;
   ---------------------------------------------------------------------------------------------------------------------------
   PROCEDURE insert_rc_schedules (p_trans_schedule_rec   IN OUT NOCOPY all_tab_pkg.rc_schd_rec
                                 ,p_rev_trans_schedule   IN OUT NOCOPY all_tab_pkg.rc_schd_data_tab
                                 ,p_rev_trans_line_schd  IN OUT NOCOPY all_tab_pkg.rc_line_schd_tab
                                 ,p_ca_amt               IN OUT        NUMBER
                                 ,p_cl_amt               IN OUT        NUMBER
                                 ,p_rc_head_rec          IN OUT NOCOPY rpro_rc_head%rowtype
                                 ) IS
     l_rem_amt           NUMBER                        := 0                    ;
     l_amount            NUMBER                        := 0                    ;
     l_id                NUMBER                                                ;
     l_dr_segments       rpro_rc_schd.dr_segments%TYPE                         ;
     l_proc              VARCHAR2 (30)                 := 'INSERT_RC_SCHEDULES';
     l_rel_pct           NUMBER                        := 0                    ;
     l_interfaced_flag   VARCHAR2 (1)                  := 'N'                  ;
     l_schd_type         VARCHAR2 (1)                                          ;
     l_model_indx        NUMBER                                                ;
     l_tot_pct           NUMBER                                                ;
     l_mod_amt           NUMBER                                                ;
     l_tot_mod_amt       NUMBER                                                ;
     l_mod_pct           NUMBER                                                ;
     l_rem_mod_pct       NUMBER                                                ;
     l_tot_mod_pct       NUMBER                                                ;
     l_exit_flag         VARCHAR2(1)                   := 'N'                  ;
     l_rem_mod_amt       NUMBER;
     l_pct               NUMBER;
     l_cost_model_id     NUMBER;
   BEGIN
      l_rem_amt                                     := p_trans_schedule_rec.rel_amt;
      l_rel_pct                                     := 0;
      l_tot_pct                                     := 0;
      l_tot_mod_amt                                 := 0;
      l_mod_amt                                     := 0;
      l_rem_mod_amt                                 := 0;
      l_mod_pct                                     := 0;
      l_rem_mod_pct                                 := 0;
      l_tot_mod_pct                                 := 0;
      l_interfaced_flag                             := 'N';
      l_amount                                      := l_rem_amt;
      l_dr_segments                                 := p_trans_schedule_rec.ca_segments;
      l_cost_model_id                               := NULL;

      IF p_trans_schedule_rec.ext_sll_prc = 0
      THEN
        l_rel_pct              := l_amount;
        l_amount               := 0;
        l_interfaced_flag      := 'Y';
      END IF;

       rpro_utility_pkg.record_log_act (p_type        => l_proc
                                       ,p_text        => 'Log: model_id~' || p_trans_schedule_rec.model_id || ' inserted into sch' || ' for Line ID~' || p_trans_schedule_rec.line_id
                                       ,p_log_level   => 8
                                       ,p_rc_id       => p_trans_schedule_rec.rc_id
                                       ,p_line_id     => p_trans_schedule_rec.line_id
                                       ,p_pob_id      => p_trans_schedule_rec.rc_pob_id
                                       );

      IF p_trans_schedule_rec.model_id IS NOT NULL
      THEN
         IF NOT rpro_revenue_process_pkg.g_model.EXISTS(p_trans_schedule_rec.model_id)
         THEN
             rpro_cache_setup_pkg.cache_dist_data(rpro_revenue_process_pkg.g_model,p_trans_schedule_rec.model_id);
         END IF;

         IF rpro_revenue_process_pkg.g_model.EXISTS(p_trans_schedule_rec.model_id)
         THEN
            IF NOT (p_trans_schedule_rec.cost_type_id <> 1 AND p_trans_schedule_rec.schd_type ='C')
            THEN
                rpro_utility_pkg.record_log_act (p_type         => l_proc
                                                ,p_text        => 'Log:  Inside model_id~' || p_trans_schedule_rec.model_id || ' inserted into sch' || ' for Line ID~' || p_trans_schedule_rec.line_id
                                                                   ||'p_trans_schedule_rec.schd_type'||p_trans_schedule_rec.schd_type
                                                ,p_log_level   => 8
                                                ,p_rc_id       => p_trans_schedule_rec.rc_id
                                                ,p_line_id     => p_trans_schedule_rec.line_id
                                                ,p_pob_id      => p_trans_schedule_rec.rc_pob_id
                                                );
               IF p_trans_schedule_rec.schd_type ='C'
               THEN
                  l_model_indx := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis.FIRST;
                  WHILE l_model_indx IS NOT NULL
                  LOOP
                        IF  rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).cost_model_id IS NOT NULL
                        THEN
                           l_cost_model_id := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).cost_model_id;
                        END IF;
                        IF l_cost_model_id IS NOT NULL
                        THEN
                           p_trans_schedule_rec.model_id := l_cost_model_id;
                           IF NOT rpro_revenue_process_pkg.g_model.EXISTS(p_trans_schedule_rec.model_id)
                           THEN
                              rpro_cache_setup_pkg.cache_dist_data(rpro_revenue_process_pkg.g_model,p_trans_schedule_rec.model_id);
                           END IF;
                           EXIT;
                        END IF;
                     l_model_indx := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis.NEXT(l_model_indx);
                  END LOOP;
               END IF;

               l_model_indx := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis.FIRST;
               WHILE l_model_indx IS NOT NULL
               LOOP
                  l_pct :=rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).pct;
                  l_tot_pct := l_tot_pct + l_pct;
                  IF (l_tot_pct >= 100)
                  THEN
                     l_mod_amt  := SIGN (l_amount) * (ABS(l_amount) - ABS(l_tot_mod_amt));
                     l_mod_pct  := l_rel_pct - l_tot_mod_pct;
                  ELSE
                     l_mod_amt  := ROUND( l_amount * (l_pct / 100), NVL(p_trans_schedule_rec.rounding,4));
                     l_mod_pct  :=l_rel_pct * (l_pct / 100);
                  END IF;
                  l_tot_mod_amt := SIGN (l_amount) * ( ABS(l_tot_mod_amt) + ABS(l_mod_amt));
                  l_tot_mod_pct := l_tot_mod_pct + l_mod_pct;

                  IF ABS(l_tot_mod_amt) > ABS(l_amount)
                  THEN
                    l_mod_amt := SIGN(l_amount) *(ABS(l_mod_amt) - (ABS(l_tot_mod_amt) - ABS(l_amount)));
                    l_tot_mod_amt := l_amount;
                    l_exit_flag   := 'Y';
                  END IF;

                  IF p_trans_schedule_rec.ext_sll_prc = 0
                  AND l_tot_mod_pct > l_rel_pct
                  THEN
                    l_mod_pct     := l_mod_pct - (l_rel_pct - l_tot_mod_pct);
                    l_tot_mod_pct := l_mod_pct;
                    l_exit_flag   := 'Y';
                  END IF;

                  l_id                                          := rpro_cust_rc_schd_id_s.nextval;
                  p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.g_set_default_ind ;--Added
                  p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_dr_acctg_flag(p_rev_trans_schedule (l_id).indicators,'L');
                  p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_cr_acctg_flag(p_rev_trans_schedule (l_id).indicators,'R');
                  p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_netting_entry_flag(p_rev_trans_schedule (l_id).indicators,'R');
                  p_rev_trans_line_schd(p_trans_schedule_rec.line_id)(p_trans_schedule_rec.schd_type)(l_id).ID  := l_id;
                  p_rev_trans_schedule (l_id).ID                := l_id;
                  p_rev_trans_schedule (l_id).rel_id            := p_trans_schedule_rec.rel_id;
                  p_rev_trans_schedule (l_id).rc_id             := p_trans_schedule_rec.rc_id;
                  p_rev_trans_schedule (l_id).rc_ver            := p_trans_schedule_rec.rc_ver;
                  p_rev_trans_schedule (l_id).line_id           := p_trans_schedule_rec.line_id;
                  p_rev_trans_schedule (l_id).orig_line_id      := p_trans_schedule_rec.orig_line_id;
                  p_rev_trans_schedule (l_id).root_line_id      := p_trans_schedule_rec.root_line_id;
                  p_rev_trans_schedule (l_id).ref_bill_id       := p_trans_schedule_rec.ref_bill_id;
                  p_rev_trans_schedule (l_id).pob_id            := p_trans_schedule_rec.pob_id;
                  p_rev_trans_schedule (l_id).curr              := p_trans_schedule_rec.curr;
                  p_rev_trans_schedule (l_id).amount            := l_mod_amt;
                  p_rev_trans_schedule (l_id).rel_pct           := l_mod_pct;
                  p_rev_trans_schedule (l_id).prd_id            := p_trans_schedule_rec.prd_id;
                  p_rev_trans_schedule (l_id).post_prd_id       := p_trans_schedule_rec.prd_id;
                  p_rev_trans_schedule (l_id).bld_fx_rate       := p_trans_schedule_rec.bld_fx_rate;
                  p_rev_trans_schedule (l_id).bld_fx_dt         := p_trans_schedule_rec.bld_fx_dt;
                  p_rev_trans_schedule (l_id).pp_amt            := nvl(p_trans_schedule_rec.pp_amt,0) * (l_pct / 100);
                  p_rev_trans_schedule (l_id).pq_amt            := nvl(p_trans_schedule_rec.pq_amt,0) * (l_pct / 100);
                  p_rev_trans_schedule (l_id).py_amt            := nvl(p_trans_schedule_rec.py_amt,0) * (l_pct / 100);

                  IF p_trans_schedule_rec.schd_type ='R'
                  THEN
                     IF nvl(p_trans_schedule_rec.def_act_type,'N') ='N'
                     THEN
                        p_rev_trans_schedule (l_id).dr_segments       := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).def_segs ;
                     ELSE
                        p_rev_trans_schedule (l_id).dr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).def_segs, p_trans_schedule_rec.def_act_type,p_trans_schedule_rec.def_segments);
                     END IF;

                     IF nvl(p_trans_schedule_rec.rev_act_type,'N') ='N'
                     THEN
                        p_rev_trans_schedule (l_id).cr_segments       := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).rev_segs;
                     ELSE
                        p_rev_trans_schedule (l_id).cr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).rev_segs, p_trans_schedule_rec.rev_act_type,p_trans_schedule_rec.rev_segments);
                     END IF;

                     IF  p_trans_schedule_rec.nat_acct_seg IS NOT NULL
                     THEN
                        p_rev_trans_schedule (l_id).cr_segments    :=rpro_utility_pkg.get_segment_str ( p_rev_trans_schedule (l_id).cr_segments
                                                                                                        ,p_trans_schedule_rec.nat_acct_seg
                                                                                                       );
                     END IF;
                  ELSIF p_trans_schedule_rec.schd_type ='A'
                  THEN
                     IF nvl(p_trans_schedule_rec.initial_rep_entry_flag,'N') ='Y'
                     THEN
                       p_rev_trans_schedule (l_id).dr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).rev_segs, rpro_acct_type_pkg.g_adjustment_revenue,p_trans_schedule_rec.def_segments );
                       p_rev_trans_schedule (l_id).cr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).def_segs, rpro_acct_type_pkg.g_adjustment_liability ,p_trans_schedule_rec.rev_segments);
                     ELSE
                       p_rev_trans_schedule (l_id).dr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).def_segs, p_trans_schedule_rec.def_act_type,p_trans_schedule_rec.def_segments);
                       p_rev_trans_schedule (l_id).cr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).rev_segs, p_trans_schedule_rec.rev_act_type,p_trans_schedule_rec.rev_segments);
                     END IF;
                  ELSE

                     IF NVL(p_trans_schedule_rec.def_act_type,'N') ='N'
                     THEN
                       p_rev_trans_schedule (l_id).dr_segments       := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).rcogs_segs;
                     ELSE
                       p_rev_trans_schedule (l_id).dr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).rcogs_segs, p_trans_schedule_rec.def_act_type,p_trans_schedule_rec.def_segments);
                     END IF;

                     IF NVL(p_trans_schedule_rec.rev_act_type,'N') ='N'
                     THEN
                       p_rev_trans_schedule (l_id).cr_segments       := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).dcogs_segs ;
                     ELSE
                       p_rev_trans_schedule (l_id).cr_segments       := rpro_utility_pkg.get_acct_sgmt_for_dist (rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis(l_model_indx).dcogs_segs, p_trans_schedule_rec.rev_act_type,p_trans_schedule_rec.rev_segments);
                     END IF;
                  END IF;
                  p_rev_trans_schedule (l_id).f_ex_rate         := p_trans_schedule_rec.f_ex_rate;
                  p_rev_trans_schedule (l_id).g_ex_rate         := p_trans_schedule_rec.g_ex_rate;
                  p_rev_trans_schedule (l_id).ex_rate_date      := p_trans_schedule_rec.ex_rate_date;
                  p_rev_trans_schedule (l_id).atr1              := to_char(to_date(substr(p_trans_schedule_rec.prd_id,5,2), 'MM'), 'MON');--Added
                  p_rev_trans_schedule (l_id).atr2              := substr(p_trans_schedule_rec.prd_id,1,4); --Added
                  p_rev_trans_schedule (l_id).atr3              := 'NO';
                  p_rev_trans_schedule (l_id).atr4              := p_trans_schedule_rec.atr4;
                  p_rev_trans_schedule (l_id).atr5              := p_trans_schedule_rec.atr5;
                  p_rev_trans_schedule (l_id).pp_amt            := nvl(p_trans_schedule_rec.pp_amt,0);
                  p_rev_trans_schedule (l_id).pq_amt            := nvl(p_trans_schedule_rec.pq_amt,0);
                  p_rev_trans_schedule (l_id).py_amt            := nvl(p_trans_schedule_rec.py_amt,0);
                  p_rev_trans_schedule (l_id).client_id         := rpro_utility_pkg.g_client_id;
                  p_rev_trans_schedule (l_id).crtd_prd_id       := rpro_utility_pkg.get_crtd_prd_id (p_trans_schedule_rec.book_id, p_trans_schedule_rec.sec_atr_val);
                  p_rev_trans_schedule (l_id).sec_atr_val       := p_trans_schedule_rec.sec_atr_val;
                  p_rev_trans_schedule (l_id).crtd_by           := p_trans_schedule_rec.crtd_by;
                  p_rev_trans_schedule (l_id).crtd_dt           := SYSDATE;
                  p_rev_trans_schedule (l_id).rord_inv_ref      := p_trans_schedule_rec.rord_inv_ref;
                  p_rev_trans_schedule (l_id).updt_by           := p_trans_schedule_rec.crtd_by;
                  p_rev_trans_schedule (l_id).updt_dt           := SYSDATE;
                  p_rev_trans_schedule (l_id).book_id           := p_trans_schedule_rec.book_id;
                  p_rev_trans_schedule (l_id).model_id          := p_trans_schedule_rec.model_id;
                  p_rev_trans_schedule (l_id).dist_id           := l_model_indx;
                  p_rev_trans_schedule (l_id).je_batch_id       := p_trans_schedule_rec.je_batch_id;
                  p_rev_trans_schedule (l_id).je_batch_name     := p_trans_schedule_rec.je_batch_name;
                  p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_multiple_flag (rpro_rc_schd_pkg.g_set_default_ind
                                                                                                       ,   'INTERFACED:'
                                                                                                        || l_interfaced_flag
                                                                                                        || '#DR_ACCTG:'
                                                                                                        || p_trans_schedule_rec.def_act_type
                                                                                                        || '#CR_ACCTG:'
                                                                                                        || p_trans_schedule_rec.rev_act_type
                                                                                                        || '#SCHD_TYPE:'
                                                                                                        || p_trans_schedule_rec.schd_type
                                                                                                        || '#INITIAL_REP_ENTRY:'
                                                                                                        || NVL (p_trans_schedule_rec.initial_rep_entry_flag, 'N')
                                                                                                        || '#IMPACT_TRANS_PRC:'
                                                                                                        || NVL (p_trans_schedule_rec.impact_trans_prc_flag, 'N')
                                                                                                        ||'#PP_ADJ:'
                                                                                                              ||NVL(p_trans_schedule_rec.pp_adj,'N')
                                                                                                        || '#PQ_ADJ:'
                                                                                                        || NVL (p_trans_schedule_rec.pq_adj, 'N')
                                                                                                        || '#RETRO_ENTRY:'
                                                                                                        || NVL (p_trans_schedule_rec.retro_entry_flag, 'N')
                                                                                                      );
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_pq_adj_flag(p_rev_trans_schedule (l_id).indicators,NVL (p_trans_schedule_rec.pq_adj, 'N'));
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_line_type_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.line_type, 'N'));
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_unbilled_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.unbill_flag, 'N'));
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_reallocation_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.realloc_flag, 'N'));
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_pord_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.pord_flag, 'N'));
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_unbill_rvrsl_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.unbill_rvrsl_flag, 'N'));
                  p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_update_or_insert_flag (p_rev_trans_schedule (l_id).indicators,'I');
                  l_rem_mod_amt := SIGN(l_amount) * (ABS(l_amount) - ABS(l_tot_mod_amt));
                  l_rem_mod_pct := l_rel_pct - l_tot_mod_pct;

                  IF l_exit_flag = 'Y'
                   THEN
                     EXIT;
                  END IF;

                  l_model_indx := rpro_revenue_process_pkg.g_model(p_trans_schedule_rec.model_id).dis.NEXT(l_model_indx);
               END LOOP;
               IF ABS(l_rem_mod_amt) > 0
               THEN
                  l_amount := l_rem_mod_amt;
               ELSE
                l_amount := 0;
               END IF;

               IF l_rem_mod_pct > 0 AND p_trans_schedule_rec.ext_sll_prc = 0
               THEN
                 l_rel_pct := l_rem_mod_pct;
               ELSE
                 l_rel_pct := 0;
               END IF;

            END IF;
         END IF;
      END IF;
      IF (l_amount <> 0 OR l_rel_pct <> 0)
      THEN
         l_id                                          := rpro_cust_rc_schd_id_s.NEXTVAL;
         p_rev_trans_line_schd(p_trans_schedule_rec.line_id)(p_trans_schedule_rec.schd_type)(l_id).ID  := l_id;
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.g_set_default_ind ;--Added
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_update_or_insert_flag (p_rev_trans_schedule (l_id).indicators,'I');--Added
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_dr_acctg_flag(p_rev_trans_schedule (l_id).indicators,'L');
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_cr_acctg_flag(p_rev_trans_schedule (l_id).indicators,'R');
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_netting_entry_flag(p_rev_trans_schedule (l_id).indicators,'R');
         p_rev_trans_schedule (l_id).ID                := l_id;
         p_rev_trans_schedule (l_id).rel_id            := p_trans_schedule_rec.rel_id;
         p_rev_trans_schedule (l_id).rc_id             := p_trans_schedule_rec.rc_id;
         p_rev_trans_schedule (l_id).rc_ver            := p_trans_schedule_rec.rc_ver;
         p_rev_trans_schedule (l_id).line_id           := p_trans_schedule_rec.line_id;
         p_rev_trans_schedule (l_id).orig_line_id      := p_trans_schedule_rec.orig_line_id;
         p_rev_trans_schedule (l_id).root_line_id      := p_trans_schedule_rec.root_line_id;
         p_rev_trans_schedule (l_id).ref_bill_id       := p_trans_schedule_rec.ref_bill_id;
         p_rev_trans_schedule (l_id).pob_id            := p_trans_schedule_rec.pob_id;
         p_rev_trans_schedule (l_id).curr              := p_trans_schedule_rec.curr;
         p_rev_trans_schedule (l_id).amount            := l_amount;
         p_rev_trans_schedule (l_id).rel_pct           := l_rel_pct;
         p_rev_trans_schedule (l_id).prd_id            := p_trans_schedule_rec.prd_id;
         p_rev_trans_schedule (l_id).post_prd_id       := p_trans_schedule_rec.prd_id;
         p_rev_trans_schedule (l_id).bld_fx_rate       := p_trans_schedule_rec.bld_fx_rate;
         p_rev_trans_schedule (l_id).bld_fx_dt         := p_trans_schedule_rec.bld_fx_dt;

         p_rev_trans_schedule (l_id).dr_segments       := p_trans_schedule_rec.def_segments;
         p_rev_trans_schedule (l_id).cr_segments       := p_trans_schedule_rec.rev_segments;

         IF  p_trans_schedule_rec.nat_acct_seg IS NOT NULL AND p_trans_schedule_rec.schd_type ='R'
         THEN
            p_rev_trans_schedule (l_id).cr_segments    :=rpro_utility_pkg.get_segment_str ( p_rev_trans_schedule (l_id).cr_segments
                                                                                          , p_trans_schedule_rec.nat_acct_seg
                                                                                          );
         END IF;

         p_rev_trans_schedule (l_id).f_ex_rate         := p_trans_schedule_rec.f_ex_rate;
         p_rev_trans_schedule (l_id).g_ex_rate         := p_trans_schedule_rec.g_ex_rate;
         p_rev_trans_schedule (l_id).ex_rate_date      := p_trans_schedule_rec.ex_rate_date;
         p_rev_trans_schedule (l_id).atr1              := TO_CHAR(TO_DATE(SUBSTR(p_trans_schedule_rec.prd_id,5,2), 'MM'), 'MON');
         p_rev_trans_schedule (l_id).atr2              := SUBSTR(p_trans_schedule_rec.prd_id,1,4);
         p_rev_trans_schedule (l_id).atr3              := 'NO';
         p_rev_trans_schedule (l_id).atr4              := p_trans_schedule_rec.atr4;
         p_rev_trans_schedule (l_id).atr5              := p_trans_schedule_rec.atr5;
         p_rev_trans_schedule (l_id).client_id         := rpro_utility_pkg.g_client_id;
         p_rev_trans_schedule (l_id).crtd_prd_id       := rpro_utility_pkg.get_crtd_prd_id (p_trans_schedule_rec.book_id, p_trans_schedule_rec.sec_atr_val);
         p_rev_trans_schedule (l_id).sec_atr_val       := p_trans_schedule_rec.sec_atr_val;
         p_rev_trans_schedule (l_id).crtd_by           := p_trans_schedule_rec.crtd_by;
         p_rev_trans_schedule (l_id).crtd_dt           := SYSDATE;
         p_rev_trans_schedule (l_id).rord_inv_ref      := p_trans_schedule_rec.rord_inv_ref;
         p_rev_trans_schedule (l_id).updt_by           := p_trans_schedule_rec.crtd_by;
         p_rev_trans_schedule (l_id).updt_dt           := SYSDATE;
         p_rev_trans_schedule (l_id).book_id           := p_trans_schedule_rec.book_id;
         p_rev_trans_schedule (l_id).pp_amt            := NVL(p_trans_schedule_rec.pp_amt,0);
         p_rev_trans_schedule (l_id).pq_amt            := NVL(p_trans_schedule_rec.pq_amt,0);
         p_rev_trans_schedule (l_id).py_amt            := NVL(p_trans_schedule_rec.py_amt,0);
         p_rev_trans_schedule (l_id).dist_id           := p_trans_schedule_rec.dist_id;
         p_rev_trans_schedule (l_id).je_batch_id       := p_trans_schedule_rec.je_batch_id;
         p_rev_trans_schedule (l_id).je_batch_name     := p_trans_schedule_rec.je_batch_name;
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_pq_adj_flag(p_rev_trans_schedule (l_id).indicators,NVL (p_trans_schedule_rec.pq_adj, 'N'));
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_line_type_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.line_type, 'N'));
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_unbilled_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.unbill_flag, 'N'));
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_reallocation_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.realloc_flag, 'N'));
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_pord_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.pord_flag, 'N'));
         p_rev_trans_schedule (l_id).indicators        := rpro_rc_schd_pkg.set_unbill_rvrsl_flag(p_rev_trans_schedule (l_id).indicators, NVL (p_trans_schedule_rec.unbill_rvrsl_flag, 'N'));

         IF nvl(p_trans_schedule_rec.rev_rec_act_evt,'N') = 'Y'
         THEN
            p_rev_trans_schedule (l_id).indicators  := rpro_rc_schd_pkg.set_rec_evt_act_flag(p_rev_trans_schedule (l_id).indicators, 'Y');
         END IF;

         rpro_utility_pkg.record_log_act (p_type        => l_proc
                                         ,p_text        => 'Log: ID~' || l_id || ' inserted into sch' || ' for Line ID~' || p_trans_schedule_rec.line_id
                                         ,p_log_level   => 8
                                         ,p_rc_id       => p_trans_schedule_rec.rc_id
                                         ,p_line_id     => p_trans_schedule_rec.line_id
                                         ,p_pob_id      => p_trans_schedule_rec.rc_pob_id
                                         );
      END IF;
   END insert_rc_schedules;
   ---------------------------------------------------------------------------------------------------------------------------
   PROCEDURE sync_rc_schd_data ( p_rc_schd_data IN OUT NOCOPY all_tab_pkg.rc_schd_data_tab
                               , p_date         IN DATE DEFAULT SYSDATE)
   IS
      TYPE indx_tab IS TABLE OF PLS_INTEGER
                       INDEX BY PLS_INTEGER;


      l_inst_indx_tab   indx_tab;
      l_del_indx_tab    indx_tab;
      l_updt_indx_tab   indx_tab;

      l_proc            VARCHAR2 (20) := 'SYNC_RC_SCHD_DATA';
      l_indx            NUMBER;

      l_inst_indx       NUMBER := 0;
      l_del_indx        NUMBER := 0;
      l_updt_indx       NUMBER := 0;
      l_rc_head_rec     all_tab_pkg.rc_head_data_tab;
      l_ecount          NUMBER := 0;
      l_curr_prd_id     NUMBER;
   BEGIN
      IF p_rc_schd_data.count > 0
      THEN

         rpro_utility_pkg.record_log_act (p_type        => l_proc
                                         ,p_text        => 'INPUT COUNT :' || p_rc_schd_data.count
                                         ,p_log_level   => 10
                                         );

         l_indx      := p_rc_schd_data.FIRST;

         WHILE l_indx IS NOT NULL LOOP
            IF rpro_rc_schd_pkg.get_update_or_insert_flag (p_rc_schd_data (l_indx).indicators) = 'I'
            THEN
               p_rc_schd_data (l_indx).indicators      := rpro_rc_schd_pkg.set_update_or_insert_flag ( p_rc_schd_data (l_indx).indicators, 'N');
               p_rc_schd_data (l_indx).updt_by         := rpro_utility_pkg.g_user;
               p_rc_schd_data (l_indx).updt_dt         := p_date;
               p_rc_schd_data (l_indx).crtd_by         := rpro_utility_pkg.g_user;
               p_rc_schd_data (l_indx).crtd_dt         := p_date;
               l_inst_indx                             := l_inst_indx + 1;
               l_inst_indx_tab (l_inst_indx)           := l_indx;
            ELSIF rpro_rc_schd_pkg.get_update_or_insert_flag (p_rc_schd_data (l_indx).indicators) = 'U'
            THEN
               p_rc_schd_data (l_indx).indicators      := rpro_rc_schd_pkg.set_update_or_insert_flag ( p_rc_schd_data (l_indx).indicators, 'N');
               p_rc_schd_data (l_indx).updt_by         := rpro_utility_pkg.g_user;
               p_rc_schd_data (l_indx).updt_dt         := p_date;
               l_updt_indx                             := l_updt_indx + 1;
               l_updt_indx_tab (l_updt_indx)           := l_indx;
            ELSIF rpro_rc_schd_pkg.get_update_or_insert_flag (p_rc_schd_data (l_indx).indicators) = 'D'
            THEN
               l_del_indx                       := l_del_indx + 1;
               l_del_indx_tab (l_del_indx)      := l_indx;
            ELSIF rpro_rc_schd_pkg.get_update_or_insert_flag (p_rc_schd_data (l_indx).indicators) = 'E'
            THEN
               l_ecount := l_ecount + 1;
            END IF;

            l_indx      := p_rc_schd_data.NEXT (l_indx);
         END LOOP;

         IF l_del_indx_tab.count > 0
         THEN
             FORALL l_indx IN VALUES OF l_del_indx_tab
             DELETE FROM rpro_cust_rc_schd
             WHERE id        = p_rc_schd_data (l_indx).id
             AND   client_id = rpro_utility_pkg.g_client_id;
         END IF;

         IF l_inst_indx_tab.COUNT > 0
         THEN
            FORALL l_indx IN VALUES OF l_inst_indx_tab
            INSERT INTO rpro_cust_rc_schd
            VALUES p_rc_schd_data (l_indx);
         END IF;
      END IF;

      IF l_updt_indx_tab.COUNT > 0
      THEN
        FORALL l_indx IN VALUES OF l_updt_indx_tab
           UPDATE rpro_cust_rc_schd
           SET ROW  = p_rc_schd_data (l_indx)
           WHERE id = p_rc_schd_data (l_indx).id
           AND client_id = rpro_utility_pkg.g_client_id;
      END IF;


      rpro_utility_pkg.record_log_act (p_type        => l_proc
                                      ,p_text        => 'INSERTED ' || l_inst_indx_tab.count || ' lines. Deleted ' || l_del_indx_tab.count || ' lines. Updated ' || l_updt_indx_tab.count || ' lines. Excluded '||l_ecount||' lines.'
                                      ,p_log_level   => 10
                                      );

      l_inst_indx_tab.DELETE;
      l_updt_indx_tab.DELETE;
      l_del_indx_tab.DELETE;
      p_rc_schd_data.DELETE;
   EXCEPTION
     WHEN OTHERS THEN
       ROLLBACK;
       l_inst_indx_tab.DELETE;
       l_updt_indx_tab.DELETE;
       l_del_indx_tab.DELETE;
       rpro_utility_pkg.record_err_act ( p_type => l_proc, p_text => 'ERROR: ' || SQLERRM);
       RAISE;
   END sync_rc_schd_data;
   ---------------------------------------------------------------------------------------------------------------------------

   ---------------------------------------------------------------------------------------------------------------------------
   PROCEDURE contract_recognize_revenue ( p_rc_schd_rec        IN OUT NOCOPY all_tab_pkg.rc_schd_rec
                                        , p_rc_schd_tab        IN OUT NOCOPY rpro_revenue_process_pkg.rc_contract_schd_tab
                                        , p_rc_schd            IN OUT NOCOPY all_tab_pkg.rc_schd_data_tab
                                        ) AS
      l_process_name               VARCHAR2 (30);
      l_num_days                   NUMBER;
      l_per_day_amt                NUMBER;
      l_first_month_start_date     DATE;
      l_last_month_end_date        DATE;
      l_full_periods               NUMBER;
      l_per_month_amt              NUMBER;
      l_period_amt                 NUMBER;
      l_period_start_date          DATE;
      l_amount                     NUMBER;
      l_total_amt                  NUMBER;
      l_accum_rev_amt              NUMBER;
      l_rc_schd_rec                all_tab_pkg.rc_schd_rec;
      l_num_mins                   NUMBER;
      l_per_min_amt                NUMBER;
      l_total_order_line_amt       NUMBER;
      l_total_order_line_rec_amt   NUMBER;
      l_accum_order_amt            NUMBER;
      l_actual_recog_amt           NUMBER;
      l_max_recog_amt              NUMBER;
      l_period_tab                 rpro_utility_pkg.rc_calender_tab;
      l_prd_indx                   NUMBER;
      l_schd_indx                  NUMBER;
      l_prv_qtr_end_prd            NUMBER;
      l_prv_prd                    NUMBER;
      l_prv_yr_prd                 NUMBER;
      l_pp_amt                     NUMBER;
      l_pq_amt                     NUMBER;
      l_py_amt                     NUMBER;
      --Temp Parameters
      --p_rc_schd                    all_tab_pkg.rc_schd_data_tab;
      p_ca_amt                     NUMBER := 0;
      p_cl_amt                     NUMBER := 0;
      p_rc_line_schd               all_tab_pkg.rc_line_schd_tab;
      p_rc_head_rec                rpro_rc_head%rowtype;

   BEGIN
      write_log('begin');

      l_pp_amt := 0;
      l_pq_amt := 0;
      l_py_amt := 0;
      IF p_rc_schd_rec.ext_sll_prc = 0 AND NVL(rpro_rc_collect_pkg.g_contractual_prospective,'N')='Y'
      THEN
         l_total_order_line_amt          := ABS(NVL(p_rc_schd_rec.contract_amt,0));
      ELSIF p_rc_schd_rec.ext_sll_prc = 0 AND NVL(rpro_rc_collect_pkg.g_contractual_prospective,'N')='N'
      THEN
         l_total_order_line_amt      := 0;
      ELSE
         l_total_order_line_rec_amt      := p_rc_schd_rec.contract_rec_amt;
         l_total_order_line_amt          := ABS(p_rc_schd_rec.contract_amt);
      END IF;





      IF (l_total_order_line_amt = 0)
      THEN
         l_total_order_line_rec_amt      := 0;
         l_total_order_line_amt          := 100;
      END IF;

      l_process_name          := 'CONTRACT_RECOGNIZE_REVENUE';
      l_amount                := ABS (p_rc_schd_rec.rel_amt);
      l_rc_schd_rec           := p_rc_schd_rec;

      rpro_revenue_process_pkg.g_sysdate := rpro_utility_pkg.get_sysdate ( p_book_id     => p_rc_schd_rec.book_id
                                                                         , p_sec_atr_val => p_rc_schd_rec.sec_atr_val);

      write_log ('Log: p_rc_schd_rec.rule_start_date : '||p_rc_schd_rec.rule_start_date
                ||chr(10)
                ||'p_rc_schd_rec.rule_end_date : '      ||p_rc_schd_rec.rule_end_date
                ||chr(10)
                ||'p_rc_schd_rec.rev_rec_type : '       ||p_rc_schd_rec.rev_rec_type
                ||chr(10)
                ||'p_rc_schd_rec.rounding : '           ||p_rc_schd_rec.rounding
                ||chr(10)
                ||'l_total_order_line_rec_amt : '       ||l_total_order_line_rec_amt
                ||chr(10)
                ||'l_total_order_line_amt : '           ||l_total_order_line_amt);





      l_num_days              := p_rc_schd_rec.rule_end_date - p_rc_schd_rec.rule_start_date + 1;
      l_per_day_amt           := l_total_order_line_amt / l_num_days;

      rpro_utility_pkg.get_periods (p_rc_schd_rec.rule_start_date
                                   ,p_rc_schd_rec.rule_end_date
                                   ,l_period_tab
                                   );

      write_log('Log:g_periods' || rpro_utility_pkg.g_periods.COUNT
               ||'~l_period_tab.COUNT~'||l_period_tab.COUNT
               ||'~l_num_days~'||l_num_days
               ||'~l_per_day_amt~'||l_per_day_amt);




      l_total_amt             := 0;
      l_accum_rev_amt         := 0;
      l_accum_order_amt       := 0;
      l_max_recog_amt         := 0;
      l_prv_qtr_end_prd       := rpro_utility_pkg.get_prv_qtr_end_prd(rpro_revenue_process_pkg.g_sysdate);
      l_prv_prd               := rpro_utility_pkg.get_prv_prd(rpro_revenue_process_pkg.g_sysdate);
      l_prv_yr_prd            := rpro_utility_pkg.get_prv_yr_end_prd(rpro_revenue_process_pkg.g_sysdate);

      l_prd_indx              := l_period_tab.FIRST;



      WHILE (l_prd_indx IS NOT NULL)
      LOOP
         write_log('Log:Working On prd_id~' || l_period_tab (l_prd_indx).ID);

         write_log('l_period_tab (l_prd_indx).start_date :'
                  ||l_period_tab (l_prd_indx).start_date
                  ||'p_rc_schd_rec.rule_start_date:'||p_rc_schd_rec.rule_start_date
                  ||'~l_period_amt~'                ||l_period_amt
                  ||'~l_amount~'                    ||l_amount
                  ||'~l_total_amt~'                 ||l_total_amt);

         IF (l_period_tab (l_prd_indx).start_date <= p_rc_schd_rec.rule_start_date)
         THEN
             write_log('Log: prd start_date <= rule_ start_date');

             l_period_amt      := ROUND ( (l_period_tab (l_prd_indx).end_date - p_rc_schd_rec.rule_start_date + 1) * l_per_day_amt, p_rc_schd_rec.rounding);
         ELSE
              write_log('Log: prd start_date > rule_ start_date');
             l_period_amt      := ROUND ( (l_period_tab (l_prd_indx).end_date - l_period_tab (l_prd_indx).start_date + 1) * l_per_day_amt, p_rc_schd_rec.rounding);
         END IF;

         IF (l_period_tab (l_prd_indx).end_date >= p_rc_schd_rec.rule_end_date)
         THEN
            write_log('Log: prd end_date > rule_end_date');
            l_period_amt      := GREATEST (l_period_amt, (l_amount - l_total_amt));
         END IF;

         IF l_period_amt = 0
         THEN
            l_period_amt := 1 / POWER (10,p_rc_schd_rec.rounding);
         END IF;

         write_log('Log: p_transaction_id- l_period_amt - l_amount-l_total_amt-p_rc_schd_rec.rev_rec_type -: '
                              || l_period_amt
                              || '-'
                              || l_amount
                              || '-'
                              || l_total_amt
                              || '-'
                              || p_rc_schd_rec.rev_rec_type);


         l_actual_recog_amt      := 0;
         l_max_recog_amt         := l_max_recog_amt + l_period_amt;

         IF p_rc_schd_tab.EXISTS (p_rc_schd_rec.rev_act_type || '-' || l_period_tab (l_prd_indx).ID || '~' || p_rc_schd_rec.schd_type)
         THEN
            write_log('Exists p_rc_schd_rec.rev_act_type~ '||p_rc_schd_rec.rev_act_type
                                                                  || '~l_period_tab (l_prd_indx).ID~'||l_period_tab (l_prd_indx).ID);
            l_actual_recog_amt      := ABS(NVL (p_rc_schd_tab (p_rc_schd_rec.rev_act_type || '-' || l_period_tab (l_prd_indx).ID || '~' || p_rc_schd_rec.schd_type).amount, 0));
         END IF;
         --20190827 waterfall issue
         /*
         IF (l_period_tab (l_prd_indx).ID > l_rc_schd_rec.open_prd_id)
         THEN
            IF (l_actual_recog_amt <= l_period_amt)
            THEN
              l_period_amt      := l_period_amt - l_actual_recog_amt;
            ELSE
              l_period_amt      := 0;
            END IF;
         ELSIF ( (l_period_tab (l_prd_indx).ID = l_rc_schd_rec.open_prd_id))
         THEN
            l_accum_rev_amt      := l_accum_rev_amt + l_period_amt;

            IF ( (l_max_recog_amt - l_actual_recog_amt) <= 0)
            THEN
               l_accum_rev_amt      := 0;
            ELSE
               IF (l_accum_rev_amt > (l_max_recog_amt - l_actual_recog_amt))
               THEN
                  l_accum_rev_amt      := (l_max_recog_amt - l_actual_recog_amt);
               END IF;
            END IF;

            l_period_amt         := l_accum_rev_amt;
            l_accum_rev_amt      := 0;
         END IF;
         */
         write_log('Log before Revenue p_transaction_id- OPEN_PERIOD_START_DATE - l_accum_rev_amt-l_period_amt
                           - period_start_date -period_end_date-p_rule_end_date-l_max_recog_amt-l_actual_recog_amt-l_rc_schd_rec.open_prd_id '
                              || l_rc_schd_rec.open_cur_start_dt
                              || '-'
                              || l_amount
                              || '-'
                              || l_total_amt
                              || '-'
                              || l_period_tab (l_prd_indx).start_date
                              || '-'
                              || l_period_tab (l_prd_indx).end_date
                              || '-'
                              || l_max_recog_amt
                              || '-'
                              || l_actual_recog_amt
                              || '-'
                              || l_rc_schd_rec.rule_end_date
                              ||'-'
                              ||l_rc_schd_rec.open_prd_id);


         IF NOT ( (l_actual_recog_amt > 0)
         AND (l_period_amt = 0))
         THEN
            IF (l_period_amt = 0)
            THEN
              l_period_amt      := 1 / POWER (10, l_rc_schd_rec.rounding);
            END IF;



            write_log ('Log Catching Up the Revenue  OPEN_PERIOD_START_DATE - l_amount-l_total_amt - period_start_date -period_end_date-rule_end_date '
                               || l_rc_schd_rec.open_cur_start_dt
                               || '-'
                               || l_amount
                               || '-'
                               || l_total_amt
                               || '-'
                               || l_period_tab (l_prd_indx).start_date
                               || '-'
                               || l_period_tab (l_prd_indx).end_date
                               || '-'
                               || l_rc_schd_rec.rule_end_date);




            IF (l_period_tab (l_prd_indx).start_date < l_rc_schd_rec.open_cur_start_dt)
            AND NOT (l_period_tab (l_prd_indx).end_date >= l_rc_schd_rec.rule_end_date)
            THEN
               l_accum_rev_amt      := l_accum_rev_amt + l_period_amt;

               IF (l_total_amt + l_accum_rev_amt > l_amount)
               THEN
                  l_accum_rev_amt      := l_amount - l_total_amt;
               END IF;
               IF l_period_tab (l_prd_indx).ID = l_prv_yr_prd
               THEN
                  l_py_amt      := (l_accum_rev_amt) * SIGN (p_rc_schd_rec.rel_amt);

                  rpro_utility_pkg.record_log_act (
                                                    p_type        => l_process_name
                                                   ,p_text        =>    'Log Catching Up the Revenue to Prv year OPEN_PERIOD_START_DATE - l_amount-l_total_amt - period_start_date -period_end_date-rule_end_date~l_pq_amt '
                                                                     || l_rc_schd_rec.open_cur_start_dt
                                                                     || '-'
                                                                     || l_amount
                                                                     || '-'
                                                                     || l_total_amt
                                                                     || '-'
                                                                     || l_period_tab (l_prd_indx).start_date
                                                                     || '-'
                                                                     || l_period_tab (l_prd_indx).end_date
                                                                     || '-'
                                                                     || l_rc_schd_rec.rule_end_date
                                                                     ||'~'
                                                                     || l_pq_amt
                                                   ,p_log_level   => 8
                                                   ,p_rc_id       => p_rc_schd_rec.rc_id
                                                   ,p_line_id     => p_rc_schd_rec.line_id
                                                   ,p_pob_id      => p_rc_schd_rec.rc_pob_id
                                                  );
               ELSIF l_period_tab (l_prd_indx).ID = l_prv_qtr_end_prd
               THEN
                  l_pq_amt      := (l_accum_rev_amt -l_py_amt) * SIGN (p_rc_schd_rec.rel_amt);

                  rpro_utility_pkg.record_log_act (
                                                    p_type        => l_process_name
                                                   ,p_text        =>    'Log Catching Up the Revenue to Prv qtr OPEN_PERIOD_START_DATE - l_amount-l_total_amt - period_start_date -period_end_date-rule_end_date~l_pq_amt '
                                                                     || l_rc_schd_rec.open_cur_start_dt
                                                                     || '-'
                                                                     || l_amount
                                                                     || '-'
                                                                     || l_total_amt
                                                                     || '-'
                                                                     || l_period_tab (l_prd_indx).start_date
                                                                     || '-'
                                                                     || l_period_tab (l_prd_indx).end_date
                                                                     || '-'
                                                                     || l_rc_schd_rec.rule_end_date
                                                                     ||'~'
                                                                     || l_pq_amt
                                                   ,p_log_level   => 8
                                                   ,p_rc_id       => p_rc_schd_rec.rc_id
                                                   ,p_line_id     => p_rc_schd_rec.line_id
                                                   ,p_pob_id      => p_rc_schd_rec.rc_pob_id
                                                  );
               ELSIF l_period_tab (l_prd_indx).ID = l_prv_prd
               THEN
                  l_pp_amt      := (l_accum_rev_amt-(l_py_amt+l_pq_amt))  * SIGN (p_rc_schd_rec.rel_amt);

                  rpro_utility_pkg.record_log_act (
                                                    p_type        => l_process_name
                                                   ,p_text        =>    'Log Catching Up the Revenue to Prv Prd OPEN_PERIOD_START_DATE - l_amount-l_total_amt - period_start_date -period_end_date-rule_end_date~l_pp_amt '
                                                                     || l_rc_schd_rec.open_cur_start_dt
                                                                     || '-'
                                                                     || l_amount
                                                                     || '-'
                                                                     || l_total_amt
                                                                     || '-'
                                                                     || l_period_tab (l_prd_indx).start_date
                                                                     || '-'
                                                                     || l_period_tab (l_prd_indx).end_date
                                                                     || '-'
                                                                     || l_rc_schd_rec.rule_end_date
                                                                     || '~'
                                                                     || l_pp_amt
                                                   ,p_log_level   => 8
                                                   ,p_rc_id       => p_rc_schd_rec.rc_id
                                                   ,p_line_id     => p_rc_schd_rec.line_id
                                                   ,p_pob_id      => p_rc_schd_rec.rc_pob_id
                                                  );



                  IF rpro_utility_pkg.g_log_level >= 7
                  THEN
                     rpro_utility_pkg.record_log_act (
                       p_type        => l_process_name
                      ,p_text        =>    'Log Catching Up the Revenue  OPEN_PERIOD_START_DATE - l_amount-l_total_amt - period_start_date -period_end_date-rule_end_date '
                                        || l_rc_schd_rec.open_cur_start_dt
                                        || '-'
                                        || l_amount
                                        || '-'
                                        || l_total_amt
                                        || '-'
                                        || l_period_tab (l_prd_indx).start_date
                                        || '-'
                                        || l_period_tab (l_prd_indx).end_date
                                        || '-'
                                        || l_rc_schd_rec.rule_end_date
                      ,p_log_level   => 8
                      ,p_rc_id       => p_rc_schd_rec.rc_id
                      ,p_line_id     => p_rc_schd_rec.line_id
                      ,p_pob_id      => p_rc_schd_rec.rc_pob_id
                     );
                  END IF;
               ELSIF  l_period_tab (l_prd_indx).ID = l_period_tab.LAST
               THEN
                  l_rc_schd_rec.rel_amt      := (l_accum_rev_amt) * SIGN (p_rc_schd_rec.rel_amt);


                  l_rc_schd_rec.prd_id      := l_rc_schd_rec.open_prd_id;
                  l_rc_schd_rec.pq_amt      := l_pq_amt;
                  l_rc_schd_rec.pp_amt      := l_pp_amt;
                  l_rc_schd_rec.py_amt      := l_py_amt;
                    rpro_utility_pkg.record_log_act (
                                      p_type        => l_process_name
                                     ,p_text        =>    'Log Came to catchup last period ~'
                                                        ||'~l_accum_rev_amt~'||l_accum_rev_amt
                                                        ||'~rel_amt~'||l_rc_schd_rec.rel_amt
                                     ,p_log_level   => 8
                                     ,p_rc_id       => p_rc_schd_rec.rc_id
                                     ,p_line_id     => p_rc_schd_rec.line_id
                                     ,p_pob_id      => p_rc_schd_rec.rc_pob_id
                                    );


                        write_log('Inserting schedules 1');
                        insert_rc_schedules (l_rc_schd_rec
                                            ,p_rc_schd
                                            ,p_rc_line_schd
                                            ,p_ca_amt
                                            ,p_cl_amt
                                            ,p_rc_head_rec
                                            );

                        l_total_amt      := l_accum_rev_amt ;
                        l_accum_rev_amt            := 0;
                        l_pp_amt         := 0 ;
                        l_pq_amt         := 0;
                        l_py_amt         := 0;



               END IF;



               IF (l_total_amt >= l_amount)
               THEN
                  EXIT;
               END IF;
            ELSE
               l_accum_rev_amt            := l_accum_rev_amt + l_period_amt;

               IF (l_total_amt + l_accum_rev_amt > l_amount)
               THEN
                 l_accum_rev_amt      := l_amount - l_total_amt;
               END IF;
               IF l_rc_schd_rec.rule_end_date BETWEEN l_period_tab (l_prd_indx).start_date AND l_period_tab (l_prd_indx).end_date
               THEN
                 l_accum_rev_amt      := l_amount - l_total_amt;
               END IF;

               l_rc_schd_rec.rel_amt      := (l_accum_rev_amt) * SIGN (p_rc_schd_rec.rel_amt);

               IF (l_period_tab (l_prd_indx).start_date < l_rc_schd_rec.open_cur_start_dt)
               THEN
                 l_rc_schd_rec.prd_id      := l_rc_schd_rec.open_prd_id;
               ELSE
                 l_rc_schd_rec.prd_id      := l_period_tab (l_prd_indx).ID;
               END IF;

               l_total_amt                := l_total_amt + l_accum_rev_amt;

               write_log('Log Revenue OPEN_PERIOD_START_DATE - l_accum_rev_amt-l_period_amt - period_start_date -period_end_date-p_rule_end_date-l_rc_schd_rec.rel_amt '
                                    || l_rc_schd_rec.open_cur_start_dt
                                    || '-'
                                    || l_accum_rev_amt
                                    || '-'
                                    || l_period_amt
                                    ||'~Rel_amt~'||l_rc_schd_rec.rel_amt);

               IF p_rc_schd_tab.EXISTS (p_rc_schd_rec.rev_act_type || '-' || l_period_tab (l_prd_indx).ID || '~' || p_rc_schd_rec.schd_type)
               THEN

                  rpro_utility_pkg.record_log_act (
                                                    p_type        => l_process_name
                                                   ,p_text        =>    'Exists p_rc_schd_rec.rev_act_type~ '||p_rc_schd_rec.rev_act_type
                                                                     || '~l_period_tab (l_prd_indx).ID~'||l_period_tab (l_prd_indx).ID
                                                   ,p_log_level   => 8
                                                   ,p_rc_id       => p_rc_schd_rec.rc_id
                                                   ,p_line_id     => p_rc_schd_rec.line_id
                                                   ,p_pob_id      => p_rc_schd_rec.rc_pob_id
                                                  );
                  p_rc_schd_tab (p_rc_schd_rec.rev_act_type || '-' || l_period_tab (l_prd_indx).ID || '~' || p_rc_schd_rec.schd_type).amount      :=   NVL (
                                                                                                                       p_rc_schd_tab (p_rc_schd_rec.rev_act_type || '-' || l_period_tab (l_prd_indx).ID || '~' || p_rc_schd_rec.schd_type).amount
                                                                                                                      ,0
                                                                                                                     )
                                                                                                                   + ABS (l_rc_schd_rec.rel_amt);
               ELSE
                 p_rc_schd_tab (p_rc_schd_rec.rev_act_type || '-' || l_period_tab (l_prd_indx).ID || '~' || p_rc_schd_rec.schd_type).amount      := ABS (l_rc_schd_rec.rel_amt);
               END IF;
               l_rc_schd_rec.pq_amt      := l_pq_amt;
               l_rc_schd_rec.pp_amt      := l_pp_amt;
               l_rc_schd_rec.py_amt      := l_py_amt;

               write_log('Inserting schedules 2');
               insert_rc_schedules ( l_rc_schd_rec
                                   , p_rc_schd
                                   , p_rc_line_schd
                                   , p_ca_amt
                                   , p_cl_amt
                                   , p_rc_head_rec
                                   );

               l_accum_rev_amt            := 0;
               l_pp_amt                   := 0;
               l_pq_amt                   := 0;
               l_py_amt                   := 0;
            END IF;

            IF (l_total_amt >= l_amount)
            THEN
              EXIT;
            END IF;
         END IF;

         l_prd_indx              := l_period_tab.NEXT (l_prd_indx);
      END LOOP;
   END contract_recognize_revenue;
  -----------------------------------------------------------------------------
   PROCEDURE contract_prospective_revenue( p_rc_schd_rec        IN OUT NOCOPY all_tab_pkg.rc_schd_rec                                 --20190809
                                         , p_rc_schd_tab        IN OUT NOCOPY rpro_revenue_process_pkg.rc_contract_schd_tab
                                         , p_rc_schd            IN OUT NOCOPY all_tab_pkg.rc_schd_data_tab
                                         , p_open_prd_id        IN NUMBER
                                         )
   IS
      l_rc_schd_rec   all_tab_pkg.rc_schd_rec;
      l_tot_days      NUMBER;
      l_consumed_days NUMBER;
      l_consump_ratio NUMBER;
      l_actual_amt    NUMBER;
      l_cur_prd_amt   NUMBER;
      l_remain_amt    NUMBER;
      p_rc_line_schd  all_tab_pkg.rc_line_schd_tab;
      l_rc_schd       all_tab_pkg.rc_schd_data_tab;
      l_rc_head_rec   rpro_rc_head%rowtype;
      l_rc_schd_tab   rpro_revenue_process_pkg.rc_contract_schd_tab ;
      l_ca_amt        NUMBER;
      l_cl_amt        NUMBER;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      l_rc_schd_rec := p_rc_schd_rec;
      write_log('Log: Begin contract_prospective_revenue ');

      write_log('Log: l_rc_schd_rec.rule_start_date: '||l_rc_schd_rec.rule_start_date
                ||CHR(10)
                ||'l_rc_schd_rec.rule_end_date: '||l_rc_schd_rec.rule_end_date
                ||CHR(10)
                ||'l_rc_schd_rec.open_cur_start_dt: '||l_rc_schd_rec.open_cur_start_dt
                ||CHR(10)
                ||'l_rc_schd_rec.atr4: '||l_rc_schd_rec.atr4
                ||CHR(10)
                ||'l_rc_schd_rec.atr5: '||l_rc_schd_rec.atr5);

      l_tot_days      := l_rc_schd_rec.rule_end_date - l_rc_schd_rec.rule_start_date + 1             ;
      l_consumed_days := ((( CASE WHEN l_rc_schd_rec.rule_end_date <= l_rc_schd_rec.open_cur_start_dt   --20190917  No end date is lesser than the current prd st dt
                                          THEN NVL(l_rc_schd_rec.rule_end_date,l_rc_schd_rec.rule_start_date)
                                          ELSE LAST_DAY(l_rc_schd_rec.open_cur_start_dt)
                                     END)
                           ) - l_rc_schd_rec.rule_start_date) + 1 ;  --20190810
      l_consump_ratio := l_consumed_days/l_tot_days                                                  ;
      l_actual_amt    := l_consump_ratio * l_rc_schd_rec.atr5                                        ; --atr5 Original cum_cv_amt
      l_cur_prd_amt   := ROUND(l_actual_amt - l_rc_schd_rec.atr4,2)                                  ; --atr4 previous period schedule amt
      l_remain_amt    := ROUND(l_rc_schd_rec.atr5 - l_rc_schd_rec.atr4 - l_cur_prd_amt,2)            ;

      write_log('Log: l_tot_days: ' ||l_tot_days
               ||CHR(10)
               ||' l_consumed_days '||l_consumed_days
               ||CHR(10)
               ||' l_consump_ratio '||l_consump_ratio
               ||CHR(10)
               ||' l_actual_amt '   ||l_actual_amt
               ||CHR(10)
               ||' l_cur_prd_amt '  ||l_cur_prd_amt
               ||CHR(10)
               ||' l_remain_amt '   ||l_remain_amt);

      l_rc_schd_rec.ext_sll_prc       :=  l_cur_prd_amt  ;
      l_rc_schd_rec.contract_amt      :=  l_cur_prd_amt  ;
      l_rc_schd_rec.contract_rec_amt  :=  l_cur_prd_amt  ;
      l_rc_schd_rec.rel_amt           :=  l_cur_prd_amt  ;
      l_rc_schd_rec.prd_id            :=  p_open_prd_id  ;
      l_rc_schd_rec.rounding          :=  2              ;


      insert_rc_schedules ( l_rc_schd_rec
                          , p_rc_schd
                          , p_rc_line_schd
                          , l_ca_amt
                          , l_cl_amt
                          , l_rc_head_rec
                          );

      IF l_remain_amt <> 0     --20190917             Changed from greater than zero to not equal to zero
      THEN
         l_rc_schd_rec.ext_sll_prc       :=  l_remain_amt  ;
         l_rc_schd_rec.contract_amt      :=  l_remain_amt  ;
         l_rc_schd_rec.contract_rec_amt  :=  l_remain_amt  ;
         l_rc_schd_rec.rel_amt           :=  l_remain_amt  ;
         l_rc_schd_rec.rule_start_date   :=  ADD_MONTHS(l_rc_schd_rec.open_cur_start_dt,1) ;
         l_rc_schd_rec.open_cur_start_dt :=  ADD_MONTHS(l_rc_schd_rec.open_cur_start_dt,1) ;

         contract_recognize_revenue( p_rc_schd_rec  =>   l_rc_schd_rec
                                   , p_rc_schd_tab  =>   l_rc_schd_tab
                                   , p_rc_schd      =>   p_rc_schd
                                   );    --20190819
      END IF;
      write_log('Log: Begin contract_prospective_revenue ');
   END contract_prospective_revenue;
  -----------------------------------------------------------------------------
  PROCEDURE no_rev_schd_generation( p_errbuf   OUT  VARCHAR2
                                  , p_retcode  OUT  NUMBER
                                  , p_batch_id IN   NUMBER DEFAULT NULL)
  IS
     CURSOR c_batch_data(p_book_id      IN  NUMBER
                        ,p_sec_atr_val  IN  VARCHAR2)
     IS
     SELECT rrl.id
           ,rrl.rc_id
           ,rrl.order_id
           ,rrl.start_date
           ,rrl.end_date
           ,rrl.ext_sll_prc
           ,rrl.doc_date
           ,rrl.book_id
           ,rrl.sec_atr_val
           ,rrl.curr
           ,rrl.f_ex_rate
           ,rrl.g_ex_rate
           ,rrl.num11
           ,CASE WHEN NVL(rrl.atr13, 'N/A') IN( 'N/A','NA')
                 THEN
                    rrl.doc_date
                 ELSE
                    CASE WHEN NVL(rrl.atr13, 'N/A') NOT IN('NA', 'N/A')
                         AND  NVL(rrl.atr14, 'N/A') IN  ( 'N/A','NA')
                         THEN GREATEST(rrl.doc_date, rrl.date3 - rrl.atr13)    --20190904 Changed start date to date3
                         WHEN NVL(rrl.atr13, 'N/A') NOT IN ( 'N/A','NA')
                         AND  NVL(rrl.atr14, 'N/A') NOT IN ( 'N/A','NA')
                         THEN rrl.date3 - rrl.atr14
                    END
            END                                                 no_start_date
            ,
            CASE WHEN ( NVL(rrl.atr14, 'N/A') NOT IN ('N/A','NA'))
                 THEN
                   rrl.end_date - rrl.atr14
                 WHEN ( NVL(rrl.atr14, 'N/A') IN( 'N/A','NA'))
                 THEN
                    CASE WHEN NVL(rrl.atr13, 'N/A') IN( 'N/A','NA')
                         THEN
                            rrl.doc_date
                         ELSE
                         CASE WHEN NVL(rrl.atr13, 'N/A') NOT IN( 'N/A','NA')
                              AND  NVL(rrl.atr14, 'N/A') IN ('N/A','NA')
                              THEN GREATEST(rrl.doc_date, rrl.date3 - rrl.atr13)
                              WHEN NVL(rrl.atr13, 'N/A') NOT IN ( 'N/A','NA')          --20190812
                              AND  NVL(rrl.atr14, 'N/A') NOT IN ( 'N/A','NA')
                              THEN rrl.date3 - rrl.atr14                               --20190913
                         END
                    END
            END                                                 no_end_date
            ,
            NVL2( rrl.cum_alctd_amt,
                  rrl.cum_alctd_amt,
                  NVL2(rrl.alctd_xt_prc,rrl.alctd_xt_prc,rrl.net_sll_prc) ) no_cum_alctd_amt
            ,rrl.num10                                                          exstng_cum_alctd_amt
            ,rrh.init_pob_exp_dt
     FROM   rpro_rc_line_g rrl
           ,rpro_rc_head_g rrh
     WHERE  rrl.rc_id       =  rrh.id
     AND    rrl.date3       IS NOT NULL  --20190906
     AND    rrh.book_id     =  p_book_id
     AND    rrh.sec_atr_val =  p_sec_atr_val
     AND    NVL(rrl.num10,-999) <> NVL2( rrl.cum_alctd_amt,     --20190927
                                      rrl.cum_alctd_amt,
                                      NVL2( rrl.alctd_xt_prc,rrl.alctd_xt_prc,rrl.net_sll_prc ))         --Not to reprocess the same line again for creating NO schedules 20190731 -- Replacing 0 to -999
     AND   rrl.atr2           IS NOT NULL  --20190802
     AND   rrl.batch_id       = NVL(p_batch_id,rrl.batch_id);

     TYPE tab_batch_data IS TABLE OF c_batch_data%ROWTYPE
                            INDEX BY BINARY_INTEGER;

     TYPE ooh_indx_tab IS TABLE OF BINARY_INTEGER
                       INDEX BY BINARY_INTEGER;
     TYPE tab_line_id IS TABLE OF VARCHAR2(50)
                      INDEX BY BINARY_INTEGER;

     l_ooh_indx_tab            ooh_indx_tab;
     l_line_id_tab             tab_line_id;
     l_ooh_schd_data           all_tab_pkg.rc_schd_data_tab;
     l_ooh_idx                 NUMBER;
     l_cnt                     NUMBER := 0;
     l_act_amt                 NUMBER := 0;
     l_no_amt                  NUMBER := 0;
     l_ooh_amt                 NUMBER := 0;

     l_open_prd_st_date        DATE;
     l_open_prd_ed_date        DATE;
     l_open_prd_id             NUMBER;
     l_rc_ver                  NUMBER;
     l_prev_schd_amt           NUMBER;
     l_ooh_prd_indx            NUMBER;
     t_batch_data              tab_batch_data;
     l_rc_schd_rec             all_tab_pkg.rc_schd_rec;
     l_rc_schd_tab             rpro_revenue_process_pkg.rc_contract_schd_tab ;
     l_rc_schd                 all_tab_pkg.rc_schd_data_tab;
     l_no_st_date              DATE;
     l_no_end_Date             DATE;
     l_prd_tab                 rpro_utility_pkg.rc_calender_tab;
     l_cum_alct_amt            NUMBER := 0;
     l_updt_dt                 DATE;
     l_revision_prd            VARCHAR2(1) := 'N';
     l_suspend_resume_flag     VARCHAR2(250);
     l_cust_schd_flag          VARCHAR2(250);
     l_order_type              rpro_ds_order_action_g.type%TYPE;
     l_line_schd_rec           rpro_rc_schd_g%ROWTYPE;
  BEGIN
     rpro_utility_pkg.set_revpro_context;
     write_log('Log: Begin no_rev_schd_generation : p_batch_id '||p_batch_id);
     --MS 20210203 Starts  LV20210318
     SELECT MAX(requested_start_date) 
     INTO l_updt_dt 
     FROM rpro_schd_prog 
     WHERE prog_id= (SELECT id 
                     FROM rpro_prog_head 
                     WHERE upper(name) = UPPER('Revpro3.0 Custom Summarize'))
     AND substr(indicators,3,1)='C';
     
     SELECT line.id 
     BULK COLLECT INTO l_line_id_tab
     FROM rpro_rc_line_g line
         ,rpro_cust_rc_schd cust 
     WHERE trunc(line.updt_dt) >= trunc(l_updt_dt) 
     AND cust.rc_id != line.rc_id 
     AND cust.line_id = line.id 
     AND line.rc_id !=0;
     write_log('Modified Line Count:'||l_line_id_tab.COUNT);
     IF l_line_id_tab.COUNT > 0
     THEN
        FORALL i IN 1 .. l_line_id_tab.COUNT
        UPDATE rpro_rc_line 
        SET num10 = NULL
        WHERE id = l_line_id_tab(i);
     END IF;
     COMMIT;
     --MS 20210203 ENDS
     
     FOR r_prd IN (SELECT rc.start_date  open_prd_st_date    --20190812
                         ,rc.end_date    open_prd_ed_date
                         ,rc.id          open_prd_id
                         ,rp.book_id     book_id
                         ,rp.sec_atr_val sec_atr_val
                   FROM   rpro_period_g   rp
                         ,rpro_calendar_g rc
                   WHERE  rp.status = 'OPEN'
                   AND    rp.id = rc.id)
     LOOP

        OPEN c_batch_data(r_prd.book_id,r_prd.sec_atr_val);
        LOOP
           FETCH c_batch_data
           BULK COLLECT
           INTO t_batch_data
           LIMIT gc_limit;
           EXIT WHEN t_batch_data.COUNT = 0;
           write_log('Log: t_batch_data.COUNT :'||t_batch_data.COUNT);
           FOR rec IN t_batch_data.FIRST..t_batch_data.LAST
           LOOP

              /*Changing Open period date and id based on NO lines booking date*/
              write_log(' rc_line_id              : '||t_batch_data(rec).id
                       ||' r_prd.open_prd_st_date : '||r_prd.open_prd_st_date
                       ||'~r_prd.open_prd_id~'          ||r_prd.open_prd_id
                       ||'~doc_date~'               ||t_batch_data(rec).doc_date
                       ||'~start_date~'             ||t_batch_data(rec).start_date
                       ||'~End_date~'               ||t_batch_data(rec).end_date
                       ||'~no_start_date~'          ||t_batch_data(rec).no_start_date
                       ||'~no_end_date~'            ||t_batch_data(rec).no_end_date
                       ||'~r_prd.open_prd_id~'          ||r_prd.open_prd_id
                       ||'~r_prd.open_prd_st_date~'     ||r_prd.open_prd_st_date );
              --20191014
              BEGIN
                 SELECT 'Y'
                 INTO   l_suspend_resume_flag
                 FROM   rpro_ds_order_action_g
                 WHERE  order_id = t_batch_data(rec).order_id
                 AND    type     IN ('Suspend','Resume')
                 AND    rownum   = 1;

              EXCEPTION
              WHEN OTHERS THEN
                 l_suspend_resume_flag := 'N';
              END;
              --20191014
              BEGIN
                 SELECT 'Y'
                 INTO   l_cust_schd_flag
                 FROM   rpro_cust_rc_schd
                 WHERE  line_id  =  t_batch_data(rec).id
                 AND    rownum   =  1;
              EXCEPTION
              WHEN OTHERS THEN
                 l_cust_schd_flag := 'N';
              END;
              write_log(' rc_id                   : '||t_batch_data(rec).rc_id
                        || CHR(10)
                        ||'  line_id              : '||t_batch_data(rec).id
                        || CHR(10)
                        ||' l_cust_schd_flag      : '||l_cust_schd_flag
                        || CHR(10)
                        ||' l_suspend_resume_flag : '||l_suspend_resume_flag);
              /*START Suspend and resume logic   20191011*/
              l_line_schd_rec := NULL;

              IF l_suspend_resume_flag = 'Y' AND  l_cust_schd_flag = 'Y'  --20191014
              THEN
                 write_log('Log: Continuing to next line without creating schedules, Since it is a suspend/resume line and its Original NO line created initially.');
                 CONTINUE;
              ELSIF  l_suspend_resume_flag = 'Y' AND  l_cust_schd_flag = 'N'   --20191014
              THEN
                 write_log('Adding $0 line to Custom schedule table. Since order action is Suspend/Resume. And line id not present in custom schedule table .');
                 l_line_schd_rec.id           := rpro_cust_rc_schd_id_s.nextval             ;
                 l_line_schd_rec.rc_id        := t_batch_data(rec).rc_id                    ;
                 l_line_schd_rec.rc_ver       := 1                                          ;
                 l_line_schd_rec.line_id      := t_batch_data(rec).id                       ;
                 l_line_schd_rec.curr         := t_batch_data(rec).curr                     ;
                 l_line_schd_rec.amount       := 0                                          ;
                 l_line_schd_rec.rel_pct      := 0                                          ;
                 l_line_schd_rec.indicators   := 'NLRNNNNNNRNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN' ;
                 l_line_schd_rec.prd_id       := r_prd.open_prd_id                          ;
                 l_line_schd_rec.post_prd_id  := r_prd.open_prd_id                          ;
                 l_line_schd_rec.f_ex_rate    := t_batch_data(rec).f_ex_rate                ;
                 l_line_schd_rec.g_ex_rate    := t_batch_data(rec).g_ex_rate                ;
                 l_line_schd_rec.client_id    := rpro_utility_pkg.g_client_id               ;
                 l_line_schd_rec.crtd_prd_id  := r_prd.open_prd_id                          ;
                 l_line_schd_rec.sec_atr_val  := r_prd.sec_atr_val                          ;
                 l_line_schd_rec.crtd_by      := rpro_utility_pkg.g_user                    ;
                 l_line_schd_rec.crtd_dt      := SYSDATE                                    ;
                 l_line_schd_rec.updt_by      := rpro_utility_pkg.g_user                    ;
                 l_line_schd_rec.updt_dt      := SYSDATE                                    ;
                 l_line_schd_rec.root_line_id := t_batch_data(rec).id                       ;
                 l_line_schd_rec.book_id      := r_prd.book_id                              ;
                 l_line_schd_rec.atr3         := 'NO'                                       ; --20191015 resume fix

                 INSERT INTO rpro_cust_rc_schd
                 VALUES l_line_schd_rec;

                 COMMIT;
                 write_log('Log: Continuing to next line with 0$ schedules since it is a Resume line ');
                 CONTINUE;
              END IF;
              /*END Suspend and resume logic*/

              BEGIN
                 SELECT rc_ver
                 INTO   l_rc_ver
                 FROM   rpro_cust_rc_schd
                 WHERE  line_id     = t_batch_data(rec).id
                 AND    sec_atr_val = r_prd.sec_atr_val     --20190813  Org based changes
                 AND    id          = (SELECT MAX(id)
                                       FROM   rpro_cust_rc_schd
                                       WHERE  line_id = t_batch_data(rec).id);
                 write_log('Log: l_rc_ver : '||l_rc_ver);
              EXCEPTION
              WHEN OTHERS THEN
                 write_error('Error: No Existing schedules available');
              END;


              SELECT NVL(SUM(amount),0)
              INTO   l_prev_schd_amt
              FROM   rpro_cust_rc_schd
              WHERE  line_id     = t_batch_data(rec).id
              AND    sec_atr_val = r_prd.sec_atr_val     --20190813  Org based changes
              AND    prd_id      < r_prd.open_prd_id;

              /*Deleting the future existing schedules*/
              DELETE FROM rpro_cust_rc_schd
              WHERE  line_id     = t_batch_data(rec).id
              AND    sec_atr_val = r_prd.sec_atr_val     --20190813  Org based changes
              AND    prd_id     >= r_prd.open_prd_id;

              write_log('Log: Future Schedules deleted count : '||SQL%ROWCOUNT);

              IF t_batch_data(rec).init_pob_exp_dt < r_prd.open_prd_ed_date   --Checking revision period. 20190809
              THEN
                 write_log('Log: Revision line');
                 l_rc_schd_rec.atr4              := l_prev_schd_amt                           ;  --capturing prior period total amount 20190809
                 l_revision_prd                  := 'Y'                                       ;
                 l_rc_schd_rec.atr5              := NVL(t_batch_data(rec).no_cum_alctd_amt,0) ;  --capturing original cum_cv_amt 20190809  --20190813 Handled NVL
              END IF;

              l_cum_alct_amt   := t_batch_data(rec).no_cum_alctd_amt - l_prev_schd_amt;    --20190917  Removed ABS

              write_log('Log: no_cum_alctd_amt: '||t_batch_data(rec).no_cum_alctd_amt
                       ||CHR(10)
                       ||'l_prev_schd_amt: '||l_prev_schd_amt
                       ||CHR(10)
                       ||'l_cum_alct_amt: '||l_cum_alct_amt);


              l_rc_schd_rec.ext_sll_prc       :=  l_cum_alct_amt  ;
              l_rc_schd_rec.contract_amt      :=  l_cum_alct_amt  ;
              l_rc_schd_rec.contract_rec_amt  :=  l_cum_alct_amt  ;
              l_rc_schd_rec.rel_amt           :=  l_cum_alct_amt  ;
              l_rc_schd_rec.rule_start_date   :=  t_batch_data(rec).no_start_date;
              l_rc_schd_rec.rule_end_date     :=  t_batch_data(rec).no_end_date  ;
              l_rc_schd_rec.rev_rec_type      :=  'DR_APR'                       ;
              l_rc_schd_rec.rounding          :=  2                              ;
              l_rc_schd_rec.curr              :=  t_batch_data(rec).curr         ;
              l_rc_schd_rec.book_id           :=  t_batch_data(rec).book_id      ;
              l_rc_schd_rec.sec_atr_val       :=  t_batch_data(rec).sec_atr_val  ;
              l_rc_schd_rec.line_id           :=  t_batch_data(rec).id           ;
              l_rc_schd_rec.root_line_id      :=  t_batch_data(rec).id           ;
              l_rc_schd_rec.rc_id             :=  t_batch_data(rec).rc_id        ;
              l_rc_schd_rec.schd_type         :=  'R'                            ;
              l_rc_schd_rec.rel_pct           :=  100                            ;
              l_rc_schd_rec.rc_ver            :=  NVL(l_rc_ver,0)+1              ;
              l_rc_schd_rec.open_cur_start_dt :=  r_prd.open_prd_st_date         ;
              l_rc_schd_rec.f_ex_rate         :=  t_batch_data(rec).f_ex_rate    ;
              l_rc_schd_rec.g_ex_rate         :=  t_batch_data(rec).g_ex_rate    ;
              l_rc_schd_rec.open_prd_id       :=  r_prd.open_prd_id              ;  --20190819


              IF l_revision_prd = 'Y'
              THEN
                 --call prospective API  20190809
                 contract_prospective_revenue( p_rc_schd_rec        => l_rc_schd_rec
                                             , p_rc_schd_tab        => l_rc_schd_tab
                                             , p_rc_schd            => l_rc_schd
                                             , p_open_prd_id        => r_prd.open_prd_id);
              ELSE
                 /*Creating NO line schedules Intial revenue*/
                 contract_recognize_revenue( p_rc_schd_rec  =>   l_rc_schd_rec
                                           , p_rc_schd_tab  =>   l_rc_schd_tab
                                           , p_rc_schd      =>   l_rc_schd
                                           );
              END IF;

              l_cum_alct_amt     := 0;
              l_revision_prd     := 'N';
           END LOOP;
           --20190731 stop reprocess the same record
           FORALL i IN t_batch_data.FIRST..t_batch_data.LAST
           UPDATE rpro_rc_line_g
           SET    num10 = t_batch_data(i).no_cum_alctd_amt
           WHERE  id    = t_batch_data(i).id;
           --AND    rc_id = t_batch_data(i).rc_id;  --LV20210318

           write_log('Log : Total schedules count l_rc_schd.COUNT~'||l_rc_schd.COUNT);
           sync_rc_schd_data ( p_rc_schd_data => l_rc_schd);
        END LOOP;
        CLOSE c_batch_data  ;
     END LOOP;
     write_log('Log: End no_rev_schd_generation : p_batch_id '||p_batch_id);
  EXCEPTION
  WHEN OTHERS THEN                                                             --20190812
     write_error(' Error in no_rev_schd_generation : '||SQLERRM
                 ||CHR(10)
                 ||'Error Queue : '||dbms_utility.format_error_backtrace);
     p_errbuf    := ' Error in no_rev_schd_generation : '||sqlerrm;
     p_retcode   := 2;
  END no_rev_schd_generation;
  -----------------------------------------------------------------------------
  PROCEDURE create_ooh_schedules( p_ooh_data       IN  tab_schd_rec
                                , p_ooh_schd_data  IN OUT   all_tab_pkg.RC_SCHD_DATA_TAB
                                , p_ooh_indx_tab   IN OUT   ooh_indx_tab
                                , p_open_period_id IN NUMBER)
  IS
     l_prd_tab          rpro_utility_pkg.rc_calender_tab;
     l_ooh_prd_indx     NUMBER;
     l_ooh_idx          NUMBER;
     l_cnt              NUMBER     ;
     l_act_amt          NUMBER := 0;
     l_no_amt           NUMBER := 0;
     l_ooh_amt          NUMBER := 0;
     l_prior_ctch_amt   NUMBER := 0;
     l_open_prd_id      NUMBER ;
  BEGIN
     l_open_prd_id := p_open_period_id;
     rpro_utility_pkg.get_periods ( LEAST(p_ooh_data.start_date,p_ooh_data.no_start_date)
                                  , GREATEST(p_ooh_data.end_date,p_ooh_data.no_end_date)
                                  , l_prd_tab
                                  );

     write_log('Log: l_prd_tab.COUNT: '||l_prd_tab.COUNT
              ||'~Min start date~'     ||LEAST(p_ooh_data.start_date,p_ooh_data.no_start_date)
              ||'~Max end date~'       ||GREATEST(p_ooh_data.end_date,p_ooh_data.no_end_date));

     l_ooh_prd_indx := l_prd_tab.FIRST;

     WHILE (l_ooh_prd_indx IS NOT NULL)
     LOOP
        write_log('Log: l_ooh_prd_indx : '||l_prd_tab (l_ooh_prd_indx).id);
        SELECT SUM(amount)
        INTO   l_no_amt
        FROM   rpro_cust_rc_schd
        WHERE  1=1 
    --  AND    rc_id   = p_ooh_data.rc_id
        AND    line_id = p_ooh_data.line_id
        AND    prd_id  = l_prd_tab (l_ooh_prd_indx).id
        AND    sec_atr_val = p_ooh_data.sec_atr_val;      --MS20201207

        write_log('Log: l_no_amt : '||l_no_amt);

        SELECT SUM(amount)
        INTO   l_act_amt
        FROM   rpro_rc_schd_g
        WHERE  1=1
      --AND    rc_id   = p_ooh_data.rc_id
        AND    line_id = p_ooh_data.line_id
        AND    ( (     rpro_rc_schd_pkg.get_schd_type_flag(indicators)         =  'R'
                  AND  rpro_rc_schd_pkg.get_initial_entry_flag(indicators)     <> 'Y')
                  OR ( rpro_rc_schd_pkg.get_schd_type_flag(indicators)         =  'A'
                  AND  rpro_rc_schd_pkg.get_initial_rep_entry_flag(indicators) <> 'Y' ))
        AND    prd_id  = l_prd_tab (l_ooh_prd_indx).id
        AND    sec_atr_val = p_ooh_data.sec_atr_val; --MS20201207
        write_log('Log: l_act_amt : '||l_act_amt);

        l_ooh_amt := NVL(l_no_amt,0) - NVL(l_act_amt,0);
        write_log('Log: l_ooh_amt : '||l_ooh_amt);

        /*Handling current month + prior month total for OOH lines*/
        IF l_prd_tab (l_ooh_prd_indx).id < l_open_prd_id
        THEN
           l_prior_ctch_amt := l_prior_ctch_amt+l_ooh_amt;              --20190828  Capture the prior amount to current period
        END IF;

        IF l_prd_tab (l_ooh_prd_indx).id = l_open_prd_id
        THEN
           l_ooh_amt := l_prior_ctch_amt + l_ooh_amt;                   --20190828  Current month plus prior month amount
        END IF;
        write_log('Log: l_prior_ctch_amt~'          ||l_prior_ctch_amt
                 ||'~l_ooh_amt~'                    ||l_ooh_amt
                 ||'~l_prd_tab (l_ooh_prd_indx).id~'||l_prd_tab (l_ooh_prd_indx).id
                 ||'~l_open_prd_id~'                ||l_open_prd_id);


        l_ooh_idx                               := rpro_cust_rc_schd_id_s.NEXTVAL            ;   --20190813   removed the zero dollar restriction
        p_ooh_schd_data(l_ooh_idx).id           := l_ooh_idx                                 ;
        p_ooh_schd_data(l_ooh_idx).line_id      := p_ooh_data.line_id                        ;
        p_ooh_schd_data(l_ooh_idx).indicators   := 'NLRNNNNNNRNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN';
        p_ooh_schd_data(l_ooh_idx).rc_ver       := 1                                         ;
        p_ooh_schd_data(l_ooh_idx).root_line_id := p_ooh_data.line_id                        ;
        p_ooh_schd_data(l_ooh_idx).rc_id        := p_ooh_data.rc_id                          ;
        p_ooh_schd_data(l_ooh_idx).amount       := l_ooh_amt                                 ;
        p_ooh_schd_data(l_ooh_idx).prd_id       := l_prd_tab (l_ooh_prd_indx).ID             ;
        p_ooh_schd_data(l_ooh_idx).post_prd_id  := l_prd_tab (l_ooh_prd_indx).ID             ;
        p_ooh_schd_data(l_ooh_idx).atr3         := 'OOH'                                     ;
        p_ooh_schd_data(l_ooh_idx).client_id    := rpro_utility_pkg.g_client_id              ;
        p_ooh_schd_data(l_ooh_idx).crtd_prd_id  := rpro_utility_pkg.get_crtd_prd_id (rpro_utility_pkg.g_book_id, rpro_utility_pkg.g_sec_atr_val);
        p_ooh_schd_data(l_ooh_idx).sec_atr_val  := p_ooh_data.sec_atr_val                    ;  --MS20201207
        p_ooh_schd_data(l_ooh_idx).crtd_by      := rpro_utility_pkg.g_user                   ;
        p_ooh_schd_data(l_ooh_idx).crtd_dt      := SYSDATE                                   ;
        p_ooh_schd_data(l_ooh_idx).updt_by      := rpro_utility_pkg.g_user                   ;
        p_ooh_schd_data(l_ooh_idx).updt_dt      := SYSDATE                                   ;
        p_ooh_schd_data(l_ooh_idx).book_id      := rpro_utility_pkg.g_book_id                ;
        p_ooh_schd_data(l_ooh_idx).f_ex_rate    := p_ooh_data.f_ex_rate                      ;
        p_ooh_schd_data(l_ooh_idx).g_ex_rate    := p_ooh_data.g_ex_rate                      ;
        p_ooh_schd_data(l_ooh_idx).curr         := p_ooh_data.curr                           ;
        p_ooh_schd_data(l_ooh_idx).rel_pct      := 0                                         ;

        l_cnt := NVL(l_cnt,0)+1;
        p_ooh_indx_tab(l_ooh_idx)   := l_ooh_idx;

        write_log('Log: custom l_ooh_idx :'||l_ooh_idx);

        l_ooh_prd_indx              := l_prd_tab.NEXT (l_ooh_prd_indx);
     END LOOP;
     l_prior_ctch_amt            := 0;
     l_ooh_amt                   := 0;
  END create_ooh_schedules;
  -----------------------------------------------------------------------------
  PROCEDURE create_snapshot_table( p_run_prd_id     IN NUMBER
                                 , p_open_period_id IN NUMBER 
                                 , p_sec_atr_val    IN VARCHAR2) --MS20201207
  IS
     CURSOR c_cur_schd(p_run_prd_id IN NUMBER
                      ,p_sec_atr_value IN VARCHAR2) --MS20201207
     IS
     SELECT id line_id
      ,rc_id
      ,start_date
      ,(SELECT end_date
        FROM   rpro_calendar_g
        WHERE  id = (SELECT MAX(prd_id)
                     FROM   rpro_rc_schd_g
                     WHERE  root_line_id = rrl.id)) end_date --20191015 fix for ooh date issue
      ,ext_sll_prc
      ,doc_date
      ,book_id
      ,sec_atr_val
      ,curr
      ,f_ex_rate
      ,g_ex_rate
      ,CASE WHEN NVL(atr13, 'N/A') IN ( 'N/A','NA')
            THEN
            doc_date
            ELSE
               CASE WHEN NVL(atr13, 'N/A') NOT IN ( 'N/A','NA')  --20190812
                    AND  NVL(atr14, 'N/A') IN ('N/A','NA')
                    THEN greatest(doc_date, date3 - atr13)       --20190904
                    ----WHEN NVL(atr13, 'N/A') <> 'N/A'
                    WHEN  NVL(atr14, 'N/A') NOT IN ('N/A','NA')
                    THEN date3 - atr14                           --20190904
               END
       END no_start_date                                     --Deriving NO start date
       ,
       CASE WHEN ( NVL(atr14, 'N/A') NOT IN ('NA', 'N/A'))
            THEN
              end_date - atr14
            WHEN ( NVL(atr14, 'N/A') IN ('NA', 'N/A'))
            THEN
               CASE WHEN NVL(atr13, 'N/A') IN('NA', 'N/A')
                    THEN
                       doc_date
                    ELSE
                    CASE WHEN NVL(atr13, 'N/A') NOT IN ( 'N/A','NA')
                         AND  NVL(atr14, 'N/A') IN ('NA', 'N/A')
                         THEN greatest(doc_date, date3 - atr13)         --20190913
                         ---WHEN NVL(atr13, 'N/A') <> 'N/A'
                         WHEN  NVL(atr14, 'N/A') NOT IN ('NA', 'N/A' )
                         THEN date3 - atr14                             --20190913
                    END
               END
       END no_end_date                                       --Deriving NO start date
       ,
       NVL2( cum_alctd_amt,
             cum_alctd_amt,
             NVL2(alctd_xt_prc,alctd_xt_prc,net_sll_prc) ) no_cum_alctd_amt  --Deriving Net revenue
     FROM   rpro_rc_line_g rrl
     WHERE  1=1
     AND rrl.rc_id NOT IN (0,1)
     AND atr2           IS NOT NULL   --20190802  
     AND sec_atr_val    = p_sec_atr_value  --MS20201207
     AND date3          IS NOT NULL   --20190906
     AND EXISTS ( SELECT 1                                 --20190813  Check if any NO lines exist, then proceed
                  FROM   rpro_cust_rc_schd rcrs1
                  WHERE  1 = 1
                --AND    rrl.rc_id   = rcrs1.rc_id
                  AND    rrl.id      = rcrs1.line_id
                )
     --AND ext_sll_prc <> 0             --20190802
     --AND batch_id       = 11424                     --needs to be commented
     AND (EXISTS (SELECT 1                           --check whether any revenue schedules exist for current period
                  FROM  rpro_rc_schd_g rrs
                  WHERE rrs.prd_id                                          = p_run_prd_id
                  AND   rpro_rc_schd_pkg.get_schd_type_flag(rrs.indicators) = 'R'
                --AND   rrl.rc_id                                           = rrs.rc_id
                  AND   rrl.id                                              = rrs.line_id )
       OR EXISTS --20190802
                 (SELECT 1                           --check whether any NO revenue schedules exist for current period
                  FROM  rpro_cust_rc_schd rcrs
                  WHERE rcrs.prd_id = p_run_prd_id
                --AND   rrl.rc_id   = rcrs.rc_id
                  AND   rrl.id      = rcrs.line_id
                 )
        );

     TYPE t_rcline IS TABLE OF tab_schd_rec
     INDEX BY PLS_INTEGER;

     l_rcline      t_rcline;
     l_tab_cnt     NUMBER;
     l_sql_stmt    VARCHAR2(32767);
     l_open_prd_id NUMBER; 
     l_sec_atr_val_param VARCHAR2(32767); --MS20201207 
     l_multi_org_val     APEX_APPLICATION_GLOBAL.VC_ARR2; --MS20201207
  BEGIN
     l_sec_atr_val_param := p_sec_atr_val; --MS20201207
     write_log('Log: Begin Creating snapshot table ');

     l_open_prd_id := p_open_period_id;
     write_log('Log: p_run_prd_id : '||p_run_prd_id||'~p_open_period_id~'||p_open_period_id);

     --g_tab_name := 'RPRO_CUST_WF_REP_' || p_run_prd_id||'_'||p_sec_atr_val; --MS20201207

     --Check the snapshot waterfall table already exists
     SELECT COUNT(1)
     INTO   l_tab_cnt
     FROM   dba_tables
     WHERE  table_name = g_tab_name
     AND    owner      = SYS_CONTEXT ( 'userenv', 'current_schema');

     write_log('Log: l_tab_cnt  COUNT : '||l_tab_cnt);

     --Check snapshot exist for prior periods,If yes just show the data
     IF l_tab_cnt = 1 AND p_run_prd_id < p_open_period_id
     THEN
        write_log('Log: Table already exists..No need to recreate the snapshot');
     ELSE
        --Recreate the snapshot for the current period when it is re-run
        IF l_tab_cnt = 1 AND p_run_prd_id = p_open_period_id
        THEN
           EXECUTE IMMEDIATE 'TRUNCATE TABLE '||g_tab_name;
           write_log('Log: Table already exists for the current period...truncating and recreating snapshot');
        --If this is the first run for the current/periods ,Create the snapshot as of now
        ELSIF l_tab_cnt = 0 AND p_run_prd_id <= p_open_period_id
        THEN
           EXECUTE IMMEDIATE 'CREATE TABLE ' || g_tab_name || ' AS SELECT * FROM rpro_rc_schd_g WHERE 1 = 2';
           write_log('Log: Table not exists for the current period...Creating snapshot');
        END IF;

        l_multi_org_val := APEX_UTIL.STRING_TO_TABLE(l_sec_atr_val_param);  --MS20201207 Handled to support ALL parameter
        
        FOR rec IN 1..l_multi_org_val.COUNT 
        LOOP
           write_log('Log: Capturing for ORG : '||l_multi_org_val(rec));
           OPEN c_cur_schd ( p_run_prd_id,l_multi_org_val(rec));  --MS20201207
           LOOP
              FETCH c_cur_schd BULK COLLECT
              INTO  l_rcline
              LIMIT 50000 ;
              write_log('Log: Line exists with current period schedules : l_rcline.COUNT : '|| l_rcline.COUNT);
              EXIT WHEN l_rcline.COUNT = 0;
           
           
              FOR i IN 1 .. l_rcline.COUNT LOOP
           
                 write_log ('l_rcline(i).rc_id : '    || l_rcline(i).rc_id ||
                            '~ l_rcline(i).line_id : '|| l_rcline(i).line_id);
           
           
                 l_sql_stmt := 'INSERT INTO ' || g_tab_name ||
                               q'( SELECT   *
                                 FROM   rpro_rc_schd_g rrs
                                 WHERE  1=1
                                 AND    (   (rpro_rc_schd_pkg.get_schd_type_flag(indicators) = 'R' AND rpro_rc_schd_pkg.get_initial_entry_flag(indicators)    <>'Y')
                                         OR (rpro_rc_schd_pkg.get_schd_type_flag(indicators) = 'A' AND rpro_rc_schd_pkg.get_initial_rep_entry_flag(indicators)<>'Y'))
                               --AND    rrs.rc_id      = :1
                                 AND    rrs.line_id    = :1
                                  AND    rrs.sec_atr_val = :2)';  --MS20201207
           
                 write_log('Log: Inserting Revenue Schedules  l_sql_stmt:'||l_sql_stmt);
           
                 /*Inserting Revenue schedules*/
                 EXECUTE IMMEDIATE l_sql_stmt USING   --l_rcline(i).rc_id,
                                                    l_rcline(i).line_id
                                                  , l_rcline(i).sec_atr_val;   --MS20201207
           
                 l_sql_stmt := 'INSERT INTO ' || g_tab_name ||
                               ' SELECT   *
                                 FROM   rpro_cust_rc_schd rcrs
                                 WHERE  1=1
                               --AND    rcrs.rc_id      = :1
                                 AND    rcrs.line_id    = :1
                                 AND    rcrs.sec_atr_val = :2 '; --MS20201207
           
                 write_log('Log: Inserting NO Revenue Schedules :l_sql_stmt:'||l_sql_stmt);
           
                 /*Inserting NO Revenue schedules*/
                 EXECUTE IMMEDIATE l_sql_stmt USING   --l_rcline(i).rc_id,
                                                      l_rcline(i).line_id
                                                    , l_rcline(i).sec_atr_val;
                 write_log('Log: NO schedules count :'||SQL%ROWCOUNT);
           
                 /*Generating OOH line schedules*/
                 create_ooh_schedules( p_ooh_data       => l_rcline(i)
                                     , p_ooh_schd_data  => RPRO_SIEMENS_REP_CUSTOM_PKG.g_ooh_schd_data
                                     , p_ooh_indx_tab   => RPRO_SIEMENS_REP_CUSTOM_PKG.g_ooh_indx_tab
                                     , p_open_period_id => l_open_prd_id);
                 l_sql_stmt := NULL;
              END LOOP;
           END LOOP;
        CLOSE c_cur_schd;  
        END LOOP; --l_multi_org_val end loop
     END IF;
     write_log('Log: g_ooh_schd_data.COUNT : '||g_ooh_schd_data.COUNT
              ||'~g_tab_name~'||g_tab_name);

     /*Inserting OOH schedules in the custom waterfall*/
     IF g_ooh_schd_data.COUNT > 0
     THEN

        l_sql_stmt:=      'BEGIN
                              FORALL dat IN VALUES OF RPRO_SIEMENS_REP_CUSTOM_PKG.g_ooh_indx_tab
                                 INSERT INTO '||g_tab_name||'
                                 VALUES  RPRO_SIEMENS_REP_CUSTOM_PKG.g_ooh_schd_data(dat);
                           END;';

        write_log('Log: Inserting OOH Schedules: l_sql_stmt :'||l_sql_stmt);
        BEGIN
           EXECUTE IMMEDIATE l_sql_stmt;
        EXCEPTION WHEN OTHERS
        THEN
           write_error('Error: While inserting OOH Schedules SQLERRM~'||SQLERRM||'~'||dbms_utility.format_error_backtrace);
        END;

     END IF;

     l_sql_stmt := NULL;
     RPRO_SIEMENS_REP_CUSTOM_PKG.g_ooh_indx_tab.DELETE;  --20190729
     RPRO_SIEMENS_REP_CUSTOM_PKG.g_ooh_schd_data.DELETE; --20190729
     COMMIT;
  EXCEPTION WHEN OTHERS
  THEN
     write_error('Main Exception: create_snapshot_table~'||SQLERRM||'~'||dbms_utility.format_error_backtrace);
  END create_snapshot_table;
  -----------------------------------------------------------------------------
  PROCEDURE siemens_waterfall_jpn_rep ( p_report_id       IN     NUMBER
                                  , p_rep_layout_id   IN     NUMBER
                                  , p_no_tags         IN     VARCHAR2
                                  , p_restrict_rows   IN     VARCHAR2
                                  , p_query          OUT     VARCHAR2
                                  , p_header         OUT     VARCHAR2
                                  , p_num_rows       OUT     NUMBER
                                  )
    AS

     CURSOR c_fields (p_rep_layout_id IN NUMBER)
     IS
     SELECT * FROM (SELECT NVL (rrf.fld_name, rlb.col_name)                  AS         col_name
                          ,NVL (SUBSTR (rlb.report_label,1,30),rlb.col_name)            label
                          ,NVL (rlb.col_type, 'VARCHAR2')                               col_type
                          ,NVL2 (alias, alias || '.', NULL)                             alias
                          ,rpro_layout_field_pkg.get_zero_check_flag (rlf.indicators)   zero_check
                          ,rpro_layout_field_pkg.get_order_by_flag (rlf.indicators)     order_by
                          ,rrf.id
                          ,rlf.seq
                          ,CASE WHEN rrf.alias = 'RRH' AND rrf.fld_name = 'ID' THEN 'Y'
                                WHEN rrf.fld_name = 'RC_ID' THEN 'Y'
                                ELSE 'N'
                           END                                                          is_rc_id
                          ,'A' || rrf.id                                                fld_name
                     FROM rpro_rep_field    rrf
                         ,rpro_rp_layout    rl
                         ,rpro_layout_field rlf
                         ,rpro_label        rlb
                     WHERE rrf.rep_id                                                      = rl.rep_id
                     AND   rl.id                                                           = rlf.layout_id
                     AND   rlf.field_id                                                    = rrf.id
                     AND   rlb.id                                                          = rrf.label_id
                     AND   rl.id                                                           = p_rep_layout_id
                     AND NVL(rpro_rp_layout_pkg.get_aggregate_rep_flag(rl.indicators),'N') = 'N'
                    UNION ALL
                    SELECT NVL (rrs.fld_name, rrf.fld_name)                         AS col_name
                          ,NVL (SUBSTR (rlb.report_label,1,30),rlb.col_name)           label
                          ,NVL (rlb.col_type, 'VARCHAR2') col_type
                          ,NULL alias
                          ,rpro_layout_field_pkg.get_zero_check_flag (rlf.indicators)  zero_check
                          ,rpro_layout_field_pkg.get_order_by_flag (rlf.indicators)    order_by
                          ,rrf.id
                          ,rlf.seq
                          ,'N'                                                         is_rc_id
                          ,'A' || rrf.id                                               fld_name
                     FROM rpro_rep_field    rrf
                         ,rpro_rp_layout    rl
                         ,rpro_layout_field rlf
                         ,rpro_label        rlb
                         ,rpro_rep_ps_setup rrs
                     WHERE rrf.rep_id                                             = rl.rep_id
                     AND rl.id                                                    = rlf.layout_id
                     AND rlf.field_id                                             = rrf.id
                     AND rlb.id                                                   = rrf.label_id
                     AND rl.id                                                    = p_rep_layout_id
                     AND rpro_rp_layout_pkg.get_aggregate_rep_flag(rl.indicators) = 'Y'
                     AND rrs.label_id(+)                                          = rrf.label_id
                )
      ORDER BY seq
              ,id;

    l_proc                           VARCHAR2 (30) := 'siemens_waterfall_jpn_rep';
    l_report_limit                   NUMBER                         ;
    l_period_overflow_buffer         NUMBER                         ;
    i                                NUMBER                         ;
    l_column_value                   VARCHAR2 (240)                 ;
    l_period                         VARCHAR2 (50)                  ;
    l_currency_type                  VARCHAR2 (20)                  ;
    l_top_stmt                       VARCHAR2 (32767)               ;
     l_top_stmt1                       VARCHAR2 (32767)               ;
    l_prd_query                      VARCHAR2 (32767)               ;
    l_pivot_stmt                     VARCHAR2 (32767)               ;
    l_stmt2                          VARCHAR2 (32767)               ;
    l_head_value                     VARCHAR2 (32767)               ;
    j                                NUMBER :=1                     ;
    l_open_period_id                 NUMBER                         ;
    l_book_id                        NUMBER                         ;
    l_period_name                    VARCHAR2(100)                  ;
    l_client_id                      NUMBER                         ;
    l_sec_atr_val                    VARCHAR2(1000)                 ;
    l_output_fmt                     VARCHAR2(100)                  ;
    l_alignment                      VARCHAR2(100)                  ;
    l_rc_str                         VARCHAR2(32767)                ;
    l_ui_type                        VARCHAR2(50)                   ;
    l_rep_sec_atr_val                rpro_period_g.sec_atr_val%TYPE ;
    l_same_open_prd                  VARCHAR2(1)                    ;
    l_end_prd_id                     NUMBER                         ;
    ---------------------------------------------
    g_amt_format         VARCHAR2 (100);
    g_date_format        VARCHAR2 (100);
    g_date_time_format   VARCHAR2 (100);
    g_num_format         VARCHAR2 (100);
    g_pct_format         VARCHAR2 (100);
    g_qty_format         VARCHAR2 (100);
    g_table_name         VARCHAR2 (100);
    l_run_prd_id         NUMBER;
    l_curr_rate          VARCHAR2(100);
    l_amt_col            VARCHAR2(2000);
    l_qtr_prd_id         NUMBER;
    l_rem_prd_id         NUMBER;
    ---------------------------------------------
  BEGIN
    rpro_utility_pkg.set_revpro_context;
    BEGIN
      g_amt_format        := NVL (rpro_utility_pkg.get_profile ('AMT_FORMAT'), '999G999G999G999G990D00');
      g_qty_format        := NVL (rpro_utility_pkg.get_profile ('QTY_FORMAT'), '999G999G999G999G990D00');
      g_date_format       := NVL (rpro_utility_pkg.get_profile ('DATE_FORMAT'), 'DD-MON-YYYY');
      g_date_time_format  := NVL (rpro_utility_pkg.get_profile ('DATE_TIME_FORMAT'),'YYYY-MM-DD HH24:MI:SS');
      g_pct_format        := NVL (rpro_utility_pkg.get_profile ('PCT_FORMAT'), '999G999G999G999G990D00');
      g_num_format        := NVL (rpro_utility_pkg.get_profile ('NUM_FORMAT'),'999G999G999G999G990D00');
      l_report_limit      := NVL (rpro_utility_pkg.get_profile ('RUN_REPORT_LIMIT'), 10000);
    EXCEPTION
      WHEN OTHERS THEN
        l_report_limit      := 10000;
        g_amt_format        := '999G999G999G999G990D00';
        g_date_format       := 'DD-MON-RRRR';
        g_date_time_format  := 'YYYY-MM-DD HH24:MI:SS';
        g_qty_format        := '999G999G999G999G990D00';
        g_num_format        := '999G999G999G999G990D00';
        g_pct_format        := '999G999G999G999G990D00';
    END;


    l_client_id   := rpro_utility_pkg.get_client_id;
    l_sec_atr_val := '('''||REPLACE(rpro_utility_pkg.g_sec_atr_val,':',''',''')||''')';
    l_ui_type     := rpro_utility_pkg.get_ui_type;
    l_output_fmt  := CASE WHEN l_ui_type = 'G' THEN 'JSON' ELSE 'UNKNOWN' END;

    rpro_utility_pkg.record_log_act (l_proc,'Output Format ~ ' || l_output_fmt,10);

    rpro_rp_frmwrk_pkg.set_gbl_ui_format(l_ui_type); -- Set UI formatting

    i := rpro_rp_frmwrk_pkg.g_param.FIRST;
    rpro_utility_pkg.record_log_act (l_proc,'rpro_rp_frmwrk_pkg.g_param.COUNT~'||rpro_rp_frmwrk_pkg.g_param.COUNT,10);

    WHILE (i IS NOT NULL)
    LOOP
       rpro_utility_pkg.record_log_act (l_proc,'Inside Filters~ '||i,10);
       l_column_value      := rpro_rp_frmwrk_pkg.g_param (i).COLUMN_VALUE;

           IF rpro_rp_frmwrk_pkg.g_param (i).column_name IN ('REVPRO_PERIOD', 'PERIOD_NAME')
           THEN                                                                                                  --20131120
               l_period      := l_column_value;
           ELSIF rpro_rp_frmwrk_pkg.g_param (i).column_name = 'CURRENCY_TYPE'
           THEN
               l_currency_type      := l_column_value; 
           ELSIF rpro_rp_frmwrk_pkg.g_param (i).column_name = 'SEC_ATR_VAL'  --MS20201207
           THEN
               IF l_column_value IS NOT NULL AND NVL (rpro_rp_frmwrk_pkg.g_param (i).operator, '=') <> 'BETWEEN' AND rpro_rp_frmwrk_pkg.g_param (i).column_type <> 'SYSTEM' THEN
                  l_rep_sec_atr_val     := l_column_value; --making sure the parameter is passed with value else ignore   
               END IF;                                                                                                                                           --20140501
           END IF;
       i                   := rpro_rp_frmwrk_pkg.g_param.NEXT (i);
    END LOOP;

    SELECT id
    INTO   l_run_prd_id
    FROM  rpro_calendar_g
    WHERE period_name = l_period;

    IF l_currency_type = 'F'
    THEN
       l_curr_rate := ' * "F Ex Rate"';
    ELSIF l_currency_type = 'R'
    THEN
       l_curr_rate := ' * "G Ex Rate"';
    END IF;

    rpro_utility_pkg.record_log_act (l_proc,'l_period ~ '  || l_period
                                    ||'~l_currency_type~'  || l_currency_type
                                    ||'~p_rep_layout_id~'  || p_rep_layout_id
                                    ||'~l_rep_sec_atr_val~'|| l_rep_sec_atr_val
                                    ,10);


    IF l_book_id IS NULL THEN
       l_book_id     := rpro_utility_pkg.g_book_id;
    END IF;

    BEGIN
       SELECT DISTINCT cal.id       --MS20201207 Multiple open period is not supported
                      ,period_name
       INTO   l_open_period_id
             ,l_period_name
       FROM rpro_period   per
           ,rpro_calendar cal
       WHERE status  = 'OPEN'
       AND   book_id = l_book_id
     --AND   per.sec_atr_val  = rpro_utility_pkg.g_sec_atr_val --MS20201207 Commented for Multiple Org
       AND   cal.id  =  per.id;
    EXCEPTION
       WHEN NO_DATA_FOUND
         OR TOO_MANY_ROWS THEN
         rpro_utility_pkg.record_err_act (l_proc, 'ERROR: Period not open/Multiple open period is not supported ' );
    END;
    --Report should not be run for the future periods
    IF l_run_prd_id > l_open_period_id
    THEN
       write_log('Log: Report cannot be run for the future period :'||l_period);
       p_query := q'(SELECT 'Report cannot be run for the future periods' "Error Message" FROM DUAL)';

       IF l_output_fmt = 'JSON'
       THEN
       p_header := '{"header":[{'
                   ||' "dataField":"'||'Error Message'||'",'
                   ||RPRO_RP_FRMWRK_PKG.GET_JSON_HDR_TYPE('VARCHAR2')
                   ||l_alignment
                   ||' "caption":"'||'Error Message'||'"'
                   ||'}]}';
       ELSE
          p_header := p_header||'Error Message:';
       END IF;
       write_log('Log: p_query : ' ||p_query);
       write_log('Log: p_header : '||p_header);
       RETURN;
  --ELSE   -- commented due to multi org changes 20201207
       --Creating Snapshot for the given period
       --create_snapshot_table(l_run_prd_id,l_open_period_id);
       --Creating Snapshot for the given period
    END IF;



    IF l_rep_sec_atr_val IS NULL THEN
        
      --Commented due to multi org changes 20201207
      --SELECT MIN(org_id)   
      --INTO   l_rep_sec_atr_val
      --FROM  rpro_org
      --WHERE rpro_org_pkg.get_enabled_flag(indicators) = 'Y'
      --AND   org_id                                    = 0;

      -- To get if all the Orgs are in the same period
      SELECT DECODE (COUNT(DISTINCT rp.id), 1, 'Y', 'N')
      INTO l_same_open_prd
      FROM rpro_period rp
          ,rpro_org    ro
      WHERE rp.status = 'OPEN'
      AND   rp.sec_atr_val = ro.org_id
      AND   rpro_org_pkg.get_enabled_flag(ro.indicators) = 'Y';
    END IF;


    IF  l_rep_sec_atr_val IS NULL
    AND l_same_open_prd   = 'N'
    THEN
      IF l_output_fmt = 'JSON'
      THEN
         p_header := p_header ||',{'
                              ||' "dataField":"'||'Warning - Org Name All is not supported in Waterfall Report for Multiple Open Periods'||'",'
                              ||rpro_rp_frmwrk_pkg.get_json_hdr_type('VARCHAR2')
                              ||' "caption":"'||'Warning - Org Name All is not supported in Waterfall Report  for Multiple Open Periods'||'"'
                              ||'}';

         p_header := regexp_replace(p_header,',','',1,1);

         p_header := '{'
                   ||'"header":['
                   ||p_header
                   ||'],'
                   ||'"rc_id":['
                   || l_rc_str
                   ||']'
                   ||'}';

      ELSE
         p_header    := 'Warning';
      END IF;
      p_query     := 'SELECT ''Org Name ''''All'''' is not supported in Waterfall Report for Multiple Open Periods'' Warning FROM DUAL';
      p_num_rows  := 1;

      rpro_utility_pkg.record_err_act (l_proc, 'Warning: Org Name - All is not supported in waterfall Report for Multiple Open Periods' );

      RETURN;
    END IF;

    IF l_rep_sec_atr_val IS NOT NULL
    THEN
     --l_sec_atr_val := '('''||l_rep_sec_atr_val||''')';  --MS20201207
       l_sec_atr_val   := l_rep_sec_atr_val;
       g_tab_name := 'RPRO_SIEM_WF_' || l_run_prd_id||'_'||l_rep_sec_atr_val;
    ELSIF l_same_open_prd = 'Y'
    THEN
     --l_sec_atr_val := '('''||replace(rpro_utility_pkg.g_sec_atr_val,':',''',''')||''')';                
       l_sec_atr_val := rpro_utility_pkg.g_sec_atr_val ;
       g_tab_name    := 'RPRO_SIEM_WF_' || l_run_prd_id;
    END IF;
    /*Creating Snapshot for the given period*/     --MS20201207
    create_snapshot_table(l_run_prd_id,l_open_period_id,l_sec_atr_val); 
    
    IF l_run_prd_id = l_open_period_id  --LV20210315
    THEN
       BEGIN  --20201211 Added logic to handle only for current qurater should not consider the closed periods.
          l_prd_query :=  'UPDATE '||g_tab_name|| q'( tmp
                          SET
                            (
                              je_batch_name,
                              updt_prd_id
                            )
                            =
                            (SELECT period_year||'Q'||qtr_num 
                                   ,CASE WHEN tmp.prd_id >= )'||l_open_period_id||q'(
                                         THEN period_year||qtr_num
                                         ELSE '0'
                                    END
                             FROM rpro_calendar_g
                             WHERE id= tmp.prd_id
                            )
                          WHERE 1=1
                          AND EXISTS
                            (SELECT 1 FROM rpro_calendar_g rc WHERE rc.id = tmp.prd_id
                            )
                            )' ;
          write_log('Log: l_prd_query :'||l_prd_query);
          EXECUTE IMMEDIATE l_prd_query;
          
       EXCEPTION
       WHEN OTHERS THEN
          write_error('Error while updating quarter details');
       END;
    END IF;
    
    /*Creating Snapshot for the given period*/
    l_stmt2           := q'(SELECT DECODE(cust.atr3,NULL,'REV','NO','NO','OOH','OOH') "Rev Type",SIEM_WF_JPN.f_ex_rate "F Ex Rate",SIEM_WF_JPN.g_ex_rate "G Ex Rate",)';--                                                                                                                                  --20140530
    l_head_value      := q'("Rev Type","F Ex Rate","G Ex Rate",)';
    IF l_output_fmt = 'JSON'
    THEN
       l_alignment := ' "alignment":"left",';
       p_header := '{"header":[{'
                   ||' "dataField":"'||'Rev Type'||'",'
                   ||rpro_rp_frmwrk_pkg.get_json_hdr_type('VARCHAR2')
                   ||l_alignment
                   ||' "caption":"'||'Rev Type'||'"'
                   ||'}';                                                   --20190813 Changed Ord type to Rev type

       p_header := p_header ||',{'
                               ||' "dataField":"'||'F Ex Rate'||'",'
                               ||rpro_rp_frmwrk_pkg.get_json_hdr_type('NUMBER') -- SET ANGULAR FORMATTING
                               ||l_alignment
                               ||' "caption":"'||'F Ex Rate'||'"'
                               ||'}';

       p_header := p_header ||',{'
                               ||' "dataField":"'||'G Ex Rate'||'",'
                               ||rpro_rp_frmwrk_pkg.get_json_hdr_type('NUMBER') -- SET ANGULAR FORMATTING
                               ||l_alignment
                               ||' "caption":"'||'G Ex Rate'||'"'
                               ||'}';

    ELSE
       p_header := p_header||'Rev Type :F Ex Rate :G Ex Rate:';
    END IF;

      rpro_utility_pkg.record_log_act (l_proc
                                      ,'Log l_sec_atr_val ~'               ||l_sec_atr_val
                                       ||'~l_same_open_prd~'               ||l_same_open_prd
                                       ||'~rpro_utility_pkg.g_sec_atr_val~'||rpro_utility_pkg.g_sec_atr_val
                                       ||'~l_rep_sec_atr_val~'             ||l_rep_sec_atr_val
                                      ,10
                                      );

    FOR r_fld IN c_fields (p_rep_layout_id) LOOP
      l_stmt2           := l_stmt2 || r_fld.alias || r_fld.col_name || ' ' || r_fld.fld_name || ',';
      l_head_value      := l_head_value||' '|| r_fld.fld_name || ',';
      IF l_output_fmt = 'JSON' THEN
          IF r_fld.col_type IN ('AMOUNT', 'NUMBER','DATE','DATE_TIME','PERCENT','QUANTITY', 'ID') THEN
              l_alignment := ' "alignment":"right",';
          ELSE
              l_alignment := ' "alignment":"left",';
          END IF;
          p_header := p_header ||',{'
                               ||' "dataField":"'||r_fld.fld_name||'",'
                               ||rpro_rp_frmwrk_pkg.get_json_hdr_type(r_fld.col_type) -- SET ANGULAR FORMATTING
                               ||l_alignment
                               ||' "caption":"'||r_fld.label||'"'
                               ||'}';
          IF r_fld.is_rc_id = 'Y' THEN -- GET LIST OF RC_ID FOR DRILL TO WB
              l_rc_str := l_rc_str || ',{'||' "dataField":"'||r_fld.fld_name||'"}';
          END IF;
      ELSE
      p_header          := p_header || r_fld.label || ': ';
      END IF;
      j                 := j + 1;
    END LOOP;

    IF LENGTH (l_stmt2) > l_period_overflow_buffer THEN
      l_stmt2      := l_stmt2 || '''...''' || ',';

      IF p_no_tags = 'Y' THEN
        p_header      := p_header || '..(Exceeds Limit.All schedules cannot be displayed) : ';
      ELSE
        p_header      := p_header || '<span onmouseover="toolTip_enable(event,this,''Exceeds limit.All schedules cannot be displayed'');" >...</span>' || ': ';
      END IF;
    END IF;
    

    l_prd_query:= 'SELECT MAX(prd_id)
                   FROM  '||g_tab_name||'
                   WHERE 1 = 1';

    

    BEGIN
       EXECUTE IMMEDIATE l_prd_query
       INTO              l_end_prd_id;
    EXCEPTION WHEN OTHERS
    THEN
       l_end_prd_id := -1;
       write_error('Error: Unable to fetch max prd_id from custom waterfall table.');
    END;
    
    l_prd_query:= 'SELECT  updt_prd_id
                   FROM  '||g_tab_name||'
                   WHERE 1 = 1
                   AND   prd_id = :a
                   AND   rownum = 1';

    

    BEGIN
       EXECUTE IMMEDIATE l_prd_query
       INTO              l_qtr_prd_id
       USING             l_run_prd_id;
    EXCEPTION WHEN OTHERS
    THEN
       l_qtr_prd_id := -1;
       write_error('Error: Unable to fetch max prd_id from custom waterfall table.');
    END;
    
    BEGIN
       SELECT updt_prd_id
       INTO   l_rem_prd_id
       FROM
         (SELECT updt_prd_id ,
           RANK() OVER (ORDER BY updt_prd_id) rnk
         FROM
           (SELECT DISTINCT PERIOD_YEAR
             ||qtr_num updt_prd_id
           FROM rpro_calendar_g
           WHERE id >= l_run_prd_id
           AND id   <= NVL(l_end_prd_id,l_run_prd_id) 
           )
         )
       WHERE rnk = 8;
    EXCEPTION 
    WHEN OTHERS THEN
       l_rem_prd_id := 99999;
    END;
    
    rpro_utility_pkg.record_log_act (p_type      => l_proc
                                    ,p_text      => 'l_prd_query: 1'||'~l_run_prd_id~'||l_run_prd_id||'~l_end_prd_id~'||l_end_prd_id
                                                     ||'~l_qtr_prd_id~'||l_qtr_prd_id
                                    ,p_log_level => 10
                                    ,p_clob_text => l_prd_query
                                    );


    IF l_output_fmt = 'JSON'
    THEN
    p_header := p_header ||',{'
                               ||' "dataField":"'||'Prior Amt'||'",'
                               ||rpro_rp_frmwrk_pkg.get_json_hdr_type('AMOUNT') -- SET ANGULAR FORMATTING
                               ||l_alignment
                               ||' "caption":"'||'Prior Amt'||'"'
                               ||'}';
    ELSE
       p_header := p_header||'Prior Amt:';
    END IF;

    l_amt_col  := CASE WHEN l_currency_type = 'F'
                       THEN ' cust.amount * SIEM_WF_JPN.f_ex_rate '
                       WHEN l_currency_type = 'R'
                       THEN ' cust.amount * SIEM_WF_JPN.g_ex_rate * SIEM_WF_JPN.f_ex_rate '
                       ELSE ' cust.amount '
                  END;
    --20190730  Added prior amount
    l_stmt2 := l_stmt2 ||   '(SELECT NVL(SUM(amount),0)
                              FROM '||g_tab_name||q'( a
                              WHERE a.line_id     = cust.line_id
                              AND NVL(a.atr3,'N') = NVL(cust.atr3,'N')
                              AND updt_prd_id          <)'||l_qtr_prd_id||'
                              ) "Prior Amt" ,
                              (SELECT NVL(SUM(amount),0)
                              FROM '||g_tab_name||q'( a
                              WHERE a.line_id     = cust.line_id
                              AND NVL(a.atr3,'N') = NVL(cust.atr3,'N')
                              AND updt_prd_id          >)'||l_rem_prd_id||'
                              ) "Remaining Bal" ,
                             cust.updt_prd_id updt_prd_id,
                             ---cust.amount amount
                             '||l_amt_col || ' amount
                           FROM '||g_tab_name||' cust,
                                 rpro_rc_line_g SIEM_WF_JPN
                           WHERE SIEM_WF_JPN.id                      = cust.line_id
                           )';
    l_head_value := l_head_value||'"Prior Amt",';
    ---l_top_stmt := l_top_stmt||l_stmt2||'PIVOT ( SUM( amount'||l_curr_rate||') FOR prd_id IN (';
    l_top_stmt1 := l_stmt2||'PIVOT ( SUM( amount) FOR updt_prd_id IN (';

    FOR I IN (SELECT * FROM (SELECT DISTINCT PERIOD_YEAR||qtr_num updt_prd_id,PERIOD_YEAR||'Q'||qtr_num prd_qtr_name
                             --,id,period_name 
                             FROM rpro_calendar_g WHERE id >=l_run_prd_id AND id<=NVL(l_end_prd_id,l_run_prd_id) 
                             ORDER BY 1) 
              WHERE rownum<9) --MS20201207
    LOOP
       IF l_output_fmt = 'JSON'
       THEN
          p_header := p_header ||',{'
                             ||' "dataField":"'||i.prd_qtr_name||'",'
                             ||RPRO_RP_FRMWRK_PKG.GET_JSON_HDR_TYPE('AMOUNT')
                             ||l_alignment
                             ||' "caption":"'||i.prd_qtr_name||'"'
                             ||'}';
       ELSE
          p_header:=p_header||i.prd_qtr_name||':';
       END IF;
       l_pivot_stmt:=l_pivot_stmt||i.updt_prd_id||'"'||i.prd_qtr_name||'"'||',';
       l_head_value:=l_head_value||'NVL("'||i.prd_qtr_name||'",0) "'||i.prd_qtr_name||'",';
    END LOOP;
    
    --Added for Remaining balance
    IF l_output_fmt = 'JSON'
    THEN
    p_header := p_header ||',{'
                               ||' "dataField":"'||'Remaining Bal'||'",'
                               ||rpro_rp_frmwrk_pkg.get_json_hdr_type('AMOUNT') -- SET ANGULAR FORMATTING
                               ||l_alignment
                               ||' "caption":"'||'Remaining Bal'||'"'
                               ||'}';
    ELSE
       p_header := p_header||'Remaining Bal:';
    END IF;
    l_head_value := l_head_value||'"Remaining Bal",';
    
    --Added for Remaining balance
    
    
    l_pivot_stmt := RTRIM(LTRIM(l_pivot_stmt,','),',');
    p_header     := RTRIM(LTRIM(p_header,':'),':');
    p_header     := RTRIM(LTRIM(p_header,','),',');
    l_head_value := RTRIM(LTRIM(l_head_value,','),',');
    IF l_output_fmt = 'JSON'
    THEN
       p_header     := p_header||']}';
    END IF;
    l_top_stmt := 'SELECT * FROM (SELECT '||l_head_value||' FROM ( ';
    l_top_stmt := l_top_stmt||l_top_stmt1||l_pivot_stmt||') ))';

    p_query           := l_top_stmt;

    rpro_utility_pkg.record_log_act (p_type      => l_proc
                                    ,p_text      => 'p_query : '
                                    ,p_log_level => 10
                                    ,p_clob_text => p_query
                                    );

  EXCEPTION WHEN OTHERS
  THEN
     rpro_utility_pkg.record_log_act (l_proc
                                     ,'Main Exception: siemens_waterfall_jpn_rep'||SQLERRM||CHR(13)||dbms_utility.format_error_backtrace
                                     ,10
                                     );
  END siemens_waterfall_jpn_rep;
    
   PROCEDURE tax_acct_reclass (p_errbuf         OUT VARCHAR2
                              ,p_retcode        OUT NUMBER)
   IS
      CURSOR c_schd_rev (p_prd_id NUMBER,p_org_id NUMBER)
      IS
      SELECT root_line_id line_id,
             SUM(amount) rev_amt
      FROM   rpro_rc_schd
      WHERE  sec_atr_val            =   p_org_id
      AND    prd_id                 =   p_prd_id
      AND    SUBSTR(indicators,2,2) IN ('LR','UR')
      GROUP BY root_line_id
      HAVING SUM(amount) !=0;
      CURSOR c_rc_line
      IS
      SELECT id line_id
            ,doc_line_id
            ,rc_id
            ,rc_pob_id
            ,curr
            ,f_cur
            ,f_ex_rate
            ,g_ex_rate
            ,doc_date
            ,num1 rc_version
      FROM rpro_rc_line;
      TYPE typ_rc_line     IS TABLE OF c_rc_line%ROWTYPE
      INDEX BY PLS_INTEGER;
      TYPE typ_rc_schd_rev IS TABLE OF c_schd_rev%ROWTYPE
      INDEX BY PLS_INTEGER;
      TYPE typ_je_head     IS TABLE OF rpro_je_head%ROWTYPE
      INDEX BY PLS_INTEGER;
      TYPE typ_je_line     IS TABLE OF rpro_je_line%ROWTYPE
      INDEX BY PLS_INTEGER;
      TYPE typ_rc_schd_cl  IS TABLE OF rpro_rc_schd%ROWTYPE
      INDEX BY PLS_INTEGER;
      TYPE typ_line        IS TABLE OF c_rc_line%ROWTYPE
      INDEX BY VARCHAR2(300);
      TYPE typ_schd_rev    IS TABLE OF c_schd_rev%ROWTYPE
      INDEX BY VARCHAR2(300);
      TYPE typ_je_grp      IS TABLE OF rpro_je_head.id%TYPE
      INDEX BY VARCHAR2(500);
      t_line_id          RPRO_VARCHAR_TAB_TYPE := RPRO_VARCHAR_TAB_TYPE();
      t_je_hd_id         RPRO_VARCHAR_TAB_TYPE := RPRO_VARCHAR_TAB_TYPE();
      l_rc               rpro_rc_head.id%TYPE;
      l_line_id          rpro_rc_line.id%TYPE;
      l_prd_start_date   rpro_calendar.start_date%TYPE;
      t_je_head_ins      typ_je_head;
      t_je_line_ins      typ_je_line;
      t_rc_schd_rev      typ_rc_schd_rev;
      t_rc_line          typ_rc_line;
      t_schd_rev         typ_schd_rev;
      t_je_head_id       typ_je_grp;
      t_line             typ_line;
      t_rc_schd_cl       typ_rc_schd_cl;
      l_je_head_idx      PLS_INTEGER   := 0;
      l_je_line_idx      PLS_INTEGER   := 0;
      l_inv_chk          NUMBER        := 0;
      l_cmc_chk          NUMBER        := 0;
      l_prd_id           NUMBER        := 0;
      l_rev_amt          NUMBER        := 0;
      l_je_amt           NUMBER        := 0;
      l_tax_org_chk      NUMBER        := 0;
      l_tax_job_chk      NUMBER        := 0;
      l_retcode          NUMBER;
      l_seq              NUMBER;
      l_code             NUMBER;
      l_request_id       NUMBER;
      l_prog_id          NUMBER;
      l_je_head_ind      VARCHAR2(200);
      l_je_line_ind      VARCHAR2(200);
      l_rc_schd_ind      VARCHAR2(200);
      l_tax_org_id       NUMBER        := rpro_utility_pkg.get_lkp_val('TAX_MJE','ORG_ID');
      l_tax_pct          NUMBER        := rpro_utility_pkg.get_lkp_val('TAX_MJE','TAX_PCT');
      l_category_code    VARCHAR2(300) := rpro_utility_pkg.get_lkp_val('TAX_MJE','CATEGORY_CODE');
      l_activity_type    VARCHAR2(300) := rpro_utility_pkg.get_lkp_val('TAX_MJE','ACTIVITY_TYPE');
      l_reason_code      VARCHAR2(300) := rpro_utility_pkg.get_lkp_val('TAX_MJE','REASON_CODE');
      l_dr_activity_type VARCHAR2(300) := rpro_utility_pkg.get_lkp_val('TAX_MJE','DR_ACTIVITY_TYPE');
      l_cr_activity_type VARCHAR2(300) := rpro_utility_pkg.get_lkp_val('TAX_MJE','CR_ACTIVITY_TYPE');
      l_cl_acct_seg      VARCHAR2(200) := rpro_utility_pkg.get_lkp_val('TAX_MJE','CL_ACCT_SEG');
      l_tax_acct_seg     VARCHAR2(200) := rpro_utility_pkg.get_lkp_val('TAX_MJE','TAX_ACCT_SEG');
      l_tax_code         VARCHAR2(200) := rpro_utility_pkg.get_lkp_val('TAX_MJE','TAX_CODE');
      l_errbuf           VARCHAR2(300);
      l_message          VARCHAR2(300);
      l_je_grp           VARCHAR2(500);
      l_tax_pay_acct     VARCHAR2(10);
      l_tax_cl_acct      VARCHAR2(10);
      ex_tax_org_chk     EXCEPTION;
      ex_tax_job_chk     EXCEPTION;
   BEGIN
      write_log('*********************************************************************************');
      write_log(' Begin Tax_acct_reclass : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      SELECT COUNT(1) 
      INTO l_tax_org_chk
      FROM rpro_role_org  role_org
           ,rpro_user_role user_role 
      WHERE role_org.org_id     = l_tax_org_id
      AND   UPPER(user_role.user_name) = UPPER(g_user)
      AND   role_org.role_id    = user_role.role_id;
      IF l_tax_org_chk = 0
      THEN
         RAISE ex_tax_org_chk;
      END IF;
      SELECT cal.id 
            ,cal.start_date
      INTO  l_prd_id
           ,l_prd_start_date
      FROM rpro_calendar cal
      WHERE id = (SELECT prd.id 
                  FROM rpro_period prd
                  WHERE prd.sec_atr_val = l_tax_org_id);
      write_log('prd_id:'||l_prd_id);
      SELECT COUNT(1)
      INTO l_tax_job_chk
      FROM rpro_je_head
      WHERE SEC_ATR_VAL =     l_tax_org_id
      AND   crtd_prd_id =     l_prd_id
      AND   name        LIKE 'Tax_MJE_%';
      IF l_tax_job_chk > 0
      THEN
         RAISE ex_tax_job_chk;
      END IF;
      OPEN c_schd_rev(l_prd_id,l_tax_org_id);
      FETCH c_schd_rev
      BULK COLLECT
      INTO t_rc_schd_rev;
      write_log('t_rc_schd_rev count:'||t_rc_schd_rev.LAST);
      FOR i IN t_rc_schd_rev.FIRST .. t_rc_schd_rev.LAST
      LOOP
         t_schd_rev(t_rc_schd_rev(i).line_id).rev_amt := t_rc_schd_rev(i).rev_amt;
         t_line_id.EXTEND;
         t_line_id(t_line_id.LAST) := t_rc_schd_rev(i).line_id;
      END LOOP;
      SELECT line.id
            ,line.doc_line_id
            ,line.rc_id
            ,line.rc_pob_id
            ,line.curr
            ,line.f_cur
            ,line.f_ex_rate
            ,line.g_ex_rate
            ,line.doc_date
            ,head.version
      BULK COLLECT 
      INTO 
      t_rc_line
      FROM rpro_rc_line line
          ,rpro_rc_head head
      WHERE head.sec_atr_val = l_tax_org_id
      AND   EXISTS (SELECT 1 
                    FROM TABLE(t_line_id) line_tbl 
                    WHERE line_tbl.column_value = line.id)
      AND   line.rc_id = head.id;
      SELECT id
      INTO l_tax_pay_acct
      FROM rpro_acct_type 
      WHERE NAME = l_cr_activity_type;
      SELECT id
      INTO l_tax_cl_acct
      FROM rpro_acct_type 
      WHERE NAME = l_dr_activity_type;
      l_je_head_ind    := rpro_je_head_pkg.set_status_flag(rpro_je_head_pkg.g_set_default_ind,'N');
      l_je_head_ind    := rpro_je_head_pkg.set_auto_appr_flag(l_je_head_ind,'Y');
      l_je_line_ind    := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y');
      l_rc_schd_ind    := rpro_rc_schd_pkg.set_schd_type_flag(rpro_rc_schd_pkg.g_set_default_ind,l_tax_pay_acct);
      l_rc_schd_ind    := rpro_rc_schd_pkg.set_interfaced_flag ( l_rc_schd_ind,'Y');
      l_rc_schd_ind    := rpro_rc_schd_pkg.set_cr_acctg_flag ( l_rc_schd_ind,l_tax_cl_acct);
      l_rc_schd_ind    := rpro_rc_schd_pkg.set_initial_rep_entry_flag ( l_rc_schd_ind,'Y');
      FOR i IN t_rc_line.FIRST .. t_rc_line.LAST
      LOOP
         l_line_id := t_rc_line(i).line_id;
         l_je_grp  := t_rc_line(i).f_cur;
         IF NOT t_je_head_id.EXISTS (l_je_grp)
         THEN
            --JE Head Entry
            l_je_head_idx                              := l_je_head_idx + 1;
            t_je_head_id(l_je_grp)                     := rpro_utility_pkg.generate_id ('RPRO_RC_HEAD_ID_S',g_client_id);
            t_je_head_ins(l_je_head_idx).id            := t_je_head_id(l_je_grp);
            t_je_head_ins(l_je_head_idx).name          := 'Tax_MJE_'||t_je_head_id(l_je_grp)||'_'||l_prd_id;
            t_je_head_ins(l_je_head_idx).description   := 'Tax account MJE for period:'||l_prd_id;
            t_je_head_ins(l_je_head_idx).category_code := l_category_code;
            t_je_head_ins(l_je_head_idx).ex_rate_type  := 'User';
            t_je_head_ins(l_je_head_idx).sob_id        :=  1;
            t_je_head_ins(l_je_head_idx).sob_name      := '1';
            t_je_head_ins(l_je_head_idx).fn_cur        := t_rc_line(i).f_cur;
            t_je_head_ins(l_je_head_idx).atr1          := l_tax_code;
            t_je_head_ins(l_je_head_idx).prd_id        := l_prd_id;
            t_je_head_ins(l_je_head_idx).client_id     := g_client_id;
            t_je_head_ins(l_je_head_idx).crtd_prd_id   := l_prd_id;
            t_je_head_ins(l_je_head_idx).sec_atr_val   := l_tax_org_id;
            t_je_head_ins(l_je_head_idx).crtd_by       := g_user;
            t_je_head_ins(l_je_head_idx).crtd_dt       := SYSDATE;
            t_je_head_ins(l_je_head_idx).updt_by       := g_user;
            t_je_head_ins(l_je_head_idx).updt_dt       := SYSDATE;
            t_je_head_ins(l_je_head_idx).indicators    := l_je_head_ind;
            t_je_head_ins(l_je_head_idx).book_id       := g_book_id;
            t_je_hd_id.EXTEND;
            t_je_hd_id(t_je_hd_id.LAST)                := t_je_head_id(l_je_grp);           
         END IF;
         l_rev_amt                                     := t_schd_rev(l_line_id).rev_amt;
         l_je_amt                                      := round(l_rev_amt/l_tax_pct,0); --Japan Currency yen does not have decimals
         --JE Line Entry
         l_je_line_idx                                 := l_je_line_idx + 1;
         t_je_line_ins(l_je_line_idx).id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id);
         t_je_line_ins(l_je_line_idx).header_id        := t_je_head_id(l_je_grp);
         t_je_line_ins(l_je_line_idx).activity_type    := l_activity_type;
         t_je_line_ins(l_je_line_idx).curr             := t_rc_line(i).curr;
         t_je_line_ins(l_je_line_idx).amount           := ABS(ROUND(l_je_amt,2));
         t_je_line_ins(l_je_line_idx).ex_rate          := t_rc_line(i).f_ex_rate;
         t_je_line_ins(l_je_line_idx).g_ex_rate        := t_rc_line(i).g_ex_rate;
         t_je_line_ins(l_je_line_idx).ex_rate_date     := t_rc_line(i).doc_date;
         t_je_line_ins(l_je_line_idx).start_date       := l_prd_start_date;
         t_je_line_ins(l_je_line_idx).end_date         := l_prd_start_date;
         t_je_line_ins(l_je_line_idx).func_amount      := ABS(ROUND(l_je_amt,2))*t_rc_line(i).f_ex_rate;
         t_je_line_ins(l_je_line_idx).reason_code      := l_reason_code;
         t_je_line_ins(l_je_line_idx).description      := 'TAX ACCOUNT MJE';
         t_je_line_ins(l_je_line_idx).comments         := 'TAX ACCOUNT MJE';
         t_je_line_ins(l_je_line_idx).dr_cc_id         := 999;
         t_je_line_ins(l_je_line_idx).dr_activity_type := l_dr_activity_type;
         t_je_line_ins(l_je_line_idx).dr_segment1      := l_cl_acct_seg;
         t_je_line_ins(l_je_line_idx).cr_cc_id         := 999;
         t_je_line_ins(l_je_line_idx).cr_activity_type := l_cr_activity_type;
         t_je_line_ins(l_je_line_idx).cr_segment1      := l_tax_acct_seg;
         t_je_line_ins(l_je_line_idx).client_id        := g_client_id;
         t_je_line_ins(l_je_line_idx).crtd_prd_id      := l_prd_id;
         t_je_line_ins(l_je_line_idx).crtd_by          := g_user;
         t_je_line_ins(l_je_line_idx).crtd_dt          := SYSDATE;
         t_je_line_ins(l_je_line_idx).updt_by          := g_user;
         t_je_line_ins(l_je_line_idx).updt_dt          := SYSDATE;
         t_je_line_ins(l_je_line_idx).indicators       := l_je_line_ind;
         t_je_line_ins(l_je_line_idx).sec_atr_val      := l_tax_org_id;
         t_je_line_ins(l_je_line_idx).book_id          := g_book_id;
         t_je_line_ins(l_je_line_idx).rc_id            := t_rc_line(i).rc_id;
         t_je_line_ins(l_je_line_idx).doc_line_id      := t_rc_line(i).doc_line_id;
         t_je_line_ins(l_je_line_idx).rc_line_id       := t_rc_line(i).line_id;
         --Schd initial entry
         t_rc_schd_cl(i).id                            := rpro_utility_pkg.generate_id('rpro_rc_schd_id_s',g_client_id);
         t_rc_schd_cl(i).rc_id                         := t_rc_line(i).rc_id;
         t_rc_schd_cl(i).rc_ver                        := t_rc_line(i).rc_version;
         t_rc_schd_cl(i).line_id                       := t_rc_line(i).line_id;
         t_rc_schd_cl(i).pob_id                        := t_rc_line(i).rc_pob_id;
         t_rc_schd_cl(i).curr                          := t_rc_line(i).curr;
         t_rc_schd_cl(i).amount                        := ABS(ROUND(l_je_amt,2));
         t_rc_schd_cl(i).indicators                    := l_rc_schd_ind;
         t_rc_schd_cl(i).prd_id                        := l_prd_id;
         t_rc_schd_cl(i).post_prd_id                   := l_prd_id;
         t_rc_schd_cl(i).cr_segments                   := l_cl_acct_seg;
         t_rc_schd_cl(i).f_ex_rate                     := t_rc_line(i).f_ex_rate;
         t_rc_schd_cl(i).g_ex_rate                     := t_rc_line(i).g_ex_rate;
         t_rc_schd_cl(i).ex_rate_date                  := t_rc_line(i).doc_date;
         t_rc_schd_cl(i).client_id                     := g_client_id;
         t_rc_schd_cl(i).crtd_prd_id                   := l_prd_id;
         t_rc_schd_cl(i).sec_atr_val                   := l_tax_org_id;
         t_rc_schd_cl(i).crtd_by                       := g_user;
         t_rc_schd_cl(i).crtd_dt                       := SYSDATE;
         t_rc_schd_cl(i).updt_by                       := g_user;
         t_rc_schd_cl(i).updt_dt                       := SYSDATE;
         t_rc_schd_cl(i).ref_bill_id                   := 1;
         t_rc_schd_cl(i).root_line_id                  := t_rc_line(i).line_id;
         t_rc_schd_cl(i).book_id                       := g_book_id;
         t_rc_schd_cl(i).orig_line_id                  := t_rc_line(i).line_id;
         t_rc_schd_cl(i).atr1                          := 'Initial Entry due to Tax Account Customization';
         
      END LOOP;

      IF l_je_head_idx > 0
      THEN
         BEGIN
            FORALL a IN 1 .. t_je_head_ins.COUNT
            INSERT INTO rpro_je_head
            VALUES t_je_head_ins(a);
            FORALL b IN 1 .. t_je_line_ins.COUNT
            INSERT INTO rpro_je_line
            VALUES t_je_line_ins(b);
            FORALL c IN 1 .. t_rc_schd_cl.COUNT
            INSERT INTO rpro_rc_schd
            VALUES t_rc_schd_cl(c);
            COMMIT;
         EXCEPTION 
         WHEN OTHERS THEN
            write_error(' Error in JE data insert : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
         END;
         --mje validation and approval
         FOR i IN 1 .. t_je_head_ins.COUNT
         LOOP
            --mje validation processes
            BEGIN
               rpro_je_validation_pkg.je_validation_wrapper  ( l_errbuf
                                                             , l_retcode
                                                             , t_je_head_ins(i).id );
               write_log('MJE validation status '||l_errbuf ||'l_retcode: '||l_retcode || 'to the id: '|| t_je_head_ins(i).id);
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error: in JE validation api call'||'~Error~'||SQLERRM);
            END;
            BEGIN
               rpro_appr_pkg.initiate_approval ('JE'
                                               ,t_je_head_ins(i).id
                                               ,l_retcode
                                               ,l_errbuf);              
               write_log('initiate approvalto header id '||t_je_head_ins(i).id ||'l_retcode: '||l_retcode || 'l_errbuf: '|| l_errbuf);  
            EXCEPTION
            WHEN OTHERS
            THEN
               write_error('Error: initiate_approval' || SQLERRM);
            END;
            IF NVL(l_retcode,0)  = 0
            THEN
               BEGIN
                  SELECT id
                        ,seq
                  INTO   l_rc
                        ,l_seq
                  FROM   rpro_rc_appr_g aa
                  WHERE  aa.status      = 'Pending'
                  AND    aa.rc_id       =  t_je_head_ins(i).id
                  AND    aa.object_type =  'JE';
               EXCEPTION
               WHEN OTHERS THEN 
                  write_error('Error:Select on Appovers failed' || SQLERRM);
                  l_rc      :=NULL;
                  l_seq     := NULL;
               END;
               IF l_rc IS NOT NULL
               THEN
                  BEGIN
                     RPRO_APPR_PKG.APPROVE ('JE'
                                           ,t_je_head_ins(i).id
                                           ,g_book_id
                                           ,NULL
                                           ,l_rc
                                           ,l_seq
                                           ,'Releasing by Manual JE Custom process'
                                           ,g_user
                                           ,l_code
                                           ,l_message);
                     write_log('MJE validation status '||l_message ||'l_code: '||l_code || 'to the id: '|| t_je_head_ins(i).id); 
                  END; 
               END IF;
            END IF;
            UPDATE rpro_rc_schd
            SET indicators = rpro_rc_schd_pkg.set_interfaced_flag ( indicators,'Y')  --This TAX MJE Schedules should not participate in Transfer Accounting
            WHERE je_batch_id = t_je_head_ins(i).id;
         END LOOP;
         BEGIN
            SELECT id
            INTO l_prog_id 
            FROM rpro_prog_head 
            WHERE  name ='Revpro3.0 MJE Schd Interface Flag';
            rpro_job_scheduler_pkg.schedule_job(p_errbuf                 => l_errbuf
                                               ,p_retcode                => l_retcode
                                               ,p_request_id             => l_request_id
                                               ,p_program_id             => l_prog_id -- prog_id from prog_schd_g
                                               ,p_parameter_text         => l_tax_org_id   -- Prog parameters
                                               ,p_requested_start_date   => SYSDATE
                                               ,p_requested_end_date     => NULL
                                               ,p_request_date           => SYSDATE);
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Revpro3.0 MJE Schd Interface Flag'
                  ||CHR(10)
                  ||SQLERRM
                  ||CHR(10)
                  ||'Error Queue : '||dbms_utility.format_error_backtrace);                           
         END;
      ELSE
         write_log('No MJE entries for Tax_Acct_Reclass in Period:'||l_prd_id);
      END IF;
      COMMIT;
      write_log(' End Tax_acct_reclass : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      write_log('*********************************************************************************');
   EXCEPTION
   WHEN ex_tax_org_chk THEN
      ROLLBACK;
      p_retcode := 2;
      p_errbuf  := 'This User does not have permision to run this job';
      write_log('This User does not have access to org:'||l_tax_org_id);  
   WHEN ex_tax_job_chk THEN
      ROLLBACK;
      p_retcode := 2;
      p_errbuf  := 'This Job has already been run for this period';
      write_log('This Job has already been run for this period:'||l_prd_id);
   WHEN OTHERS THEN
      ROLLBACK;
      p_retcode := 2;
      p_errbuf  := 'Custom Error';
      write_error(' Error in Tax_acct_reclass : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
   END tax_acct_reclass;
   PROCEDURE tax_je_schd_flg (p_errbuf         OUT VARCHAR2
                             ,p_retcode        OUT NUMBER
                             ,p_tax_org_id     IN  VARCHAR2)
   AS
      TYPE typ_je_hd_id IS TABLE OF rpro_je_head.id%TYPE
      INDEX BY PLS_INTEGER;
      t_je_hd_id   typ_je_hd_id;
      l_prd_id     NUMBER;
      l_tax_org_id VARCHAR2(10);
   BEGIN
      write_log('*********************************************************************************');
      write_log(' Begin tax_je_schd_flg : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      SELECT id
      INTO l_prd_id
      FROM rpro_period prd
      WHERE prd.sec_atr_val = p_tax_org_id;
      write_log('l_prd_id:'||l_prd_id);
      SELECT id
      BULK COLLECT
      INTO  t_je_hd_id
      FROM  rpro_je_head
      WHERE SEC_ATR_VAL =     p_tax_org_id
      AND   crtd_prd_id =     l_prd_id
      AND   name        LIKE 'Tax_MJE_%';
      write_log('collection count:'||t_je_hd_id.COUNT);
      FOR i IN 1 .. t_je_hd_id.COUNT
      LOOP
      UPDATE rpro_rc_schd schd
      SET schd.indicators = rpro_rc_schd_pkg.set_interfaced_flag ( schd.indicators,'Y')  --This TAX MJE Schedules should not participate in Transfer Accounting
      WHERE schd.je_batch_id = t_je_hd_id(i);
      END LOOP;
      COMMIT;
      write_log(' End tax_je_schd_flg : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      write_log('*********************************************************************************');
   EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      p_retcode := 2;
      p_errbuf  := SQLERRM;
      write_error(' Error in tax_je_schd_flg : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
   END tax_je_schd_flg;   
  ----------------------
  ---------------------------------------------------------------------------------------------------
END RPRO_SIEMENS_REP_CUSTOM_PKG;
/
sho err;
/
