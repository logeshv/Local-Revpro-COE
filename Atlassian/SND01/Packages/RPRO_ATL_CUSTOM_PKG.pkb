create or replace PACKAGE BODY RPRO_ATL_CUSTOM_PKG
IS

   /*==========================================================================================+
   |                    ZUORA, San Jose, California                                            |
   +===========================================================================================+
   |                                                                                           |
   |    File Name:         RPRO_ATL_CUSTOM_PKG.pkb                                             |
   |    Object Name:       RPRO_ATL_CUSTOM_PKG                                                 |
   |                                                                                           |
   |    Pre-reqs:                                                                              |
   |                                                                                           |
   |    Exceptions:        Procedure to handle stage events                                    |
   |                                                                                           |
   |    Revision History:                                                                      |
   |    Date          Name              Revision Description                                   |
   |    =========    ===============   ========================================================|
   |    01 MAR 2018  Logesh Varathan   Created                                                 |
   |    26 APR 2018  Logesh Varathan   rpro_after_bndl_explode-Added Data augmnentation        |
   |    03 MAY 2018  Logesh Varathan   Stop iterate if immediate upgrade with cmod flag- N     |
   |    17-MAY-2018  Sreeni B          GL Outbound Stage Handler to Split GL Batch             |
   |    01-JUN-2018  Sreeni B          Credit Rule Changes for Recast and multi year st dt upd |
   |    12-JUN-2018  Arul S            CV amt,Adjustment Revenue issues fix                    |
   |    19-JUN-2018  Sera              Retrospective Changes                                   |
   |    21-FEB-2019  Venkatesh Gurusamy  SR#104431 , added during upgrade testing to make      |
   |                                     current period as CT_MOD date                         |
   |    04-NOV-2020  Haribabu P        Enhancement to new scenario on link/delink  20201104    |
   |    28-OCT-2020  Logesh Varathan   Enhancement to new scenatio on link/delink  20201028    |
   |    11-NOV-2020  Logesh Varathan   Enhancement to switch allocation            20201118    |
   |    19-NOV-2020  Logesh Varathan   Fix for count check                         20201119    |
   |    02-DEC-2020  Logesh Varathan   Moved to collection end                     20201202    |
   +===========================================================================================*/
   -------------------------
   ---  Global Variables ---
   -------------------------
   gc_module         CONSTANT  VARCHAR2(150)     := 'RPRO_ATL_CUSTOM_PKG';
   gc_log_level      CONSTANT  NUMBER            := 2;
   gc_error_level    CONSTANT  NUMBER            := 2;
   g_rc_id           rpro_rc_head.id%TYPE;
   g_alloc_trmt      VARCHAR2(150);--06192018
   g_limit           NUMBER := 5000;
   -------------------------------------------------------------------------------------------------------------------------
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
   -------------------------------------------------------------------------------------------------------------------------
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
  -----------------------------------------------------------------------------------------
   PROCEDURE rpro_atl_create_manual_rc(p_errbuf  OUT VARCHAR2
                                      ,p_retcode OUT NUMBER )

   AS

      CURSOR c_upg_line IS
      SELECT rrb.atr43       orig_inv_num
            ,rrb.atr46       sen_num
            ,rrb.start_date
            ,rrb.rc_id
            ,rrb.atr41       atr41
            ,rrb.atr17       atr17
      FROM   rpro_rc_bill_g       rrb
      WHERE  1                  = 1
      AND    rrb.type           = 'INV'
      AND    rrb.atr17          = 'Y'
      AND    NVL(rrb.atr55,'N') <>'Y'
      AND    UPPER(rrb.atr41)   = 'UPGRADE'
      AND    rrb.rc_id          >  0
      AND    rrb.doc_num       !=  NVL(rrb.atr43,'X')
      AND    (EXISTS (SELECT 1
                      FROM   rpro_rc_bill_g rrb1
                      WHERE  1                 = 1
                      AND    rrb.atr43         = rrb1.doc_num
                      AND    UPPER(rrb1.atr41) = 'UPGRADE'
                      AND    rrb1.type         = 'INV'
                    --AND    rrb1.atr17        = 'Y'
                      AND    rrb.atr46         = rrb1.atr46
                      AND    rrb1.rc_id        > 0)
                      OR
              EXISTS (SELECT 1
                      FROM   rpro_rc_bill_g rrb1
                      WHERE  1                  = 1
                      AND    rrb.atr43          = rrb1.doc_num
                      AND    UPPER(rrb1.atr41) <> 'UPGRADE'
                      AND    rrb1.type          = 'INV'
                      AND    rrb.atr46          = rrb1.atr46
                      AND    rrb1.rc_id         > 0
                      AND    rrb.start_date     < rrb1.end_date
                      ))
      GROUP BY   rrb.atr43
                ,rrb.atr46
                ,rrb.start_date
                ,rrb.rc_id
                ,rrb.atr41
                ,rrb.atr17;

      l_rc_id_data        VARCHAR2(1000);
      l_tmpl_id           rpro_rc_tmpl_g.id%TYPE;
      l_new_rc_id         rpro_rc_line.rc_id%TYPE;
      l_errbuf            VARCHAR2(1000);
      l_retcode           NUMBER;
      l_rec               NUMBER;
      l_upg_rc_id         NUMBER;
      l_rc_count          NUMBER;
      l_period_name       rpro_calendar_g.period_name%TYPE;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('Log:Inside '||l_proc);

      BEGIN
         SELECT id
         INTO   l_tmpl_id
         FROM   rpro_rc_tmpl_g rrtg
         WHERE  1                                                   = 1
         AND    UPPER(rrtg.name)                                    = 'ATL GROUPING RULE'
         AND    rpro_rc_tmpl_pkg.get_enabled_flag (rrtg.indicators) = 'Y'
         AND    TRUNC (SYSDATE) BETWEEN rrtg.start_date AND NVL (rrtg.end_date,TRUNC (SYSDATE));
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Exception: Error while fetching RC template~'||SQLERRM  );
      END;

      BEGIN
         SELECT rc.period_name
         INTO   l_period_name
         FROM   rpro_calendar_g rc
               ,rpro_period_g   rp
         WHERE  rc.id    =   rp.id;
      EXCEPTION
      WHEN OTHERS THEN
         l_period_name := NULL;
      END;

      FOR r_upg_line IN c_upg_line
      LOOP
        /*Open C_UPG_LINE */
         write_log('Log: Inside C_UPG_LINE~ LEVEL 1~RC_ID~'
                   ||r_upg_line.rc_id
                   ||'~ORIG_INV_NUM~'
                   ||r_upg_line.orig_inv_num
                   ||'~MOD_SEN_NUM~'
                   ||r_upg_line.sen_num
                   );

         SELECT   rc_id
         INTO     l_upg_rc_id
         FROM     rpro_rc_bill_g
         WHERE    atr43                         = r_upg_line.orig_inv_num
         AND      atr46                         = r_upg_line.sen_num
         AND      type                          = 'INV'
         AND      NVL(start_date,'01-JAN-2016') = NVL(r_upg_line.start_date,'01-JAN-2016')
         AND      rownum                        = 1
         GROUP BY rc_id;

         write_log('Log: Upgrade RC~'||l_upg_rc_id);

         FOR r_ref_line IN(SELECT   atr43
                                   ,atr46
                                   ,atr17 --cmod_dlag
                                   ,atr41 --Sale Action
                                   ,rc_id
                                   ,doc_num
                                   ,start_date
                                   ,end_date
                           FROM     rpro_rc_bill_g A
                           WHERE    1          = 1
                           AND      type       = 'INV'
                           AND      doc_num    = r_upg_line.orig_inv_num
                           AND      atr46      = r_upg_line.sen_num
                           AND      a.doc_num != NVL(a.atr43 ,'X')
                           AND      rc_id      > 0 --Eliminate delinked lines
                           ORDER BY end_date DESC
                           )
         LOOP
            /*Open Nested LOOP*/
            /*Handling the def amount*/

            IF r_ref_line.end_date < r_upg_line.start_date
            THEN
               write_log('Log: Exiting...Level 1 r_ref_line.end_date~'
                         ||r_ref_line.end_date
                         ||'~r_upg_line.start_date~'
                         ||r_upg_line.start_date
                         ||'~Mod sen num~'
                         ||r_ref_line.atr46
                         );
               /*Stop traversing further as the child line doesnt have def amount*/
               l_temp_inv_number:=NULL;
               EXIT;
            ELSE
               write_log('Log: Level 2 ~Line in def state');
            END IF;

            /*Deleting the exisitng records with old errors*/
            DELETE
            FROM    rpro_atl_cust_cmod_det
            WHERE   1                     = 1
            AND     invoice_num           = r_ref_line.doc_num
            AND     NVL(orig_inv_num,'X') = NVL(r_ref_line.atr43,'X')
            AND     NVL(mod_sen_num,'X')  = NVL(r_ref_line.atr43,'X');

            INSERT
            INTO rpro_atl_cust_cmod_det(rc_id
                                       ,invoice_num
                                       ,orig_inv_num
                                       ,mod_sen_num
                                       ,crtd_by
                                       ,crtd_dt
                                       ,updt_by
                                       ,updt_dt
                                        )
            VALUES                     (r_ref_line.rc_id
                                       ,r_ref_line.doc_num
                                       ,r_ref_line.atr43
                                       ,r_ref_line.atr46
                                       ,rpro_utility_pkg.g_user_id
                                       ,sysdate
                                       ,rpro_utility_pkg.g_user_id
                                       ,sysdate
                                       );

            write_log('Log: LEVEL 2~RC_ID~'
                    ||r_ref_line.rc_id
                    ||'~ORIG_INV_NUM~'
                    ||r_ref_line.atr43
                    ||'~MOD SEN NUM~'
                    ||r_ref_line.atr46
                    ||'~Cmod Flag~'
                    ||r_ref_line.atr17
                    );

            l_temp_inv_number       := r_ref_line.atr43;
            l_mod_sen_number        := r_ref_line.atr46;
            l_prnt_st_dt            := r_ref_line.start_date;

            IF NVL(r_ref_line.atr17,'N') = 'N' AND UPPER(r_ref_line.atr41) = 'UPGRADE'
            THEN
               write_log('Log: LEVEL 3~RC_ID~'
                          ||'Exiting...CMOD flag is N'
                        );
            ELSE
               WHILE l_temp_inv_number IS NOT NULL
               LOOP
                  write_log('Log: LEVEL 3~RC_ID~'
                           ||r_ref_line.rc_id
                           ||'~ORIG_INV_NUM~'
                           ||r_ref_line.atr43
                           );

                  rpro_atl_recursive_itrtn(l_temp_inv_number
                                          ,l_mod_sen_number
                                          ,l_prnt_st_dt
                                          );
               END LOOP;
            END IF;

            l_cnt                 := l_cnt+1 ;
            rc_tab(l_cnt).rc_id   := r_ref_line.rc_id;
            rc_tab(l_cnt).atr43   := r_ref_line.atr43;
            rc_tab(l_cnt).doc_num := r_ref_line.doc_num;
            rc_tab(l_cnt).atr41   := NULL;--r_ref_line.atr41;
            rc_tab(l_cnt).atr17   := r_ref_line.atr17;

         END LOOP;/*Close Nested LOOP*/

         l_cnt                 := l_cnt+1 ;
         rc_tab(l_cnt).rc_id   := l_upg_rc_id;
         rc_tab(l_cnt).atr43   := r_upg_line.orig_inv_num;
         rc_tab(l_cnt).atr41   := r_upg_line.atr41;
         rc_tab(l_cnt).atr17   := r_upg_line.atr17;
         rc_tab(l_cnt).doc_num := NULL;

         l_cnt:=0;

         /*Reintializing the collection*/
         FOR r_rec IN 1..rc_tab.COUNT
         LOOP
            rc_tab_type.EXTEND();
            rc_tab_type(r_rec).rc_id   := rc_tab(r_rec).rc_id;
            rc_tab_type(r_rec).doc_num := rc_tab(r_rec).doc_num; --Invoice Number
            rc_tab_type(r_rec).atr17   := rc_tab(r_rec).atr17;
            rc_tab_type(r_rec).atr41   := rc_tab(r_rec).atr41;
         END LOOP;

         SELECT DISTINCT rc_id
                        ,doc_num
                        ,atr17
                        ,atr41
         BULK COLLECT
         INTO rc_tab_type_dist_rc
         FROM TABLE(rc_tab_type);

         SELECT count(DISTINCT rc_id)
         INTO l_rc_count
         FROM TABLE(rc_tab_type);

         IF l_rc_count > 1
         THEN
            write_log('Log: Count RC_TAB_TYPE_DIST_RC~'||rc_tab_type_dist_rc.count);
            l_rc_id_data :='';
            g_alloc_trmt := 'P'; --06192018
            write_log('Log Initial: g_alloc_trmt~'||g_alloc_trmt);
            FOR rec IN rc_tab_type_dist_rc.FIRST..rc_tab_type_dist_rc.LAST
            LOOP
               rpro_atl_delink(p_rc_id   => rc_tab_type_dist_rc(rec).rc_id
                              ,p_atr17   => rc_tab_type_dist_rc(rec).atr17
                              ,p_atr41   => rc_tab_type_dist_rc(rec).atr41);
               l_rc_id_data := l_rc_id_data || '~'||rc_tab_type_dist_rc(rec).rc_id*(-1);
               write_log('Log: L_RC_ID_DATA~'||l_rc_id_data);

               write_log('Log: after delink g_alloc_trmt~'||g_alloc_trmt || '~rc_tab_type_dist_rc(rec).rc_id ~'||rc_tab_type_dist_rc(rec).rc_id);
            END LOOP;

            BEGIN
               l_errbuf  := NULL;
               l_retcode := NULL;

               write_log('Log: Final g_alloc_trmt~'||g_alloc_trmt);

               rpro_rc_manual_pkg.create_manual_rc( p_tmpl_id         => l_tmpl_id
                                                   ,p_doc_line_id_str => NULL
                                                   ,p_rc_pob_id_str   => NULL
                                                   ,p_comments        => 'New RC Created for atlassian custom  mod'
                                                   ,p_rc_id           => l_new_rc_id
                                                   ,p_errbuf          => l_errbuf
                                                   ,p_retcode         => l_retcode
                                                   ,p_commit_flag     => 'Y'
                                                   ,p_alloc_trmt      => g_alloc_trmt
                                                   ,p_alloc_per       => 'R'
                                                   ,p_rc_id_str       => RTRIM(l_rc_id_data,'~')
                                                  );
               write_log('Log: l_new_rc_id~'||l_new_rc_id);

               UPDATE rpro_rc_line_g
               SET    atr23     =   'Y'||'-'||g_alloc_trmt||'-'||l_period_name
               WHERE  rc_id     =   l_new_rc_id;

               UPDATE rpro_rc_bill_g
               SET    atr23     =   'Y'||'-'||g_alloc_trmt||'-'||l_period_name
               WHERE  rc_id     =   l_new_rc_id;

               UPDATE rpro_rc_line_g
               SET    atr55 = 'Y'
               ---      ,atr23 = 'Y'||'-'||g_alloc_trmt||'-'||l_period_name
               WHERE  atr43                         = r_upg_line.orig_inv_num
               AND    atr46                         = r_upg_line.sen_num
               AND    NVL(start_date,'01-JAN-2016') = NVL(r_upg_line.start_date,'01-JAN-2016');

               write_log('Log: rc line update count~'||sql%rowcount||'~rc_id~'||l_upg_rc_id);

               UPDATE rpro_rc_bill_g
               SET    atr55 = 'Y'
               ---      ,atr23 = 'Y'||'-'||g_alloc_trmt||'-'||l_period_name
               WHERE  atr43                         = r_upg_line.orig_inv_num
               AND    atr46                         = r_upg_line.sen_num
               AND    NVL(start_date,'01-JAN-2016') = NVL(r_upg_line.start_date,'01-JAN-2016');

               write_log('Log: rc bill update count~'||'rc_id~'||l_upg_rc_id||sql%rowcount);

               COMMIT;

               BEGIN
                  IF g_alloc_trmt = 'R'
                  THEN
                     rpro_fv_allocation_pkg.recalc_rc
                        (p_rc_id       => l_new_rc_id
                        ,p_book_id     => rpro_utility_pkg.g_book_id
                        ,p_rc_version  => 1
                        ,p_ret_msg     => l_errbuf
                        ,p_type        => 'REALLOCATE'
                        ,p_lock_rc     => 'N');

                     write_log('Log: reallocate ~'||'rc_id~'||l_new_rc_id || '~ Ret Msg ~'||l_errbuf);
                  END IF;
               EXCEPTION
               WHEN OTHERS THEN
                  write_error('Exception: '||sqlerrm||'~L_ERRBUF~'||l_errbuf||'~L_RETCODE~'||l_retcode  );
               END;

            EXCEPTION
            WHEN OTHERS THEN
               write_error('Exception: '||sqlerrm||'~L_ERRBUF~'||l_errbuf||'~L_RETCODE~'||l_retcode  );
            END;
         END IF;
         rc_tab_type_dist_rc.DELETE ;
         rc_tab.DELETE;
         rc_tab_type.DELETE;

      END LOOP;
      /*Close C_UPG_LINE */
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Log: Main Exception@~'||dbms_utility.format_error_backtrace||'~'||sqlerrm  );
   END;

   -------------------------------------------------------------------------------------------------------
   PROCEDURE rpro_atl_delink( p_rc_id IN NUMBER
                             ,p_atr17 IN VARCHAR2
                             ,p_atr41 IN VARCHAR2)
   AS
      l_errbuf            VARCHAR2(1000);
      l_retcode           NUMBER;
      l_obj_version       rpro_rc_head_g.obj_version%TYPE;
      l_book_id           rpro_rc_head_g.book_id%TYPE;
      l_id                rpro_rc_head_g.id%TYPE;
      l_ct_mod_end_date   rpro_rc_head_g.ct_mod_end_dt%TYPE;
      l_posted_flag       VARCHAR2(150);
      l_alloc_trmt        VARCHAR2(1);
      l_cnt               NUMBER;
      l_end_date          DATE;

   BEGIN
      BEGIN
         SELECT end_Date
         INTO   l_end_date
         FROM   rpro_calendar_g c
               ,rpro_period_g   p
         WHERE  1     = 1
         AND    p.id  = c.id;
      EXCEPTION
      WHEN OTHERS THEN
         l_end_date := SYSDATE;
      END;

      BEGIN
         SELECT obj_version
               ,book_id
               ,id
               ,ct_mod_end_dt
               ,rpro_rc_head_pkg.get_alloc_trtmt_flag(indicators)
               ,rpro_rc_head_pkg.get_posted_flag(indicators)
         INTO   l_obj_version
               ,l_book_id
               ,l_id
               ,l_ct_mod_end_date
               ,l_alloc_trmt
               ,l_posted_flag
         FROM   rpro_rc_head_g
         WHERE  id   =  p_rc_id ;
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Exception: While fetching rc head details~OBJ VER'||l_obj_version||'~BOOK~'||l_book_id||'~ID~'||l_id  );
      END;

      rpro_rc_collect_pkg.g_rc_line_data.DELETE;
       --DS3232018
      write_log('Debug Delink RC_ID  : ' || l_id
               || CHR(10)
               ||' l_alloc_trmt ' || l_alloc_trmt
               || CHR(10)
               ||' l_ct_mod_end_date : '||l_ct_mod_end_date
               || CHR(10)
               ||' l_end_date   :  '|| l_end_date
               || ' p_atr41     :  '|| p_atr41
               || 'p_atr17      :  '|| p_atr17);



      IF --l_ct_mod_end_date = l_end_date AND
         --UPPER(NVL(p_atr41,'OLD'))   != 'UPGRADE'  AND
         --NVL(p_atr17,'N') != 'Y'        AND
         l_alloc_trmt      = 'R'
      THEN
         write_log('Debug inside g alloc retro RC_ID  : ' || l_id );
         g_alloc_trmt  := 'R';
      ELSE
         UPDATE rpro_rc_head_g
         SET    indicators = rpro_rc_head_pkg.set_alloc_trtmt_flag(indicators,'P'),
                CT_MOD_END_DT=l_end_date --Added by Venkatesh for the ticket#104431
         WHERE  id         = p_rc_id;
         write_log('Debug inside prospective delink RC_ID  : ' || l_id || ' ~ SQL Row Count ~'|| sql%rowcount );
         COMMIT;

      END IF;

      IF l_posted_flag = 'Y'
      THEN
         BEGIN
            write_log('Log: Unfreeze Data~P_RC_ID~'||p_rc_id||'~P_OBJ_VERSION~'||l_obj_version ||'~L_BOOK_ID~'||l_book_id ||'~L_ERRBUF~'||l_errbuf  );
            rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => p_rc_id
                                                    ,p_obj_version => l_obj_version
                                                    ,p_book_id     => l_book_id
                                                    ,p_ret_msg     => l_errbuf
                                                    ,p_lock_rc     => 'N'
                                                    );
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Exception: Error while Unfreezing RC : '||p_rc_id
                       ||CHR(10)
                       ||' Error msg  : '||l_errbuf
                       ||CHR(10)
                       ||'sqlerrm : '||SQLERRM);
         END;
      END IF;

      BEGIN
         l_errbuf  := NULL;
         l_retcode := NULL;
         rpro_rc_action_pkg.delink_rc_pob ( p_doc_line_id_str => NULL
                                           ,p_rc_pob_id_str   => NULL
                                           ,p_rc_id           => p_rc_id
                                           ,p_book_id         => l_book_id
                                           ,p_object_version  => l_obj_version
                                           ,p_comments        => 'RPRO_ATL_DELINK for Atlasian custom  mod'
                                           ,p_retcode         => l_retcode
                                           ,p_errbuf          => l_errbuf
                                           ,p_lock_rc         => 'N'
                                           ,p_commit_flag     => 'Y'
                                           ,p_summary_data    => NULL
                                           );
         
         write_log('Log: While delinking: l_retcode:'||l_retcode||' :l_errbuf: '||l_errbuf);
         
         IF l_errbuf IS NULL
         THEN
            UPDATE rpro_atl_cust_cmod_det
            SET    delink   = 'Y'
                  ,comments = 'SUCCESSFULLY DELINKED'
            WHERE  rc_id    = p_rc_id;

         ELSE
            UPDATE rpro_atl_cust_cmod_det
            SET    delink   = 'N'
                  ,comments = 'ERROR WHILE DELINK...VIEW LOG FOR MORE INFO'
            WHERE  rc_id    = p_rc_id;
         END IF;
         COMMIT;
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Exception: Error while Delinking RC~'||l_errbuf||'~'||l_retcode);
      END;
   END rpro_atl_delink;
  ------------------------------------------------------------------------------------------------------------------
   PROCEDURE rpro_atl_recursive_itrtn(p_temp_inv_number VARCHAR2
                                     ,p_mod_sen_num     VARCHAR2
                                     ,p_prnt_st_dt      DATE
                                     ,p_ref_rc_id       NUMBER  --20201119
                                     )
   AS
      CURSOR cur_ref_line IS
      SELECT atr43 --orig_inv_number
            ,atr46 --mod_sen_number
            ,atr17 --comd_flag
            ,atr41 --sale_action
            ,rc_id
            ,doc_num
            ,start_date
            ,end_date
            ,doc_date
      FROM   rpro_rc_bill_g a
      WHERE  1           = 1
      AND    doc_num     = p_temp_inv_number
      AND    type        = 'INV'
      AND    a.doc_num  != NVL(a.atr43 ,'X')
      AND    rc_id       > 0
      AND    a.rc_id    != p_ref_rc_id;  --20201119

      l_level NUMBER:=3;
      l_tran_count NUMBER :=0;/*31052018*/
   BEGIN


      FOR r_rec_line IN cur_ref_line
      LOOP
         /*Start Loop*/
         IF r_rec_line.end_date < p_prnt_st_dt
         THEN
            write_log('Log: Exiting...level'
                      ||l_level
                      ||'~r_rec_line.end_date~'
                      ||r_rec_line.end_date
                      ||'~p_prnt_st_dt~'
                      ||p_prnt_st_dt
                      );
            l_temp_inv_number := NULL;
            EXIT;
         ELSE
            write_log('Log: Level'||l_level||'Line in def state');
         END IF;

         /*Deleting the exisitng records with old errors*/
         DELETE
         FROM  rpro_atl_cust_cmod_det
         WHERE 1                     = 1
         AND   invoice_num           = r_rec_line.doc_num
         AND   NVL(orig_inv_num,'X') = NVL(r_rec_line.atr43,'X');

         /*Refreshin with new errors*/
         INSERT
         INTO rpro_atl_cust_cmod_det(rc_id
                                    ,invoice_num
                                    ,orig_inv_num
                                    --,mod_sen_num
                                    ,crtd_by
                                    ,crtd_dt
                                    ,updt_by
                                    ,updt_dt)
         VALUES                     (r_rec_line.rc_id
                                    ,r_rec_line.doc_num
                                    ,r_rec_line.atr43
                                    --,r_rec_line.atr46
                                    ,rpro_utility_pkg.g_user_id
                                    ,sysdate
                                    ,rpro_utility_pkg.g_user_id
                                    ,sysdate);


         l_cnt   := l_cnt   + 1;

         write_log('Log:Inside RPRO_ATL_RECURSIVE_ITRTN~'||l_cnt);

         rc_tab(l_cnt).rc_id   := r_rec_line.rc_id;
         rc_tab(l_cnt).atr43   := r_rec_line.atr43;
         rc_tab(l_cnt).doc_num := r_rec_line.doc_num;

         write_log('Log: RC_TAB(I.RC_ID).RC_ID~'
                  ||rc_tab(l_cnt).rc_id
                  ||'~RC_TAB(I.RC_ID).ATR43~'
                  ||rc_tab(l_cnt).atr43
                  ||'~RC_TAB(I.RC_ID).DOC_NUM~'
                  ||rc_tab(l_cnt).doc_num
                  ||'~L_CNT~'
                  ||l_cnt
                  );

         IF r_rec_line.atr43 IS NOT NULL
         THEN
            
            l_level  := l_level+1;
            write_log('Log: L_LEVEL '||l_level);
            
            l_temp_inv_number := r_rec_line.atr43;
            l_prnt_st_dt      := r_rec_line.start_date;
            
            rpro_atl_recursive_itrtn(p_temp_inv_number => l_temp_inv_number
                                    ,p_mod_sen_num     => l_mod_sen_number
                                    ,p_prnt_st_dt      => l_prnt_st_dt
                                    ,p_ref_rc_id       => r_rec_line.rc_id  --20201119
                                    );
            
         ELSE
            l_temp_inv_number := NULL;
                  write_log('Log: Orig_inv_num(atr43) is null/ CMOD flag is N ');
         END IF;

      END LOOP;
   /*End Loop*/
     ---If cursor is not having the data this if condition will make l_temp_inv_number as null
     IF l_tran_count = 0
     THEN
        l_temp_inv_number := NULL;
     END IF;
   END rpro_atl_recursive_itrtn;
---------------------------------------------------------------------------------------------------------------------------------
   PROCEDURE rpro_after_bndl_explode (p_rc_id      IN VARCHAR2 DEFAULT NULL
                                     ,p_batch_id   IN NUMBER)
   AS
      /****************************************************************************************************************************
          1) BUNDLE Child License Rev Account segment and End Date using value in ATR28
       ***************************************************************************************************************************/
      -- LOCAL VARIABLES
      l_proc              VARCHAR2 (30) := 'RPRO_AFTER_BNDL_EXPLODE';
      l_seperator         VARCHAR2 (1) := '-';

      CURSOR l_segment_cur IS
         SELECT   /*+ PARALLEL(rlshg,4) */
                  id,
                  (regexp_substr (rlshg.rev_segments, '[^' || ':' || ']+', 1,1)
                  || ':'
                  || regexp_substr (rlshg.rev_segments, '[^' || ':' || ']+', 1, 2)
                  || ':'
                  || regexp_substr (rlshg.rev_segments, '[^' || ':' || ']+', 1, 3)
                  || ':'
                  || rlshg.atr28)
                  rev_segments
         FROM     rpro_line_stg_g rlshg
         WHERE    batch_id                     =  p_batch_id
         AND      NVL (rlshg.bndl_cnfg_id, 0) <> 0
         AND      NVL (rlshg.rc_id, 0)        <> 0
         AND      rlshg.atr28 IS NOT NULL;

      TYPE l_segment_cur_tb IS TABLE OF l_segment_cur%ROWTYPE
      INDEX BY pls_integer;

      l_segment_update    l_segment_cur_tb;


      CURSOR l_end_date_cursor_id IS
         (SELECT id
                ,ext_sll_prc
                ,bndl_prnt_id
          FROM   rpro_line_stg_g
          WHERE  1 = 2);
      /* Start Cursor added by COE team*/
      CURSOR c_rsd_upd IS
      SELECT   inv_num
              ,atr46
              ,type
              ,GREATEST( MIN(inv_date) , MIN(start_date)) min_date --MIN(inv_date) min_date     -- kgm 5/23
      FROM     rpro_line_stg b
      WHERE    type       =  'INV'
      AND      atr41      IN ('NEW','UPGRADE','DOWNGRADE','RENEWAL')
      AND      item_num   IN ('Term License', 'Term License (incremental)','Stack License','Stack License (incremental)')
      GROUP BY inv_num
              ,atr46
              ,type
      UNION ALL
      SELECT   inv_num
              ,atr46
              ,type
              ,GREATEST( MIN(so_date),MIN(start_date)) min_date --MIN(so_date) min_date   -- kgm 5/23
      FROM     rpro_line_stg b
      WHERE    type       =  'SO'
      AND      atr41      IN ('NEW','UPGRADE','DOWNGRADE','RENEWAL')
      AND      item_num   IN ('Term License', 'Term License (incremental)','Stack License','Stack License (incremental)')
      GROUP BY inv_num
              ,atr46
              ,type;
      /*End Cursor added by COE team*/
      TYPE l_id_stage IS TABLE OF l_end_date_cursor_id%ROWTYPE
      INDEX BY pls_integer;
      TYPE tab_rsd_upd IS TABLE OF c_rsd_upd%ROWTYPE
      INDEX BY pls_integer;
      l_id_stage_v        l_id_stage;
      t_rsd_upd           tab_rsd_upd;
   BEGIN
      rpro_utility_pkg.set_revpro_context;

      /* Start Update Added by COE Team */
      ----------------------------------------------------------------------------------------------------------------------
      UPDATE rpro_line_stg
      SET    start_date =  inv_date
      WHERE  atr41      =  'RENEWAL'
      AND    item_num   IN ('Term License', 'Term License (incremental)', 'Stack License', 'Stack License (incremental)')
      AND    type       =  'INV'
      AND    inv_date   >  start_date;

      write_log('Log: Renewal update for INV lines~'||SQL%ROWCOUNT);

      UPDATE rpro_line_stg
      SET    start_date =  so_date
      WHERE  atr41      =  'RENEWAL'
      AND    item_num   IN ('Term License', 'Term License (incremental)', 'Stack License', 'Stack License (incremental)')
      AND    type       =  'SO'
      AND    so_date    >  start_date;

      write_log('Log: Renewal update for SO lines~'||SQL%ROWCOUNT);

      ---------------------------------
      OPEN  c_rsd_upd;
      FETCH c_rsd_upd BULK COLLECT INTO t_rsd_upd;
      CLOSE c_rsd_upd;

      FORALL rec IN t_rsd_upd.FIRST..t_rsd_upd.LAST
         UPDATE rpro_line_stg
         SET    start_date = t_rsd_upd(rec).min_date
         WHERE  type       = t_rsd_upd(rec).type
         AND    inv_num    = t_rsd_upd(rec).inv_num
         AND    atr46      = t_rsd_upd(rec).atr46
         AND    atr41      IN ('NEW','UPGRADE','DOWNGRADE','RENEWAL')
         AND    item_num   IN ('Term License', 'Term License (incremental)','Stack License','Stack License (incremental)');


      write_log('Log: No of rows updated~'||SQL%ROWCOUNT);

      COMMIT;
      ---------------------------------------------------------------------------------------------------------------------
      /* End Update Added by COE Team */


      BEGIN
         SELECT    NVL (MAX (rpv.VALUE), l_seperator)
         INTO      l_seperator
         FROM      rpro_profile rp
                  ,rpro_profile_value rpv
         WHERE     rp.id               = rpv.prof_id
         AND       UPPER (rp.category) = 'APPLICATION'
         AND       UPPER (rp.name)     = 'SEGMENT_DELIMITER';
      EXCEPTION WHEN OTHERS
      THEN
         l_seperator := ':';
      END;

      BEGIN
         l_segment_update.DELETE;

         OPEN l_segment_cur;

         LOOP
            FETCH l_segment_cur
            BULK COLLECT
            INTO  l_segment_update
            LIMIT 5000;

            EXIT WHEN l_segment_update.count = 0;

            FORALL z IN l_segment_update.FIRST .. l_segment_update.LAST
               UPDATE rpro_line_stg_g      rlshg
               SET    rlshg.rev_segments = l_segment_update (z).rev_segments
               WHERE  rlshg.id           = l_segment_update (z).id
               AND    rlshg.batch_id     = p_batch_id;

               COMMIT;
         END LOOP;

         CLOSE l_segment_cur;
      END;

      BEGIN

         l_id_stage_v.DELETE;

         SELECT /*+ PARALLEL(rrl1,4) */
                rrl1.id
               ,rrl1.ext_sll_prc
               ,rrl1.bndl_prnt_id
         BULK   COLLECT
         INTO   l_id_stage_v
         FROM   rpro_line_stg_g rrl1
         WHERE  1               =     1
         AND    rrl1.item_num   LIKE  '%License%'
         AND    rrl1.so_line_id <>    rrl1.bndl_prnt_id
         AND    rrl1.end_date   IS NOT NULL
         AND    rrl1.batch_id   =     p_batch_id
         AND    rrl1.so_line_id IS NOT NULL
         AND    type IN ('SO', 'RORD');

         FORALL z IN l_id_stage_v.FIRST .. l_id_stage_v.LAST
            UPDATE rpro_line_stg_g rrl
            SET    end_date = NULL
            WHERE  rrl.id   = l_id_stage_v (z).id
            AND    batch_id = p_batch_id;

         COMMIT;

      END;

      BEGIN
         l_id_stage_v.DELETE;

         SELECT rrl1.id
               ,rrl1.ext_sll_prc
               ,rrl1.bndl_prnt_id
         BULK COLLECT
         INTO   l_id_stage_v
         FROM   rpro_line_stg_g rrl1
         WHERE  1                = 1
         AND    rrl1.item_num    LIKE '%Lic%'
         AND    rrl1.inv_line_id <> rrl1.bndl_prnt_id
         AND    batch_id         = p_batch_id
         AND    rrl1.inv_line_id IS NOT NULL
         AND    TYPE NOT IN ('SO', 'RORD');

         FORALL z IN l_id_stage_v.FIRST .. l_id_stage_v.LAST
            UPDATE rpro_line_stg_g rrl
            SET    end_date    = NULL
                  ,credit_rule = NULL
            WHERE  rrl.id      = l_id_stage_v (z).id
            AND    batch_id    = p_batch_id;

         COMMIT;
      END;
   EXCEPTION WHEN OTHERS
   THEN
      write_error('Unexpected Error -'||sqlerrm|| '~ Error Queue : '||dbms_utility.format_error_backtrace);
   END rpro_after_bndl_explode;
-----------------------------------------------------------------------------------------------------------------------------------------------
   PROCEDURE  before_validate (p_rc_id    NUMBER DEFAULT NULL
                              ,p_batch_id NUMBER
                              )
   IS

      CURSOR  rsd_cur IS
      SELECT /*+ PARALLEL(rls ,4)  */
              rls.id
             ,rls.inv_line_id
             ,inv_date
             ,trunc(rls.start_date) start_date
             ,rls.item_num
             ,GREATEST(rls.start_date, rls.inv_date) upd_dt
      FROM    rpro_line_stg_g rls
      WHERE   1 = 1
      AND     NVL (rls.processed_flag, 'N') !=  'Y'
      AND     rls.type                       =  'INV'
      AND     rls.atr41                      IN ('NEW', 'UPGRADE', 'DOWNGRADE')
      AND     EXISTS (SELECT 1
                      FROM   rpro_lkp_g     lkp
                           , rpro_lkp_val_g val
                      WHERE  lkp.category     = 'CUSTOM'
                      AND    lkp.id           = val.lookup_id
                      AND    val.lookup_value = rls.item_num);

      TYPE tb_rsd IS TABLE OF rsd_cur%rowtype
      INDEX BY pls_integer;

      l_tbl_rsd              tb_rsd;
      l_st_dt                DATE;
      l_end_dt               DATE;
      l_post_st_dt           DATE;
      l_prd_id               NUMBER;
      l_so_line_id           rpro_line_stg_g.so_line_id%TYPE;
      l_pob_identifier       rpro_line_stg_g.atr2%TYPE;

      l_bulk_collect_limit   NUMBER := 1000;
      dml_errors             EXCEPTION;
      pragma exception_init (dml_errors, -24381);

   BEGIN

      write_log( ' Begin before_validate_prc : '||to_char(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
      /*Update to remove the empty spaces after data integrated to stage*/
      BEGIN
         UPDATE rpro_line_stg_g
         SET  atr43   = TRIM(atr43)
             ,atr46   = TRIM(atr46)
             ,so_num  = TRIM(so_num)
             ,inv_num = TRIM(inv_num);
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Exception: While updating to trim the data');
      END;

      BEGIN
         UPDATE rpro_line_stg_g
         SET    atr41                      = UPPER (atr41)
               ,credit_rule                = CASE
                                             WHEN UPPER(atr33) = 'RECAST' AND type IN ('CM','CM-R')
                                             THEN 'F'
                                             WHEN UPPER(NVL(atr33,'X')) != 'RECAST' AND type IN ('CM','CM-R')
                                             THEN 'P'
                                             ELSE  NULL
                                             END
         WHERE  1                          = 1
         AND    NVL (processed_flag, 'N') !=  'Y';
      EXCEPTION WHEN OTHERS
      THEN
         write_error('Exception: While Converting Sales Action ATR41 to UPPER- '||sqlerrm);
      END;

     -- Derive Revenue Start Date based on rules  the greatest of INV Date or RSD
      OPEN rsd_cur;
      LOOP

         fetch rsd_cur
         BULK  COLLECT
         INTO  l_tbl_rsd
         LIMIT l_bulk_collect_limit;

            EXIT WHEN l_tbl_rsd.COUNT = 0;

            FOR i IN 1.. l_tbl_rsd.COUNT
            LOOP

               BEGIN
                  --Updating HW/SW Maint and Post Maint line with POB link identifier
                  UPDATE rpro_line_stg_g
                  SET    start_date  = l_tbl_rsd (i).upd_dt
                  WHERE  1           = 1
                  AND    inv_line_id = l_tbl_rsd(i).inv_line_id;

               EXCEPTION WHEN OTHERS
               THEN
                  write_error('Exception: While updating RSD- '||sqlerrm);
               END;

            END LOOP;

      END LOOP;

      l_tbl_rsd.DELETE;
      CLOSE rsd_cur;

      COMMIT;
      write_log( 'End before_validate_prc : '||to_char(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception: before_validate_prc. Error '||sqlerrm|| '~ Error Queue : '||dbms_utility.format_error_backtrace);
   END before_validate;

--------------------------------------------------------------------------------------------------------------------------

   PROCEDURE after_post(p_rc_id     IN   NUMBER
                       ,p_batch_id  IN   NUMBER)
   IS
      lc_split_lines_lkp    CONSTANT    NUMBER   := 100;
      CURSOR c_group_id_cur (p_batch_id IN NUMBER ) IS
      SELECT   batch_id
              ,reference24 rc_id
              ,COUNT(*) cnt
              ,0 group_id
      FROM     rpro_gl_int_stage
      WHERE    batch_id    = p_batch_id
      GROUP    BY reference24
              ,batch_id
      ORDER    BY 3;

      TYPE l_group_id_tab IS
      TABLE OF c_group_id_cur%ROWTYPE;
      l_group_id              l_group_id_tab;
      l_grp_cnt               NUMBER   := 0;
      l_group_id_s            NUMBER   := 1;
      l_updated_lines         NUMBER   := 0;
   BEGIN
      g_rc_id  := p_rc_id;
      write_log( ' Begin after_post : '||TO_CHAR(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
      OPEN  c_group_id_cur(p_batch_id);
      FETCH c_group_id_cur
      BULK COLLECT
      INTO  l_group_id;
      CLOSE c_group_id_cur;

      FOR i IN 1..l_group_id.COUNT
      LOOP
         l_grp_cnt := l_grp_cnt+l_group_id(i).cnt;
         IF l_grp_cnt    > lc_split_lines_lkp
         THEN
            l_grp_cnt    := l_group_id(i).cnt;
            l_group_id_s := l_group_id_s+1;
         END IF;
         l_group_id(i).group_id := l_group_id_s;
      END LOOP;

      FORALL i IN 1..l_group_id.COUNT

      UPDATE rpro_gl_int_stage
      SET    attribute19 = l_group_id(i).group_id
      WHERE  attribute19 IS NULL
      AND    batch_id    = l_group_id(i).batch_id
      AND    reference24 = l_group_id(i).rc_id;
      COMMIT;

      write_log( ' End after_post : '||TO_CHAR(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception: after_post. Error '||SQLERRM
                 ||CHR(10)
                 ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END after_post;

   PROCEDURE after_fv_analysis(p_rc_id    IN NUMBER 
                              ,p_batch_id IN NUMBER)
   AS
      r_idx  NUMBER;
      r_idx1 VARCHAR2(1000);
   BEGIN
     write_log( ' Begin after_fv_analysis : '||TO_CHAR(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
     r_idx := rpro_rc_collect_pkg.g_rc_line_data.FIRST;
     WHILE r_idx IS NOT NULL
     LOOP
        write_log('Log: rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41:'||rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41
                 ||CHR(10)
                 ||'rpro_rc_collect_pkg.g_rc_line_data(r_idx).batch_id:'||rpro_rc_collect_pkg.g_rc_line_data(r_idx).batch_id
                 ||CHR(10)
                 ||'rpro_utility_pkg.g_batch_id:'||rpro_utility_pkg.g_batch_id);
        IF  rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41     = 'UPGRADE' 
        AND rpro_rc_collect_pkg.g_rc_line_data(r_idx).batch_id  = rpro_utility_pkg.g_batch_id 
        THEN
           write_log('Log: Within_range_flag:'||rpro_rc_line_pkg.get_within_fv_range_flag(rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators));
           
           IF rpro_rc_line_pkg.get_within_fv_range_flag(rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators) = 'Y'
           THEN
              write_log('Log: with fv Y');
              tab_so_num_idx(rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num).within_fv_flag_y := 1;
              tab_so_num_idx(rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num).so_num          := rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num;
              tab_so_num_idx(rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num).ct_num          := rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr16;
           ELSIF rpro_rc_line_pkg.get_within_fv_range_flag(rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators) = 'N'
           THEN
              write_log('Log: with fv N');
              tab_so_num_idx(rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num).within_fv_flag_n := 1;
              tab_so_num_idx(rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num).so_num          := rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num;
              tab_so_num_idx(rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num).ct_num          := rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr16;
           END IF;
        END IF;
        r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);
     END LOOP;
     
     r_idx1 := tab_so_num_idx.FIRST;
     write_log('Count : '||tab_so_num_idx.COUNT);
     WHILE r_idx1 IS NOT NULL
     LOOP
           write_log('Values of tab_so_num_idx : '|| r_idx1
                    ||CHR(10)
                    ||' WITHIN_Y : '|| tab_so_num_idx(r_idx1).within_fv_flag_y
                    ||CHR(10)
                    ||' WITHIN_N : '|| tab_so_num_idx(r_idx1).within_fv_flag_n
                    ||CHR(10)
                    ||' so_num : '|| tab_so_num_idx(r_idx1).so_num
                    ||CHR(10)
                    ||' ct_num : '|| tab_so_num_idx(r_idx1).ct_num);
           IF  tab_so_num_idx(r_idx1).within_fv_flag_y > 0 
           AND tab_so_num_idx(r_idx1).within_fv_flag_n > 0
           THEN
              r_idx := rpro_rc_collect_pkg.g_rc_line_data.FIRST;
              WHILE r_idx IS NOT NULL
              LOOP
                 IF  tab_so_num_idx(r_idx1).so_num = rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_num
                 AND tab_so_num_idx(r_idx1).ct_num = rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr16
                 THEN
                    --iterate line data and skip ct mod;
                    write_log('Updating SKIP_CT_MOD_FLAG');
                    rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators := rpro_rc_line_pkg.set_skip_ct_mod_flag(rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators,'Y');
                 END IF;
              r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);
              END LOOP;
           END IF;
        r_idx1 := tab_so_num_idx.NEXT(r_idx1);
     END LOOP;
     write_log( ' End after_fv_analysis : '||TO_CHAR(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
     write_error('Exception: after_fv_analysis. Error '||SQLERRM ||CHR(10) ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END after_fv_analysis;
   
   PROCEDURE after_validate(p_rc_id    IN NUMBER 
                           ,p_batch_id IN NUMBER)
   AS
      CURSOR c_grp(p_cur_end_dt IN DATE)
      IS
      SELECT rls.rc_id
      FROM   rpro_line_stg_g rls
            ,rpro_rc_head_g    rrh
      WHERE  1=1
      AND    rls.batch_id                  = p_batch_id
      AND    rls.rc_id                     = rrh.id
      AND    rrh.init_pob_exp_dt           <= p_cur_end_dt  --condition to check revision rc's
    --AND    NVL(rls.cv_eligible_flag,'N') = 'Y'
      AND    EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  NVL(rrl.atr16,'X') = NVL(rls.atr16,'X'))
      GROUP BY rls.rc_id;
      TYPE tab_grp IS TABLE OF c_grp%ROWTYPE
      INDEX BY PLS_INTEGER;

      TYPE tab_grp_rec IS TABLE OF rpro_rc_grp_dtl_g%rowtype 
      INDEX BY PLS_INTEGER;

      t_grp_dtl_g   tab_grp_rec;   
      t_grp         tab_grp;
      l_rc_id       NUMBER ;
      l_prd_id      NUMBER;
      l_cur_end_dt  DATE;
      l_min_rc_id   NUMBER;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log( ' Begin after_validate : '||TO_CHAR(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
      l_cur_end_dt := rpro_utility_pkg.get_cur_end_dt( p_book_id      => rpro_utility_pkg.g_book_id
                                                     , p_sec_atr_val  => rpro_utility_pkg.g_sec_atr_val
                                                     , p_client_id    => rpro_utility_pkg.g_client_id );
      /*SELECT min(rc.rc_id)
      INTO   l_min_rc_id
      FROM   rpro_line_stg_g rls
            ,rpro_rc_head_g    rrh
            ,rpro_rc_line_g  rc
      WHERE  1=1
      AND    rls.batch_id                  = p_batch_id
      AND    rls.rc_id                     = rrh.id
      AND    (NVL(rc.atr16,'X')             = NVL(rls.atr16,'X') OR rc.doc_num = rls.so_num)
      AND    rrh.init_pob_exp_dt          <= l_cur_end_dt
      AND    rc.rc_id                      > 0;*/
      write_log('Log: l_cur_end_dt : '    ||l_cur_end_dt||' p_batch_id : '||p_batch_id ||'~p_rc_id~'||p_rc_id||' min_rc_id : '||l_min_rc_id);
      BEGIN
         SELECT id
         INTO   l_prd_id
         FROM   rpro_period_g
         WHERE  status = 'OPEN';
      EXCEPTION 
      WHEN OTHERS THEN
         write_error('Error: Exiting from the code as no open period found :');
      END;
      OPEN c_grp(l_cur_end_dt);
      LOOP
         FETCH c_grp BULK COLLECT INTO t_grp
         LIMIT g_limit;
         write_log('Log: t_grp.COUNT: '||t_grp.COUNT);
         EXIT WHEN t_grp.COUNT = 0;
         FOR rec IN t_grp.FIRST..t_grp.LAST
         LOOP       
            l_rc_id := rpro_utility_pkg.generate_id('RPRO_RC_HEAD_ID_S',rpro_utility_pkg.g_client_id);
            UPDATE rpro_line_stg_g stg
            SET    num15      = rc_id
            WHERE  rc_id      = t_grp(rec).rc_id
            AND    type            = 'SO'
            AND    NOT EXISTS(SELECT 1
                              FROM   rpro_rc_line_g ln
                              WHERE  ln.doc_line_id   = stg.so_line_id);
         END LOOP;
         COMMIT;
      END LOOP;
      CLOSE c_grp;
      t_grp.DELETE;
      write_log( ' End after_validate : '||TO_CHAR(SYSDATE, 'DD-Mon-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
     write_error('Exception: after_validate. Error '||SQLERRM 
                ||CHR(10) 
                ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END after_validate;
   
   PROCEDURE upd_ct_mod_flag ( p_rc_id    IN NUMBER DEFAULT NULL
                             , p_batch_id IN NUMBER DEFAULT NULL)
   IS
      CURSOR c_upd_ct_mod_flg
      IS
      SELECT so_num
      FROM   rpro_line_stg_g stg
      WHERE  batch_id                                                   = p_batch_id
      AND    (stg.atr41                                                IN ('UPGRADE','DOWNGRADE') -- Sales Action
              OR (stg.atr41                                             = 'NEW'
                  AND    EXISTS (SELECT 1 
                                 FROM   rpro_rc_line_g rrl 
                                 WHERE  NVL(rrl.atr16,'X')              = NVL(stg.atr16,'X') -- Contract number
                                )   
                 )
             );
      TYPE tab_upd_ct_mod_flg IS TABLE OF c_upd_ct_mod_flg%ROWTYPE
      INDEX BY PLS_INTEGER;
      t_upd_ct_mod_flg     tab_upd_ct_mod_flg;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('Log: Start upd_ct_mod_flag : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      OPEN c_upd_ct_mod_flg;
      FETCH c_upd_ct_mod_flg BULK COLLECT INTO t_upd_ct_mod_flg;
      CLOSE c_upd_ct_mod_flg;
      FORALL upd IN t_upd_ct_mod_flg.FIRST..t_upd_ct_mod_flg.LAST
         UPDATE rpro_line_stg_g
         SET    atr17 = 'Y'
         WHERE  so_num         = t_upd_ct_mod_flg(upd).so_num
         AND    NVL(atr17,'N')  = 'N';
         write_log('Update row count : '||sql%rowcount);
      COMMIT;
      write_log('Log: End upd_ct_mod_flag : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception upd_ct_mod_flag : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END upd_ct_mod_flag;
   
 PROCEDURE cust_link_delink(p_src_rc_id  IN NUMBER
                           ,p_dest_rc_id IN NUMBER
                           ,p_batch_id   IN NUMBER
                           ,p_line_id    IN NUMBER
                           ,p_rc_pob_id  IN NUMBER)
   IS
      l_retcode          NUMBER;
      l_errbuf           VARCHAR2(1000);
      l_book_id          NUMBER;
      l_obj_version      NUMBER;
      l_posted_flag      VARCHAR(1);
      l_delink_status    VARCHAR(1);
      l_alloc_trtmt_flag VARCHAR(1);
      l_cmd_cnt          NUMBER;
      l_prd_id           NUMBER;
      l_rule_id          NUMBER;
      l_mod_durn_type    VARCHAR2(300);
      l_ct_mod_date      DATE;
      l_ret_code         NUMBER;
      l_err_buf          VARCHAR2(4000);
      TYPE rec_rc_ct_mod_det_g IS RECORD (id              NUMBER
                                         ,rc_id           NUMBER
                                         ,line_id         NUMBER
                                         ,rule_id         NUMBER
                                         ,crtd_prd_id     NUMBER
                                         ,indicators      VARCHAR2(100)
                                         ,client_id       NUMBER
                                         ,sec_atr_val     VARCHAR2(10)
                                         ,crtd_by         VARCHAR2(100)
                                         ,crtd_dt         DATE
                                         ,updt_by         VARCHAR2(100)
                                         ,updt_dt         DATE
                                         ,ct_mod_end_date DATE
                                         ,ssp_trmt        VARCHAR2(10)
                                         ,batch_id        NUMBER);

      t_rc_ct_mod_det_g   rec_rc_ct_mod_det_g;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      /*Restting the values*/
      l_posted_flag   := 'N';
      l_delink_status := 'N';

      write_log('Log: p_src_rc_id : '||p_src_rc_id
               ||CHR(10)
               ||'p_dest_rc_id : '   ||p_dest_rc_id);
      BEGIN
         SELECT id
         INTO   l_prd_id
         FROM   rpro_period_g
         WHERE  status = 'OPEN';
      EXCEPTION
      WHEN OTHERS THEN
         l_prd_id:= NULL;
      END;

      BEGIN   
         SELECT rule_trmt              
               ,id
         INTO   l_alloc_trtmt_flag
               ,l_rule_id
         FROM   rpro_ct_md_evt_g 
         WHERE  UPPER(name) = 'NEW POB NOT WITHIN SSP RANGE';
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Error: rpro_ct_md_evt_g :'||l_errbuf||'~'||SQLERRM);
      END;

      BEGIN   
         SELECT UPPER(mod_durn_type) 
         INTO   l_mod_durn_type
         FROM   rpro_ct_md_dt_g 
         WHERE  rpro_ct_md_dt_pkg.get_hist_flag(indicators)='N';
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Error: rpro_ct_md_dt_g :'||l_errbuf||'~'||SQLERRM);
      END;

      BEGIN   
         SELECT book_id
               ,obj_version
               ,rpro_rc_head_pkg.get_posted_flag(indicators)
         INTO   l_book_id
               ,l_obj_version
               ,l_posted_flag
         FROM   rpro_rc_head_g
         WHERE  id = p_src_rc_id;
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : '||SQLERRM);
      END;

      IF l_posted_flag  = 'Y' 
      THEN
         rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => p_src_rc_id
                                                 ,p_obj_version => l_obj_version 
                                                 ,p_book_id     => l_book_id 
                                                 ,p_ret_msg     => l_errbuf 
                                                 ,p_lock_rc     => 'N');
      END IF;

      IF l_errbuf IS NULL  --If unfreeze happened successfully, delinking the rc
      THEN

         BEGIN   
            SELECT book_id
                  ,obj_version
            INTO   l_book_id
                  ,l_obj_version
            FROM   rpro_rc_head_g
            WHERE  id = p_src_rc_id;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : l_errbuf :'||l_errbuf||'~'||SQLERRM);
         END;

         --delink api
         rpro_rc_action_pkg.delink_rc_pob ( p_doc_line_id_str => NULL
                                          , p_rc_pob_id_str   => to_char(p_rc_pob_id)
                                          , p_rc_id           => p_src_rc_id
                                          , p_book_id         => l_book_id 
                                          , p_object_version  => l_obj_version 
                                          , p_comments        => 'Atlassian Regrouping delink '||SYSDATE 
                                          , p_retcode         => l_retcode 
                                          , p_errbuf          => l_errbuf 
                                          , p_lock_rc         => 'N' 
                                          , p_commit_flag     => 'Y' 
                                          , p_summary_data    => NULL );
         IF NVL(l_retcode,0) = 0 
         THEN
            l_delink_status := 'Y';
         END IF;
      ELSE
         write_error('Error: Unable to unfreeze the rc..So delink cannot be done: l_errbuf :'||l_errbuf);
      END IF;
      l_posted_flag := 'N';
      IF l_delink_status = 'Y' --Linking only if delink happens successfully
      THEN
         IF(l_alloc_trtmt_flag IS NOT NULL)              
         THEN
            l_ct_mod_date                     := CASE WHEN l_mod_durn_type='RC MODIFIED PERIOD'
                                                 THEN rpro_utility_pkg.get_prd_end_dt(l_prd_id,rpro_utility_pkg.g_client_id)
                                                 ELSE rpro_utility_pkg.get_qtr_end_dt(rpro_utility_pkg.g_book_id,rpro_utility_pkg.g_sec_atr_val)
                                                 END;
            t_rc_ct_mod_det_g.id              := rpro_utility_pkg.generate_id('RPRO_RC_CT_MOD_DET_ID_S',rpro_utility_pkg.g_client_id);
            t_rc_ct_mod_det_g.rc_id           := p_dest_rc_id;
            t_rc_ct_mod_det_g.line_id         := p_line_id;
            t_rc_ct_mod_det_g.rule_id         := l_rule_id;
            t_rc_ct_mod_det_g.crtd_prd_id     := l_prd_id;
            t_rc_ct_mod_det_g.indicators      := rpro_rc_ct_mod_det_pkg.g_set_default_ind;
            t_rc_ct_mod_det_g.client_id       := rpro_utility_pkg.g_client_id;
            t_rc_ct_mod_det_g.sec_atr_val     := rpro_utility_pkg.g_sec_atr_val;
            t_rc_ct_mod_det_g.crtd_by         := rpro_utility_pkg.g_user;
            t_rc_ct_mod_det_g.crtd_dt         := sysdate;
            t_rc_ct_mod_det_g.updt_by         := rpro_utility_pkg.g_user;
            t_rc_ct_mod_det_g.updt_dt         := sysdate;
            t_rc_ct_mod_det_g.ct_mod_end_date := l_ct_mod_date;
            t_rc_ct_mod_det_g.ssp_trmt        := l_alloc_trtmt_flag;
            t_rc_ct_mod_det_g.batch_id        := p_batch_id;
         END IF;
         BEGIN  
           INSERT 
           INTO     rpro_rc_ct_mod_det_g
           VALUES   t_rc_ct_mod_det_g;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Exception INSERT INrpro_rc_ct_mod_det_g : '
                       ||'error : '||SQLERRM
                       ||CHR(10)
                       ||'error trace : '||dbms_utility.format_error_backtrace);
         END;

         IF(t_rc_ct_mod_det_g.id IS NOT NULL)          
         THEN
            BEGIN
               UPDATE rpro_rc_head_g
               SET    indicators    = rpro_rc_head_pkg.set_alloc_trtmt_flag(indicators,l_alloc_trtmt_flag)
                     ,ct_mod_end_dt = l_ct_mod_date
               WHERE  id            = p_dest_rc_id;

               UPDATE rpro_rc_head_g
               SET    indicators  =  rpro_rc_head_pkg.set_posted_flag(indicators,'Y')
               WHERE  id          =  p_dest_rc_id;
            EXCEPTION WHEN OTHERS
            THEN
               write_error('Exception UPDATE header table date ct mod : '
                           ||'error : '||SQLERRM
                           ||CHR(10)
                           ||'error trace : '||dbms_utility.format_error_backtrace);
            END;
         END IF;

         BEGIN
            SELECT book_id
                  ,obj_version
                  ,rpro_rc_head_pkg.get_posted_flag(indicators)
            INTO   l_book_id
                  ,l_obj_version
                  ,l_posted_flag
            FROM   rpro_rc_head_g
            WHERE  id = p_dest_rc_id;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Error: Unable to fetch p_dest_rc_id book_id/obj version : '||SQLERRM);
         END;

         --l_ct_mod_date := NULL;

         IF l_posted_flag  = 'Y' 
         THEN
            --By default unfreeze will do RETAIN ALLOCATION
            rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => p_dest_rc_id
                                                    ,p_obj_version => l_obj_version 
                                                    ,p_book_id     => l_book_id 
                                                    ,p_ret_msg     => l_errbuf 
                                                    ,p_lock_rc     => 'N');

            write_log('Log: unfreeze rc since it is already posted~rc_id : '||p_dest_rc_id
                      ||CHR(10)
                      ||' l_errbuf : '||l_errbuf);
         END IF;
         BEGIN
            SELECT book_id
                  ,obj_version
                  ,rpro_rc_head_pkg.get_posted_flag(indicators)
            INTO   l_book_id
                  ,l_obj_version
                  ,l_posted_flag
            FROM   rpro_rc_head_g
            WHERE  id = p_dest_rc_id;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Error: Unable to fetch p_dest_rc_id book_id/obj version : '||SQLERRM);
         END;
         --RC level link api
         /*rpro_rc_action_pkg.link_rc_level (p_rc_id_str         => (p_src_rc_id) * -1
                                          ,p_linked_to_rc_id   => p_dest_rc_id 
                                          ,p_book_id           => l_book_id
                                          ,p_object_version    => l_obj_version 
                                          ,p_comments          => 'Atlassian Regrouping link '||SYSDATE 
                                          ,p_retcode           => l_retcode
                                          ,p_errbuf            => l_errbuf
                                          );*/
                                          
         rpro_rc_action_pkg.link_rc_pob_line( p_doc_line_id_str   => NULL
                                            , p_rc_id             => p_dest_rc_id
                                            , p_book_id           => l_book_id
                                            , p_object_version    => l_obj_version
                                            , p_rc_pob_id_str     => TO_CHAR(p_rc_pob_id)
                                            , p_comments          => 'Atlassian Regrouping link '||SYSDATE
                                            , p_retcode           => l_retcode
                                            , p_errbuf            => l_errbuf
                                            , p_lock_rc           => 'N'
                                            );
         write_log('After Link RC Level API for RC ID '
                   ||' p_dest_rc_id  : '||p_dest_rc_id
                   ||CHR(10)
                   ||'l_retcode    : '||l_retcode
                   ||CHR(10)
                   ||'l_errbuf     : '||l_errbuf);
         IF l_retcode = 0 
         THEN 
           
            /*Updating the rc close flag in grp dtl and head */
            UPDATE rpro_rc_grp_dtl_g
            SET    rc_close_flag = 'Y'
            WHERE  rc_id         = p_src_rc_id;

            write_log('Log: Existing rc id : '   ||p_dest_rc_id
                     ||CHR(10)                   
                     ||' Current rc_id: '        ||p_src_rc_id);

            UPDATE rpro_rc_head_g
            SET    indicators = rpro_rc_head_pkg.set_rc_closed_flag(indicators,'Y')
            WHERE  id = p_src_rc_id ;
            write_log('Closed the old RC '
                      ||' p_dest_rc_id  : '||p_dest_rc_id
                      ||CHR(10)
                      ||'l_ret_code    : '||l_ret_code);
            COMMIT;
         END IF; 
      END IF;
      l_posted_flag := 'N';
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception link_delink_bg_job : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END cust_link_delink;
   
   --Handle link/Delink for cv eligible flag is N  #Haribabu P 20201104
   PROCEDURE cust_link_delink_cv(p_src_rc_id  IN NUMBER
                                ,p_dest_rc_id IN NUMBER
                                ,p_batch_id   IN NUMBER
                                ,p_line_id    IN NUMBER
                                ,p_rc_pob_id  IN NUMBER)
   IS
      l_retcode                 NUMBER;
      l_errbuf                  VARCHAR2(1000);
      l_book_id                 NUMBER;
      l_obj_version             NUMBER;
      l_posted_flag             VARCHAR(1);
      l_delink_status           VARCHAR(1);
      l_alloc_trtmt_flag        VARCHAR(1);
      l_cmd_cnt                 NUMBER;
      l_prd_id                  NUMBER;
      l_rule_id                 NUMBER;
      l_mod_durn_type           VARCHAR2(300);
      l_ct_mod_date             DATE;
      l_ret_code                NUMBER;
      l_err_buf                 VARCHAR2(4000);
      l_alloc_trtmnt_flag       VARCHAR2(10) := 'R';
      l_alloc_prv_trtmnt_flag   VARCHAR2(10) := 'R';
      l_cv_flag                 VARCHAR2(10);
      TYPE rec_rc_ct_mod_det_g IS RECORD (id              NUMBER
                                         ,rc_id           NUMBER
                                         ,line_id         NUMBER
                                         ,rule_id         NUMBER
                                         ,crtd_prd_id     NUMBER
                                         ,indicators      VARCHAR2(100)
                                         ,client_id       NUMBER
                                         ,sec_atr_val     VARCHAR2(10)
                                         ,crtd_by         VARCHAR2(100)
                                         ,crtd_dt         DATE
                                         ,updt_by         VARCHAR2(100)
                                         ,updt_dt         DATE
                                         ,ct_mod_end_date DATE
                                         ,ssp_trmt        VARCHAR2(10)
                                         ,batch_id        NUMBER);

      t_rc_ct_mod_det_g   rec_rc_ct_mod_det_g;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      /*Restting the values*/
      l_posted_flag   := 'N';
      l_delink_status := 'N';

      write_log('Log: p_src_rc_id : '||p_src_rc_id
               ||CHR(10)
               ||'p_dest_rc_id : '   ||p_dest_rc_id);
      BEGIN
         SELECT id
         INTO   l_prd_id
         FROM   rpro_period_g
         WHERE  status = 'OPEN';
      EXCEPTION
      WHEN OTHERS THEN
         l_prd_id:= NULL;
      END;


      BEGIN   
         SELECT book_id
               ,obj_version
               ,rpro_rc_head_pkg.get_posted_flag(indicators)
         INTO   l_book_id
               ,l_obj_version
               ,l_posted_flag
         FROM   rpro_rc_head_g
         WHERE  id = p_src_rc_id;
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : '||SQLERRM);
      END;

      IF l_posted_flag  = 'Y' 
      THEN
         rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => p_src_rc_id
                                                 ,p_obj_version => l_obj_version 
                                                 ,p_book_id     => l_book_id 
                                                 ,p_ret_msg     => l_errbuf 
                                                 ,p_lock_rc     => 'N');
      END IF;

      IF l_errbuf IS NULL  --If unfreeze happened successfully, delinking the rc
      THEN

         BEGIN   
            SELECT book_id
                  ,obj_version
                  ,rpro_rc_head_pkg.get_alloc_trtmt_flag(indicators) --#20201111
            INTO   l_book_id
                  ,l_obj_version
                  ,l_alloc_prv_trtmnt_flag --#20201111
            FROM   rpro_rc_head_g
            WHERE  id = p_src_rc_id;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : l_errbuf :'||l_errbuf||'~'||SQLERRM);
         END;

         --delink api
         rpro_rc_action_pkg.delink_rc_pob ( p_doc_line_id_str => NULL
                                          , p_rc_pob_id_str   => to_char(p_rc_pob_id)
                                          , p_rc_id           => p_src_rc_id
                                          , p_book_id         => l_book_id 
                                          , p_object_version  => l_obj_version 
                                          , p_comments        => 'Atlassian Regrouping delink '||SYSDATE 
                                          , p_retcode         => l_retcode 
                                          , p_errbuf          => l_errbuf 
                                          , p_lock_rc         => 'N' 
                                          , p_commit_flag     => 'Y' 
                                          , p_summary_data    => NULL );
         IF NVL(l_retcode,0) = 0 
         THEN
            l_delink_status := 'Y';
           /*Updating the rc delink flag in grp dtl and head */
            UPDATE rpro_rc_grp_dtl_g
            SET    delink_flag = 'N'
            WHERE  rc_id         = p_src_rc_id
            AND    NVL(delink_flag,'N') = 'Y';

            write_log('Log: Existing rc id : '   ||p_dest_rc_id
                     ||CHR(10)                   
                     ||' Current rc_id: '        ||p_src_rc_id);

            UPDATE rpro_rc_head_g
            SET    indicators = rpro_rc_head_pkg.set_delink_flag(indicators,'N')
            WHERE  id = p_src_rc_id ;
            write_log('Updated Delink Flag '
                      ||' p_dest_rc_id  : '||p_dest_rc_id
                      ||CHR(10)
                      ||'l_ret_code    : '||l_ret_code);
            BEGIN   
               SELECT book_id
                     ,obj_version
                     ,rpro_rc_head_pkg.get_alloc_trtmt_flag(indicators) --#20201111
               INTO   l_book_id
                     ,l_obj_version
                     ,l_alloc_trtmnt_flag --#20201111
               FROM   rpro_rc_head_g
               WHERE  id = p_src_rc_id;
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : l_errbuf :'||l_errbuf||'~'||SQLERRM);
            END;
            --#20201111
            IF l_alloc_trtmnt_flag <> l_alloc_prv_trtmnt_flag
            THEN
               BEGIN
                  UPDATE rpro_rc_head_g
                  SET    indicators    = rpro_rc_head_pkg.set_alloc_trtmt_flag(indicators,l_alloc_prv_trtmnt_flag)
                  WHERE  id            = p_src_rc_id;
               
                  UPDATE rpro_rc_head_g
                  SET    indicators  =  rpro_rc_head_pkg.set_posted_flag(indicators,'Y')
                  WHERE  id          =  p_src_rc_id;
                  COMMIT;
               EXCEPTION WHEN OTHERS
               THEN
                  write_error('Exception while updating alloc trtmnt flag in the rc header table: '
                              ||' RC_ID : '||p_src_rc_id
                              ||CHR(10)
                              ||'error : '||SQLERRM
                              ||CHR(10)
                              ||'error trace : '||dbms_utility.format_error_backtrace);
               END;
               
               BEGIN
                  SELECT book_id
                        ,obj_version
                        ,rpro_rc_head_pkg.get_posted_flag(indicators)
                  INTO   l_book_id
                        ,l_obj_version
                        ,l_posted_flag
                  FROM   rpro_rc_head_g
                  WHERE  id = p_src_rc_id;
               EXCEPTION
               WHEN OTHERS THEN
                  write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : '||SQLERRM);
               END;
               
               IF l_posted_flag  = 'Y' 
               THEN
                  --By default unfreeze will do RETAIN ALLOCATION
                  rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => p_src_rc_id
                                                          ,p_obj_version => l_obj_version 
                                                          ,p_book_id     => l_book_id 
                                                          ,p_ret_msg     => l_errbuf 
                                                          ,p_lock_rc     => 'N');
               
                  write_log('Log: unfreeze rc since it is already posted~rc_id : '||p_src_rc_id
                            ||CHR(10)
                            ||' l_errbuf : '||l_errbuf);
                            
                  BEGIN
                     SELECT book_id
                           ,obj_version
                           ,rpro_rc_head_pkg.get_posted_flag(indicators)
                     INTO   l_book_id
                           ,l_obj_version
                           ,l_posted_flag
                     FROM   rpro_rc_head_g
                     WHERE  id = p_src_rc_id;
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error: Unable to fetch p_src_rc_id book_id/obj version : '||SQLERRM);
                  END;
                  
                  rpro_fv_allocation_pkg.recalc_rc (p_src_rc_id,
                                                    l_obj_version,
                                                    l_book_id,
                                                    l_errbuf,
                                                    'REALLOCATE',
                                                    'N');
               END IF;
            END IF;
            COMMIT;
         END IF;
      ELSE
         write_error('Error: Unable to unfreeze the rc..So delink cannot be done: l_errbuf :'||l_errbuf);
      END IF;
      l_posted_flag := 'N';
      IF l_delink_status = 'Y' --Linking only if delink happens successfully
      THEN
         BEGIN
            SELECT book_id
                  ,obj_version
                  ,rpro_rc_head_pkg.get_posted_flag(indicators)
            INTO   l_book_id
                  ,l_obj_version
                  ,l_posted_flag
            FROM   rpro_rc_head_g
            WHERE  id = p_dest_rc_id;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Error: Unable to fetch p_dest_rc_id book_id/obj version : '||SQLERRM);
         END;

         --l_ct_mod_date := NULL;

         IF l_posted_flag  = 'Y' 
         THEN
            --By default unfreeze will do RETAIN ALLOCATION
            rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => p_dest_rc_id
                                                    ,p_obj_version => l_obj_version 
                                                    ,p_book_id     => l_book_id 
                                                    ,p_ret_msg     => l_errbuf 
                                                    ,p_lock_rc     => 'N');

            write_log('Log: unfreeze rc since it is already posted~rc_id : '||p_dest_rc_id
                      ||CHR(10)
                      ||' l_errbuf : '||l_errbuf);
         END IF;
         BEGIN
            SELECT book_id
                  ,obj_version
                  ,rpro_rc_head_pkg.get_posted_flag(indicators)
            INTO   l_book_id
                  ,l_obj_version
                  ,l_posted_flag
            FROM   rpro_rc_head_g
            WHERE  id = p_dest_rc_id;
         EXCEPTION
         WHEN OTHERS THEN
            write_error('Error: Unable to fetch p_dest_rc_id book_id/obj version : '||SQLERRM);
         END;
                                                   
         rpro_rc_action_pkg.link_rc_pob_line( p_doc_line_id_str   => NULL
                                            , p_rc_id             => p_dest_rc_id
                                            , p_book_id           => l_book_id
                                            , p_object_version    => l_obj_version
                                            , p_rc_pob_id_str     => TO_CHAR(p_rc_pob_id)
                                            , p_comments          => 'Atlassian Regrouping link '||SYSDATE
                                            , p_retcode           => l_retcode
                                            , p_errbuf            => l_errbuf
                                            , p_lock_rc           => 'N'
                                            );
         write_log('After Link RC Level API for RC ID '
                   ||' p_dest_rc_id  : '||p_dest_rc_id
                   ||CHR(10)
                   ||'l_retcode    : '||l_retcode
                   ||CHR(10)
                   ||'l_errbuf     : '||l_errbuf);
         IF l_retcode = 0 
         THEN 
            BEGIN   
               SELECT rpro_rc_line_pkg.get_cv_eligible_flag(indicators) --#20201111
               INTO   l_cv_flag --#20201111
               FROM   rpro_rc_line_g
               WHERE  rc_pob_id = p_rc_pob_id;
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error while fetching cv_eligible_flag : l_errbuf :'||l_errbuf||'~'||SQLERRM);
            END;
            IF l_cv_flag = 'Y'
            THEN
               UPDATE rpro_rc_line_g
               SET    indicators = rpro_rc_line_pkg.set_cv_eligible_flag(indicators,'N')
               WHERE  rc_pob_id = p_rc_pob_id;
               COMMIT;
            END IF;
            BEGIN   
               SELECT rpro_rc_head_pkg.get_alloc_trtmt_flag(indicators) --#20201111
               INTO   l_alloc_trtmnt_flag --#20201111
               FROM   rpro_rc_head_g
               WHERE  id = p_dest_rc_id;
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error: Unable to fetch p_dest_rc_id book_id/obj version : l_errbuf :'||l_errbuf||'~'||SQLERRM);
            END;
            --#20201111
            IF l_alloc_trtmnt_flag = 'P'
            THEN
               BEGIN
                  UPDATE rpro_rc_head_g
                  SET    indicators    = rpro_rc_head_pkg.set_alloc_trtmt_flag(indicators,'R')
                  WHERE  id            = p_dest_rc_id;
                  COMMIT;
               EXCEPTION WHEN OTHERS
               THEN
                  write_error('Exception while updating alloc trtmnt flag in the rc header table: '
                              ||' RC_ID : '||p_dest_rc_id
                              ||CHR(10)
                              ||'error : '||SQLERRM
                              ||CHR(10)
                              ||'error trace : '||dbms_utility.format_error_backtrace);
               END;
            END IF;
            write_log('Successfully linked pob_id : '||p_rc_pob_id|| ' to RC_ID : '||p_dest_rc_id);
         ELSE
            write_log('Error while linking pob_id : '||p_rc_pob_id|| ' to RC_ID : '||p_dest_rc_id);
         END IF;
      END IF;
      l_posted_flag := 'N';
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception cust_link_delink_cv : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END cust_link_delink_cv;
   
   --End #Haribabu P 20201104
   
   PROCEDURE switch_link_delink(  p_retcode    OUT NUMBER
                                , p_errbuf     OUT VARCHAR
                                , p_batch_id   IN  NUMBER DEFAULT NULL
                                , p_rc_id      IN  NUMBER DEFAULT NULL)
   IS
      CURSOR c_rc_level_range
      IS
      SELECT rc_id                                                         rc_id
            ,num15                                                         orig_rc_id
            ,alctbl_fn_xt_prc                                       total_ext_sll_prc
            ,batch_id                                                batch_id               
            ,id                                                      line_id                
            ,rc_pob_id                                              rc_pob_id
      FROM   rpro_rc_line_g rc
      WHERE  (CASE WHEN p_batch_id IS NOT NULL                              
                   THEN rc.batch_id
                   WHEN p_rc_id IS NOT NULL
                   THEN rc.rc_id
              END)                                                    = NVL(p_batch_id,p_rc_id)
      AND    rc.num15                                                 IS NOT NULL
      AND    rpro_rc_line_pkg.get_cv_eligible_flag(rc.indicators)     = 'Y'   
      AND    rpro_rc_line_pkg.get_within_fv_range_flag(rc.indicators) = 'Y'
      AND    NVL(rc.bndl_prnt_id,'X')                                 <> rc.doc_line_id
      AND    rc.err_msg                                               IS NULL
      AND    EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  rrl.batch_id                                              = p_batch_id
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)      = 'Y'
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators) = 'N'
                     AND    rrl.rc_id                                                <> rc.rc_id
                     AND    NVL(rrl.atr16,'X')                                        = NVL(rc.atr16,'X')   -- Contract number
                     AND    rrl.doc_num                                               = rc.doc_num
                     );
              
      l_with_in_range_flag     VARCHAR(1);
      l_rc_id                  NUMBER;
      l_dest_rc_id             NUMBER;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('Log: Start switch_link_delink : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      write_log('Log:   p_batch_id : '     ||p_batch_id
                ||CHR(10)
                ||'     rc_id      : '     ||p_rc_id);
                
      FOR rec IN c_rc_level_range
      LOOP
           /*Calling delink/link API through scheduler JOB*/
            IF rec.orig_rc_id IS NOT NULL
            THEN
               cust_link_delink( p_src_rc_id  => rec.rc_id
                               , p_dest_rc_id => rec.orig_rc_id 
                               , p_batch_id   => rec.batch_id           
                               , p_line_id    => rec.line_id
                               , p_rc_pob_id  => rec.rc_pob_id);          
            END IF; 
            write_log('Log: rc_id : '           ||rec.rc_id
                     ||CHR(10)                  
                     ||'rec.orig_rc_id : '        ||rec.orig_rc_id);
            /*UPDATE rpro_rc_line_g
            SET    num15 = NULL   --resetting the original rc's to refresh for future add on lines
            WHERE  rc_id    = rec.orig_rc_id
            AND    (CASE WHEN p_batch_id IS NOT NULL
                         THEN batch_id
                         ELSE 1
                    END)      = NVL(p_batch_id,1);
         COMMIT;*/
      END LOOP;
   write_log('Log: End switch_link_delink : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception switch_link_delink : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END switch_link_delink;
   -----------------------Logesh Added Start
   PROCEDURE switch_link_delink_cv(  p_retcode    OUT NUMBER   --20201028
                                , p_errbuf     OUT VARCHAR
                                , p_batch_id   IN  NUMBER DEFAULT NULL
                                , p_rc_id      IN  NUMBER DEFAULT NULL)
   IS
      CURSOR c_rc_level_range
      IS
      SELECT rc_id                                                         rc_id
            ,num15                                                         orig_rc_id
            ,alctbl_fn_xt_prc                                       total_ext_sll_prc
            ,batch_id                                                batch_id               
            ,id                                                      line_id                
            ,rc_pob_id                                              rc_pob_id
            ,doc_num                                                doc_num
            ,atr16                                                  atr16
      FROM   rpro_rc_line_g rc
      WHERE  rc.batch_id                                              = p_batch_id
      AND    rc.num15                                                 IS NOT NULL   -- picking only the rc_id's updated in the after validate procedure(i.e revision lines)  
      AND    rpro_rc_line_pkg.get_cv_eligible_flag(rc.indicators)     = 'N'   
      AND    rpro_rc_line_pkg.get_within_fv_range_flag(rc.indicators) = 'N'
      AND    rc.err_msg                                               IS NULL
    --AND    NVL(rc.bndl_prnt_id,'X')                                 <> rc.doc_line_id
      AND    EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  rrl.batch_id                                               = p_batch_id
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)      = 'Y'
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators)  = 'Y'
                     AND    rrl.rc_id                                                 != rc.rc_id
                     AND    NVL(rrl.atr16,'X')                                         = NVL(rc.atr16,'X')   -- Contract number
                     AND    rrl.doc_num                                                = rc.doc_num
                    )
     AND NOT EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  rrl.batch_id                                              = p_batch_id
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)      = 'Y'
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators) = 'N'
                     AND    rrl.rc_id                                                 = rc.rc_id
                     AND    NVL(rrl.atr16,'X')                                        = NVL(rc.atr16,'X')   -- Contract number
                     AND    rrl.doc_num                                               = rc.doc_num
                    );
              
      l_with_in_range_flag     VARCHAR(1);
      l_rc_id                  NUMBER;
      l_dest_rc_id             NUMBER;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('Log: Start switch_link_delink : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      write_log('Log:   p_batch_id : '     ||p_batch_id
                ||CHR(10)
                ||'     rc_id      : '     ||p_rc_id);
                
      FOR rec IN c_rc_level_range
      LOOP  
      
            SELECT rc_id
            INTO   l_dest_rc_id
            FROM   rpro_rc_line_g rrl 
            WHERE  rrl.batch_id                                              = p_batch_id
            AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)     = 'Y'
            AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators) = 'Y'
            AND    rrl.rc_id                                                != rec.rc_id
            AND    NVL(rrl.atr16,'X')                                        = NVL(rec.atr16,'X')   -- Contract number
            AND    rrl.doc_num                                               = rec.doc_num
            AND    ROWNUM                                                    = 1;
            
           /*Calling delink/link API through scheduler JOB*/
            IF rec.rc_id IS NOT NULL
            THEN
               cust_link_delink_cv( p_src_rc_id  => rec.rc_id                                           --#Haribabu P 20201104
                                  , p_dest_rc_id => l_dest_rc_id--rec.orig_rc_id 
                                  , p_batch_id   => rec.batch_id           
                                  , p_line_id    => rec.line_id
                                  , p_rc_pob_id  => rec.rc_pob_id);          
            END IF; 
            write_log('Log: rc_id : '           ||rec.rc_id
                     ||CHR(10)                  
                     ||'rec.orig_rc_id : '        ||rec.orig_rc_id);
      END LOOP;
   write_log('Log: End switch_link_delink : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception switch_link_delink_cv : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END switch_link_delink_cv;
   -----------------------------------------------------------------------------
   PROCEDURE cmod_wrapper( p_retcode    OUT NUMBER
                         , p_errbuf     OUT VARCHAR
                         , p_batch_id   IN  NUMBER DEFAULT NULL
                         , p_rc_id      IN  NUMBER DEFAULT NULL)
   IS
      l_retcode            NUMBER;
      l_errbuf             VARCHAR2(1000);
   BEGIN
       write_log('Log: Start cmod_wrapper : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
       write_log('Log:   p_batch_id : '     ||p_batch_id
                 ||CHR(10)
                 ||'     rc_id      : '     ||p_rc_id);
              switch_link_delink( p_retcode    => l_retcode
                                , p_errbuf     => l_errbuf
                                , p_batch_id   => p_batch_id
                                , p_rc_id      => p_rc_id );
                                
              switch_link_delink_cv( p_retcode    => l_retcode
                                  , p_errbuf     => l_errbuf
                                  , p_batch_id   => p_batch_id
                                  , p_rc_id      => p_rc_id );                 
       write_log('Log: End cmod_wrapper : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   END cmod_wrapper;
   -----------------------Logesh Added End
   PROCEDURE fv_roll_up_wrapper ( p_rc_id    IN NUMBER DEFAULT NULL
                                , p_batch_id IN NUMBER DEFAULT NULL)
   IS
      l_retcode            NUMBER;
      l_errbuf             VARCHAR2(1000);
      l_request_id         NUMBER;
      l_prog_id            NUMBER;
      l_cnt                NUMBER;
      l_cv_cnt             NUMBER;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('Log: Start fv_roll_up_wrapper : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      
      SELECT COUNT(1)
      INTO   l_cnt
      FROM   rpro_rc_line_g rc
      WHERE  rc.batch_id                                              = p_batch_id
      AND    rc.num15                                                 IS NOT NULL   -- picking only the rc_id's updated in the after validate procedure(i.e revision lines)  
      AND    rpro_rc_line_pkg.get_cv_eligible_flag(rc.indicators)     = 'Y'   
      AND    rpro_rc_line_pkg.get_within_fv_range_flag(rc.indicators) = 'Y'
      AND    rc.err_msg                                               IS NULL
      AND    NVL(rc.bndl_prnt_id,'X')                                 <> rc.doc_line_id
      AND    EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  rrl.batch_id                                              = p_batch_id
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)      = 'Y'
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators) = 'N'
                     AND    rrl.rc_id                                                <> rc.rc_id
                     AND    NVL(rrl.atr16,'X')                                        = NVL(rc.atr16,'X')   -- Contract number
                     AND    rrl.doc_num                                               = rc.doc_num
                    );
      
      --Scenario to handle if CV eligible comes with N , it need to go to new RC 20201029
      SELECT COUNT(1)
      INTO   l_cv_cnt
      FROM   rpro_rc_line_g rc
      WHERE  rc.batch_id                                              = p_batch_id
      AND    rc.num15                                                 IS NOT NULL   -- picking only the rc_id's updated in the after validate procedure(i.e revision lines)  
      AND    rpro_rc_line_pkg.get_cv_eligible_flag(rc.indicators)     = 'N'   
      AND    rpro_rc_line_pkg.get_within_fv_range_flag(rc.indicators) = 'N'
      AND    rc.err_msg                                               IS NULL
    --AND    NVL(rc.bndl_prnt_id,'X')                                 <> rc.doc_line_id
      AND    EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  rrl.batch_id                                               = p_batch_id
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)      = 'Y'
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators)  = 'Y'
                     AND    rrl.rc_id                                                 != rc.rc_id
                     AND    NVL(rrl.atr16,'X')                                         = NVL(rc.atr16,'X')   -- Contract number
                     AND    rrl.doc_num                                                = rc.doc_num
                    )
     AND NOT EXISTS (SELECT 1 
                     FROM   rpro_rc_line_g rrl 
                     WHERE  rrl.batch_id                                              = p_batch_id
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators)      = 'Y'
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl.indicators) = 'N'
                     AND    rrl.rc_id                                                 = rc.rc_id
                     AND    NVL(rrl.atr16,'X')                                        = NVL(rc.atr16,'X')   -- Contract number
                     AND    rrl.doc_num                                               = rc.doc_num
                    );
      
      write_log('Log: fv_roll_up_wrapper l_cnt : '||l_cnt);
      IF l_cnt > 0 OR l_cv_cnt > 0
      THEN
         BEGIN
            SELECT id
            INTO   l_prog_id
            FROM   rpro_prog_head_g
            WHERE  name = 'Revpro3.0 Custom link/delink';  --20201029
         EXCEPTION 
         WHEN OTHERS THEN
            write_error('Error: Unable to find the link/delink custom job: '||SQLERRM);
            RETURN; 
         END;

         rpro_job_scheduler_pkg.schedule_job (p_errbuf                 => l_errbuf
                                             ,p_retcode                => l_retcode
                                             ,p_request_id             => l_request_id
                                             ,p_program_id             => l_prog_id
                                             ,p_parameter_text         => p_batch_id||'~'||NULL||'~'||NULL
                                             ,p_requested_start_date   => SYSDATE
                                             ,p_requested_end_date     => NULL
                                             ,p_request_date           => SYSDATE
                                             ,p_parent_req_id          => rpro_utility_pkg.g_request_id
                                             );

         write_log('Log: l_errbuf : '||l_errbuf||' l_retcode : '||l_retcode);
      END IF;

      write_log('Log: End fv_roll_up_wrapper : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception fv_roll_up_wrapper : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END fv_roll_up_wrapper;
   --------------------------------------------------------------------------------------------------------
   PROCEDURE cust_switch_allocation( p_src_id     IN NUMBER
                                   , p_alloc_trmt IN VARCHAR)
   IS
      l_obj_ver                      NUMBER;
      l_book_id                      NUMBER;
      l_code                         NUMBER;
      l_message                      VARCHAR2(1000);
      l_chng_id                      VARCHAR2(1000);
      l_ret_msg                      VARCHAR2(2000);
      l_alloc_trtmt                  VARCHAR2(1000);
   BEGIN
      SELECT obj_version
            ,book_id
      INTO   l_obj_ver
            ,l_book_id
      FROM   rpro_rc_head
      WHERE  id      = p_src_id 
      AND    ROWNUM = 1;

      IF p_alloc_trmt = 'P'
      THEN
         l_alloc_trtmt:= 'Prospective';
      ELSE
         l_alloc_trtmt:= 'Retrospective';
      END IF;
      
      rpro_rc_action_pkg.check_elgbl_for_switch_alct( p_src_id
                                                    , l_book_id
                                                    , l_ret_msg
                                                     );
                                                     
      write_log('Log: Swtich allocation: err msg:'||l_ret_msg);
      
      IF l_ret_msg = 'N'
      THEN
         rpro_ws_wb_rc_pkg.rpro_rc_switch_allocation ( p_rc_id        => p_src_id
                                                     , p_book_id      => l_book_id
                                                     , p_obj_ver      => l_obj_ver
                                                     , p_alloc_trmt   => l_alloc_trtmt
                                                     , p_code         => l_code
                                                     , p_message      => l_message
                                                     , p_chng_id      => l_chng_id );
      ELSE
         l_message := l_ret_msg;
         l_code :=1;
      END IF;
      
      write_log('Log: l_message: '||l_message);
      
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception cust_switch_allocation : '
                 ||'error : '||SQLERRM
                 ||CHR(10)
                 ||'error trace : '||dbms_utility.format_error_backtrace);
   END cust_switch_allocation;
   --------------------------------------------------------------------------------------------------------
   PROCEDURE rpro_cmod_link_delink( p_rc_id      IN VARCHAR2 DEFAULT NULL
                                  , p_batch_id   IN NUMBER )

   AS 
      CURSOR c_eligible_rcs
      IS
      SELECT rc_id
      FROM   rpro_rc_line_g rrl
      WHERE  NVL(rrl.atr24,'N')   = 'Y'        --Multi Inv Ref Flag
      AND    UPPER(rrl.atr41)    IN  ('UPGRADE','DOWNGRADE') --Sale Action
      AND    rrl.atr55           IS NULL 
      AND    rrl.batch_id         = p_batch_id  --20201202
      AND    rrl.rc_id           > 0
      AND    EXISTS( SELECT 1 
                     FROM   rpro_rc_line_g rrl1
                     WHERE  rrl1.atr24 = rrl.atr24
                     AND    rrl1.atr41 = rrl.atr41
                     AND    rrl1.rc_id = rrl.rc_id
                     AND    rpro_rc_line_pkg.get_within_fv_range_flag(rrl1.indicators) = 'N'
                     AND    rpro_rc_line_pkg.get_cv_eligible_flag(rrl1.indicators)     = 'Y')  --20201106
      GROUP BY rrl.rc_id
      HAVING COUNT(rc_id) > 1;
      
      
      CURSOR c_upg_line(p_rc_id IN NUMBER) 
      IS
      SELECT rrb.atr43            orig_inv_num
            ,rrb.start_date       start_date
            ,rrb.rc_id            rc_id
            ,rrb.atr41            atr41
            ,rrb.doc_date         doc_date
            ,rrb.atr17
            ,rrb.atr16
            ,rowid                row_id
      FROM   rpro_rc_bill_g       rrb
      WHERE  1                   =  1
      AND    rrb.type            =  'INV'
      AND    NVL(rrb.atr24,'N')  =  'Y'
      AND    NVL(rrb.atr55,'N') !=  'Y'
      AND    UPPER(rrb.atr41)    =  'UPGRADE'
      AND    rrb.doc_num        !=   NVL(rrb.atr43,'X')
      AND    rrb.rc_id           =   p_rc_id
      AND    EXISTS (SELECT 1
                     FROM   rpro_rc_bill_g rrb1
                     WHERE  1                  = 1
                     AND    rrb.atr43          = rrb1.doc_num
                     AND    rrb1.type          = 'INV'
                     AND    rrb1.rc_id         > 0
                     AND    rrb1.rc_id         != rrb.rc_id   
                     AND    rrb.start_date     < rrb1.end_date
                     );

      l_rc_id_data        VARCHAR2(1000);
      l_tmpl_id           rpro_rc_tmpl_g.id%TYPE;
      l_new_rc_id         rpro_rc_line.rc_id%TYPE;
      l_errbuf            VARCHAR2(1000);
      l_retcode           NUMBER;
      l_rec               NUMBER;
      l_upg_rc_id         NUMBER;
      l_rc_count          NUMBER;
      l_period_name       rpro_calendar_g.period_name%TYPE;
      l_posted_flag       VARCHAR2(1);
      l_book_id           NUMBER;
      l_obj_version       NUMBER;
      

      l_cont_num          VARCHAR2(1000);
      l_orig_cmod_trmt    VARCHAR2(1);
      l_curr_cmod_trmt    VARCHAR2(1);
   BEGIN
      write_log('Log:Inside '||l_proc||'************************');

      FOR rec IN c_eligible_rcs
      LOOP
         write_log('********Log: Eligible rc:**********'||rec.rc_id);
         rpro_atl_custom_pkg.g_min_doc_date := NULL;
         rpro_atl_custom_pkg.g_min_rc_id    := NULL;
         FOR r_upg_line IN c_upg_line(rec.rc_id)
         LOOP

              write_log('Log: Level 1 rc_id~'||r_upg_line.rc_id
                       ||CHR(10)
                       ||'~ORIG_INV_NUM~'    ||r_upg_line.orig_inv_num);
                   
            
            
                     
            FOR r_ref_line IN(SELECT   atr43
                                      ,atr46
                                      ,atr17 --cmod_dlag
                                      ,atr41 --Sale Action
                                      ,rc_id
                                      ,doc_num
                                      ,start_date
                                      ,end_date
                                      ,doc_date
                              FROM     rpro_rc_bill_g A
                              WHERE    1          = 1
                              AND      type       = 'INV'
                              AND      doc_num    = r_upg_line.orig_inv_num
                              AND      a.doc_num != NVL(a.atr43 ,'X')
                              AND      a.rc_id   != r_upg_line.rc_id  --20201119
                              AND      rc_id      > 0 --Eliminate delinked lines
                              )
            LOOP
               /*Open Nested LOOP*/
               /*Handling the def amount*/
         
               IF r_ref_line.end_date < r_upg_line.start_date
               THEN
                  write_log('Log: Exiting...Stop traversing further as the child line doesnt have def amount. Level 1 r_ref_line.end_date~'
                            ||r_ref_line.end_date
                            ||'~r_upg_line.start_date~'
                            ||r_upg_line.start_date
                            ||'~Mod sen num~'
                            ||r_ref_line.atr46
                            );
                  /*Stop traversing further as the child line doesnt have def amount*/
                  l_temp_inv_number:=NULL;
                  EXIT;
               ELSE
                  write_log('Log: Line in def state');
               END IF;
               
               
               write_log('Log: Level2 rc_id~'||r_ref_line.rc_id
                        ||CHR(10)
                        ||'~ORIG_INV_NUM~'   ||r_ref_line.atr43);
                        
 
                        
               /*Deleting the exisitng records with old errors*/
               DELETE
               FROM    rpro_atl_cust_cmod_det
               WHERE   1                     = 1
               AND     invoice_num           = r_ref_line.doc_num
               AND     NVL(orig_inv_num,'X') = NVL(r_ref_line.atr43,'X');
         
               INSERT
               INTO rpro_atl_cust_cmod_det(rc_id
                                          ,invoice_num
                                          ,orig_inv_num
                                          ,crtd_by
                                          ,crtd_dt
                                          ,updt_by
                                          ,updt_dt
                                           )
               VALUES                     (r_ref_line.rc_id
                                          ,r_ref_line.doc_num
                                          ,r_ref_line.atr43
                                          ,rpro_utility_pkg.g_user_id
                                          ,sysdate
                                          ,rpro_utility_pkg.g_user_id
                                          ,sysdate
                                          );
         
               l_temp_inv_number       := r_ref_line.atr43;
               l_prnt_st_dt            := r_ref_line.start_date;
         
               
               WHILE l_temp_inv_number IS NOT NULL
               LOOP
         
                  rpro_atl_recursive_itrtn(l_temp_inv_number
                                          ,l_mod_sen_number
                                          ,l_prnt_st_dt
                                          ,r_ref_line.rc_id  --20201119
                                          );
               END LOOP;
               
         
               l_cnt                 := l_cnt+1 ;
               rc_tab(l_cnt).rc_id   := r_ref_line.rc_id;
               rc_tab(l_cnt).atr43   := r_ref_line.atr43;
               rc_tab(l_cnt).doc_num := r_ref_line.doc_num;
               rc_tab(l_cnt).atr41   := NULL;--r_ref_line.atr41;
               rc_tab(l_cnt).atr17   := r_ref_line.atr17;
         
            END LOOP;/*Close Nested LOOP*/
         
            l_cnt                 := l_cnt+1 ;
            rc_tab(l_cnt).rc_id   := r_upg_line.rc_id;  --20201111
            rc_tab(l_cnt).atr43   := r_upg_line.orig_inv_num;
            rc_tab(l_cnt).atr41   := r_upg_line.atr41;
            rc_tab(l_cnt).atr17   := r_upg_line.atr17;
            rc_tab(l_cnt).doc_num := NULL;
         
            
         END LOOP;
         
         l_cnt:=0;
         /*Reintializing the collection*/
         FOR r_rec IN 1..rc_tab.COUNT
         LOOP
            rc_tab_type.EXTEND();
            rc_tab_type(r_rec).rc_id   := rc_tab(r_rec).rc_id;
            rc_tab_type(r_rec).doc_num := rc_tab(r_rec).doc_num; --Invoice Number
            rc_tab_type(r_rec).atr17   := rc_tab(r_rec).atr17;
            rc_tab_type(r_rec).atr41   := rc_tab(r_rec).atr41;
         END LOOP;
         
         
         SELECT rc_id                                            --20201118
               ,doc_date
               ,atr16              
         INTO   rpro_atl_custom_pkg.g_min_rc_id
               ,rpro_atl_custom_pkg.g_min_doc_date
               ,l_cont_num
         FROM ( SELECT rc_id
               ,doc_date
               ,atr16
               ,row_number() over (order by doc_date,rc_id asc) as rn 
                FROM  rpro_rc_bill a
                WHERE rc_id IN    (SELECT rc_id 
                                   FROM   TABLE(rc_tab_type))
                AND   type        = 'INV' )
         WHERE rn = 1;
         
        write_log('Log: g_min_doc_date:'||rpro_atl_custom_pkg.g_min_doc_date
        ||CHR(10)
        ||'g_min_rc_id:'                ||rpro_atl_custom_pkg.g_min_rc_id); 
         
         
         SELECT rpro_rc_head_pkg.get_alloc_trtmt_flag(indicators)  --20201118
         INTO   l_orig_cmod_trmt
         FROM   rpro_rc_head
         WHERE  id = rpro_atl_custom_pkg.g_min_rc_id;
         
         write_log('Log: Original Treatment: '||l_orig_cmod_trmt);
         
         SELECT DISTINCT rc_id
                        ,NULL doc_num
                        ,NULL atr17
                        ,NULL atr41
         BULK COLLECT
         INTO rc_tab_type_dist_rc
         FROM TABLE(rc_tab_type)
         WHERE rc_id != rpro_atl_custom_pkg.g_min_rc_id;
         
        
         IF rc_tab_type_dist_rc.COUNT >= 1  --20201119
         THEN
            write_log('Log: Count RC_TAB_TYPE_DIST_RC~'||rc_tab_type_dist_rc.count);
            l_rc_id_data :='';
            g_alloc_trmt := 'P'; --06192018
            write_log('Log Initial: g_alloc_trmt~'||g_alloc_trmt);
            FOR rec IN rc_tab_type_dist_rc.FIRST..rc_tab_type_dist_rc.LAST
            LOOP
               rpro_atl_delink(p_rc_id   => rc_tab_type_dist_rc(rec).rc_id
                              ,p_atr17   => rc_tab_type_dist_rc(rec).atr17
                              ,p_atr41   => rc_tab_type_dist_rc(rec).atr41);
               l_rc_id_data := l_rc_id_data || ':'||rc_tab_type_dist_rc(rec).rc_id*(-1);
               write_log('Log: L_RC_ID_DATA~'||l_rc_id_data);
         
               write_log('Log: after delink g_alloc_trmt~'||g_alloc_trmt || '~rc_tab_type_dist_rc(rec).rc_id ~'||rc_tab_type_dist_rc(rec).rc_id);
            END LOOP;
            
            write_log('Log: Rc to be linked:'||rpro_atl_custom_pkg.g_min_rc_id);
            
            --Start: Linking to minimum rc
            BEGIN
               SELECT book_id
                     ,obj_version
                     ,rpro_rc_head_pkg.get_posted_flag(indicators)
               INTO   l_book_id
                     ,l_obj_version
                     ,l_posted_flag
               FROM   rpro_rc_head_g
               WHERE  id = rpro_atl_custom_pkg.g_min_rc_id;
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error: Unable to fetch g_min_rc_id book_id/obj version : '||SQLERRM);
            END;
            
            IF l_posted_flag  = 'Y' 
            THEN
               --By default unfreeze will do RETAIN ALLOCATION
               rpro_rc_collect_pkg.unfreeze_rc_wrapper (p_rc_id       => rpro_atl_custom_pkg.g_min_rc_id
                                                       ,p_obj_version => l_obj_version 
                                                       ,p_book_id     => l_book_id 
                                                       ,p_ret_msg     => l_errbuf 
                                                       ,p_lock_rc     => 'N');
            
               write_log('Log: unfreeze rc since it is already posted~rc_id : '||rpro_atl_custom_pkg.g_min_rc_id
                         ||CHR(10)
                         ||' l_errbuf : '||l_errbuf);
            END IF;
            
            BEGIN
            --RC level link api
            l_rc_id_data := LTRIM(l_rc_id_data,':');
            l_rc_id_data := RTRIM(l_rc_id_data,':');
            
            rpro_rc_action_pkg.link_rc_level (p_rc_id_str         => l_rc_id_data
                                             ,p_linked_to_rc_id   => rpro_atl_custom_pkg.g_min_rc_id 
                                             ,p_book_id           => l_book_id
                                             ,p_object_version    => l_obj_version 
                                             ,p_comments          => 'ATL Regrouping link '||SYSDATE 
                                             ,p_retcode           => l_retcode
                                             ,p_errbuf            => l_errbuf
                                             );
            EXCEPTION WHEN OTHERS
            THEN
               write_error('Error while delinking:l_retcode:'||l_retcode||' l_errbuf: '||l_errbuf);
            END;
            --End: Linking to minimum rc
            COMMIT;

            SELECT rpro_rc_head_pkg.get_alloc_trtmt_flag(indicators)  --20201118
            INTO   l_curr_cmod_trmt
            FROM   rpro_rc_head
            WHERE  id = rpro_atl_custom_pkg.g_min_rc_id;
            
            write_log('Log: Current Treatment: '||l_curr_cmod_trmt);
            
            IF l_curr_cmod_trmt != l_orig_cmod_trmt                    --20201118
            THEN
               write_log('Log: Switching the modication:');
               cust_switch_allocation( rpro_atl_custom_pkg.g_min_rc_id 
                                     , l_curr_cmod_trmt);
            END IF;
            
            BEGIN                  
               UPDATE rpro_rc_line_g
               SET    atr55 = 'Y'
                     --,atr16 = l_cont_num
               WHERE  rc_id                         = rpro_atl_custom_pkg.g_min_rc_id;
         
               write_log('Log: rc line update count~'||sql%rowcount||'~rc_id~'||rpro_atl_custom_pkg.g_min_rc_id);
         
               UPDATE rpro_rc_bill_g
               SET    atr55 = 'Y'
                     --,atr16 = l_cont_num
               WHERE  rc_id                         = rpro_atl_custom_pkg.g_min_rc_id;
         
               write_log('Log: rc bill update count~'||sql%rowcount||'rc_id~'||rpro_atl_custom_pkg.g_min_rc_id);
         
               COMMIT;

            EXCEPTION
            WHEN OTHERS THEN
               write_error('Exception: '||sqlerrm||'~L_ERRBUF~'||l_errbuf||'~L_RETCODE~'||l_retcode  );
            END;
         END IF;
         rc_tab_type_dist_rc.DELETE ;
         rc_tab.DELETE;
         rc_tab_type.DELETE;
         write_log('***************');
         
      END LOOP; --c_eligible_rcs end loop
      /*Close C_UPG_LINE */
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Log: Main Exception@~'||dbms_utility.format_error_backtrace||'~'||sqlerrm  );
   END rpro_cmod_link_delink;
   ---------------------------------------------------------------------------------------------------------------------------------
END rpro_atl_custom_pkg;
/