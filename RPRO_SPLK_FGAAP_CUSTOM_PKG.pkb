CREATE OR REPLACE PACKAGE BODY rpro_splk_fgaap_custom_pkg
AS
  /**************************************************************************************************
  * Name : RPRO_SPLK_FGAAP_CUSTOM_PKG.pkb *
  * Author : Zuora *
  * Date : 01-Nov-2016 *
  * Description : *
  * Version : *
  * $Header: RPRO_SPLK_FGAAP_CUSTOM_PKG.pkb 1 2015-09-31 20:35:45 admin $ *
  * Modifications History: *
  * *
  * Modified By           Date        Version     Description *
  * --------------      ----------- ------------  -----------------------------------------*
  Leeyo               01-Nov-2016   1.0         Splunk Future GAAP Customization for 606
  Gokul               15-Nov-2018   2.0         Round of to 15 decimal fix in CSSM bundle
  Gourav              13-Nov-2018   3.0         alctd_prc_within_assp : ASSP_WITHIN_RANGE FLG(Atr49) to be updated as 'Y' if allocated amount is between range.
  Gokul               29-NOV-2018   4.0         Added term licence/pertual licence logic in ASSP flag derivation
  Raphael C           03-Jan-2019   5.0         Unbill report code fix Suppport#103215
  Gokul               04-DEC-2018   6.0         splunk_qtd_wf_rep is added.:GK12182018
  Elakkiya            12-MAR-2019   7.0         Changed the qtd report view  #03122019
  Elakkiya            13-NOV-3018   8.0         Added condtion in MJE and rebill Comments: EK 11132018
  Elakkiya            27-NOV-2018   8.1         Handle the updated SO scenario in rebill EK11272018
  Elakkiya            30-NOV-2018   8.2         Added unbill flag condition Comments:EK11302018
  Elakkiya            02-JAN-2019   8.3         Handled the manual release scenario for credit and rebill :EK01022019
  Elakkiya            19-MAR-2019   9.0         Handled the Premium Support in level2 allocation #03192019
  Elakkiya            16-MAR-2019   10.0        CSM bundle num7 update #03082019
  GOKUL               16-MAR-2019   10.1        Added logic to derive level2 fv based on the custom upload#04122019
  Elakkiya            04-APR-2019   11.0        GL outbound changes to forall #04042019
  GOKUL               25-APR-2019   12.0        Added Order quantity in the Formula #25042019
  Elakkiya            09-Nov-2018   13.0        l3_alctd_amt : Calculating the level 3 allocated amount
  Elakkiya            28-FEB-2019   14.0        Handled num6<>0 condition and min and max date condition #02282019
  Elakkiya            22-MAR-2019   15.0        Storing the prior alctd_xt_prc in attribute28 #03222019
  Gokul               19-APR-2019   16.0        Ramp Deal Enhancement changes  #04192019,#04242019
  Elakkiya            12-NOV-2019   25.1        Added 2 period amout in the waterfall report --#20191112
  Elakkiya            19-NOV-2019   25.2        Included adjustment in the qtd report--#20191119
  Logesh              13-JAN-2020   25.3        Handled Rounding issue #01132020
  Gokul               16-JAN-2020   25.4        Handled looping logic for level2--16012020
  Manisundaram        17-MAR-2020   25.5        Handled CR DR Mis match Records  #MS17032020
  Logesh              01-APR-2020   25.6        LV20200401
  Logesh              15-APR-2020   25.7        Moved rounding from 2 to 10 20200415
  Logesh              06-MAY-2020   25.8        Commented split logic and added fix 20200506
  Logesh              24-AUG-2020   25.9        Sequenced prod ctgry from lookup    LV20200824
  Avinash R           25-AUG-2020   26.0        Added Fix for sequence prod ctgry   20200826
  Logesh V            03-SEP-2020   26.1        Fixed variable reset issue          LV20200903
  Logesh V            10-SEP-2020   26.2        Fixed num1 resetting for training   LV20200910
  Logesh V            14-SEP-2020   26.3        Enhancements from num6 to num7      LV20200914
  Logesh V            14-SEP-2020   26.3        Enhancements from num7 to num5      LV20200915
  Implementation
  **************************************************************************************************/
  /************************************************************************************************
  *Copyright - 2009 Leeyo Software. All rights reserved
  *
  *
  * Leeyo REVPRO product is an application modified and extended for clients, based on the REVPRO   *
  * owned by Leeyo Software.
  *
  * All related documentation, code, methods and other materials concerning REVPRO are the property *
  * of Leeyo Software and may not be reused, copied, or transmitted in material or electronic form  *
  * without the express written consent of Leeyo Software.
  *
  **************************************************************************************************/

   gc_module         CONSTANT  VARCHAR2(150)     := 'RPRO_SPLK_FGAAP_CUSTOM_PKG';
   gc_log_level      CONSTANT  NUMBER            :=  10;
   g_rc_id                     rpro_rc_head.id%TYPE;
   g_crtd_prd_id               NUMBER;
   le_dml_exp                  EXCEPTION;                      --#04042019
   PRAGMA EXCEPTION_INIT (le_dml_exp, -24381);                 --#04042019


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


   --LV20200401
   FUNCTION get_unbilled_amt (p_root_line_id     IN   rpro_rc_line_g.id%TYPE
                             ,p_type             IN   VARCHAR2
                             ,p_prd_id           IN   rpro_calendar_g.id%TYPE)
   RETURN NUMBER
   IS
      l_unbill_amt   NUMBER := 0;
   BEGIN
      SELECT  NVL(SUM(cr_amount-dr_amount),0) amount
      INTO   l_unbill_amt
      FROM   (SELECT   schd.rc_id
                      ,schd.amount
                      ,schd.curr
                      ,schd.f_ex_rate
                      ,schd.ex_rate_date
                      ,schd.post_date
                      ,schd.prd_id
                      ,schd.post_prd_id
                      ,schd.post_batch_id
                      ,schd.root_line_id
                      ,schd.ref_bill_id
                      ,schd.id
                      ,schd.line_id
                      ,DECODE (dual.id, 1, schd.dr_segments, 2, schd.cr_segments) acctg_seg
                      ,DECODE (dual.id, 1, DECODE (SIGN (schd.amount), 1, schd.amount, 0), 2, DECODE (SIGN (schd.amount), 1, 0, ABS (schd.amount))) dr_amount
                      ,DECODE (dual.id, 1, DECODE (SIGN (schd.amount), 1, 0, ABS (schd.amount)), 2, DECODE (SIGN (schd.amount), 1, schd.amount, 0)) cr_amount
                      ,DECODE (dual.id, 1, rpro_rc_schd_pkg.get_dr_acctg_flag (schd.indicators), 2, rpro_rc_schd_pkg.get_cr_acctg_flag (schd.indicators)) acctg_type
                      ,rpro_rc_schd_pkg.get_unbilled_flag(schd.indicators)  unbilled_flag
              FROM     rpro_rc_schd_g schd ,
                       (SELECT 1 ID
                        FROM   DUAL
                        UNION ALL
                        SELECT 2 ID
                        FROM   DUAL
                       ) dual
              ) schd ,
              rpro_acct_type_g acct
      WHERE  schd.acctg_type = acct.id
      AND    schd.unbilled_flag   = 'Y'
      AND    schd.root_line_id    = p_root_line_id
      AND    post_prd_id         <= p_prd_id
      AND    acct.name            = p_type;
      RETURN l_unbill_amt;
   EXCEPTION
   WHEN OTHERS THEN
      l_unbill_amt := 0;
      RETURN l_unbill_amt;
   END get_unbilled_amt;

  FUNCTION rpro_splk_schd_amt(
      p_line_id IN NUMBER,
      p_type    IN VARCHAR2,
      p_prd_id  IN NUMBER)
    RETURN NUMBER
  AS
    l_amount NUMBER:=0;

    CURSOR c_amt
    IS

      SELECT   SUM(CR_AMOUNT-DR_AMOUNT) amount
        FROM
          (SELECT   SCHD.RC_ID ,
              SCHD.AMOUNT ,
              SCHD.CURR ,
              SCHD.F_EX_RATE ,
              SCHD.EX_RATE_DATE ,
              SCHD.POST_DATE ,
              SCHD.PRD_ID ,
              SCHD.POST_PRD_ID ,
              SCHD.POST_BATCH_ID ,
              SCHD.ROOT_LINE_ID ,
              SCHD.REF_BILL_ID ,
              SCHD.ID ,
              SCHD.LINE_ID ,
              DECODE (DUAL.ID, 1, SCHD.DR_SEGMENTS, 2, SCHD.CR_SEGMENTS) ACCTG_SEG ,
              DECODE (DUAL.ID, 1, DECODE (SIGN (SCHD.AMOUNT), 1, SCHD.AMOUNT, 0), 2, DECODE (SIGN (SCHD.AMOUNT), 1, 0, ABS (SCHD.AMOUNT))) DR_AMOUNT ,
              DECODE (DUAL.ID, 1, DECODE (SIGN (SCHD.AMOUNT), 1, 0, ABS (SCHD.AMOUNT)), 2, DECODE (SIGN (SCHD.AMOUNT), 1, SCHD.AMOUNT, 0)) CR_AMOUNT ,
              DECODE (DUAL.ID, 1, RPRO_RC_SCHD_PKG.GET_DR_ACCTG_FLAG (SCHD.INDICATORS), 2, RPRO_RC_SCHD_PKG.GET_CR_ACCTG_FLAG (SCHD.INDICATORS)) ACCTG_TYPE
            FROM RPRO_RC_SCHD_G SCHD ,
              (SELECT 1 ID FROM DUAL
            UNION ALL
            SELECT 2 FROM DUAL
              ) dual
          ) SCHD ,
          RPRO_ACCT_TYPE_G ACCT
        WHERE SCHD.ACCTG_TYPE = ACCT.ID
          AND schd.line_id    = p_line_id
          AND post_prd_id    <= p_prd_id   --LV20200401
          AND ACCT.NAME       = p_type;
    BEGIN

      FOR i IN c_amt
      LOOP
        l_amount := i.amount;

      END LOOP;

      RETURN l_amount;

    END rpro_splk_schd_amt;

  PROCEDURE RPRO_SPLK_MRG_RC(
        p_rc_id    IN NUMBER,
        p_batch_id IN NUMBER,
        p_new_rc_id OUT NUMBER)
    IS

      CURSOR c_rc_ids
      IS

        SELECT DISTINCT rc_id
          FROM rpro_rc_line_g
          WHERE atr3 IN
            (SELECT atr3 FROM rpro_rc_line_g WHERE rc_id = p_rc_id
            )
        ORDER BY rc_id;

      l_errbuf      VARCHAR2(2000);
      l_retcode     NUMBER;
      l_freeze_flag VARCHAR2(10);
      l_posted_flag VARCHAR2(10);
      l_obj_version rpro_rc_head_g.obj_version%type;
      l_book_id rpro_rc_head_g.book_id%type;
      l_pob_str VARCHAR2(500);
      l_rc_id rpro_rc_head_g.id%type;
      l_final_pob_str VARCHAR2(3500);
      l_tmpl_id rpro_rc_tmpl_g.id%type;
      l_new_rc_id rpro_rc_head_g.id%type;
      l_proc                 VARCHAR2(30):='RPRO_SPLK_MRG_RC';
      l_unfreeze_exception   EXCEPTION;
      l_delink_exception     EXCEPTION;
      l_createrc_exception   EXCEPTION;
      l_freezeflag_exception EXCEPTION;
      l_pob_str_exception    EXCEPTION;
      l_tmplid_exception     EXCEPTION;
      l_rc_exception         EXCEPTION;
      l_rc_count             NUMBER;
      l_err_msg              VARCHAR2(1000);
      l_log_lvl              NUMBER:=2;
    BEGIN
      l_errbuf        := NULL;
      l_retcode       := NULL;
      l_freeze_flag   := NULL;
      l_obj_version   := NULL;
      l_book_id       := NULL;
      l_pob_str       := NULL;
      l_final_pob_str := NULL;
      l_tmpl_id       := NULL;
      l_new_rc_id     := NULL;

      FOR i IN c_rc_ids
      LOOP

        IF i.rc_id > 0 THEN
          l_rc_id :=NULL;
          l_rc_id := i.rc_id;

          IF l_new_rc_id IS NULL THEN
            l_new_rc_id  := l_rc_id;

          END IF;
          BEGIN

            SELECT   --RPRO_RC_HEAD_PKG.GET_FREEZE_FLAG(a.indicators) freeze_flag,
                RPRO_RC_HEAD_PKG.GET_POSTED_FLAG(a.indicators) posted_flag,
                obj_version,
                book_id
              INTO --l_freeze_flag,
                l_posted_flag,
                l_obj_version,
                l_book_id
              FROM rpro_rc_head_g a
              WHERE id = l_rc_id;

          EXCEPTION

          WHEN OTHERS THEN
            raise l_freezeflag_exception;

          END;

          -- Un-Freezing all the frozen RCs : Start

          UPDATE rpro_rc_head_g
            SET indicators=rpro_rc_head_pkg.SET_ALLOC_TRTMT_FLAG(indicators,'P')
            WHERE ID      = l_rc_id;

      --    IF (l_freeze_flag = 'Y' OR l_posted_flag = 'Y') THEN
          IF l_posted_flag = 'Y' THEN

            l_errbuf       := NULL;
            l_retcode      := NULL;
            rpro_utility_pkg.record_log_act ( 'RPRO_SPLK_MRG_RC' ,'Inside Before Validate Stage handler:  ' ||'Calling rpro_rc_action_pkg.unfreeze_rc_wrapper for ' ||' RC_ID: ' || i.rc_id ||' Book ID: ' || l_book_id ||' Obj Version: ' || l_obj_version ,l_log_lvl );
            BEGIN
              rpro_rc_collect_pkg.unfreeze_rc_wrapper ( p_rc_id => l_rc_id ,p_obj_version => l_obj_version ,p_book_id => l_book_id ,p_ret_msg => l_errbuf ,p_lock_rc => 'N' );

            EXCEPTION

            WHEN OTHERS THEN
              rpro_utility_pkg.record_err_act ( 'RPRO_SPLK_MRG_RC' ,'Inside Before Validate Stage handler: ' ||'Exception : while calling rpro_rc_collect_pkg.unfreeze_rc_wrapper for the RC ID: ' || i.rc_id ||'. ' || SQLERRM );

            END;

            IF l_errbuf IS NOT NULL THEN
              rpro_utility_pkg.record_log_act ( 'RPRO_SPLK_MRG_RC' ,'Inside Before Validate Stage handler: ' ||'Exception while unfreezing the RC ' || i.rc_id ||' Book ID: ' || l_book_id ||' Obj Version: ' || l_obj_version ||'. Return Code is: ' || l_retcode ||' and Errbuf is: ' || l_errbuf ,l_log_lvl );
              raise l_unfreeze_exception;

            END IF;

          END IF;
          -- Un-Freezing all the frozen RCs : End

          IF l_new_rc_id    <> i.rc_id THEN
            l_final_pob_str := l_final_pob_str ||':-'||i.rc_id ;
            l_errbuf        := NULL;
            l_retcode       := NULL;

            SELECT   --RPRO_RC_HEAD_PKG.GET_FREEZE_FLAG(a.indicators) freeze_flag,
                RPRO_RC_HEAD_PKG.GET_POSTED_FLAG(a.indicators) posted_flag,
                obj_version,
                book_id
              INTO --l_freeze_flag,
                l_posted_flag,
                l_obj_version,
                l_book_id
              FROM rpro_rc_head_g a
              WHERE id = i.rc_id;

            rpro_rc_action_pkg.delink_rc_pob
            (p_doc_line_id_str => NULL ,
             p_rc_pob_id_str => NULL ,
             p_rc_id => i.rc_id ,
             p_book_id =>l_book_id ,
             p_object_version =>l_obj_version ,
             p_comments => 'Delink for Splk custom Contract mod' ,
             p_retcode =>l_retcode ,
             p_errbuf => l_errbuf ,
             p_lock_rc => 'Y' ,
             p_commit_flag => 'Y' ,
             p_summary_data => NULL );

          END IF;
        ELSE
          raise l_rc_exception;

        END IF;

      END LOOP;
      rpro_utility_pkg.record_log_act ( 'RPRO_SPLK_MRG_RC' ,'Inside RPRO_SPLK_MRG_RC: l_rc_str '||l_final_pob_str,l_log_lvl);

      IF l_final_pob_str IS NOT NULL THEN

        l_errbuf  := NULL;
        l_retcode := NULL;

        SELECT   --RPRO_RC_HEAD_PKG.GET_FREEZE_FLAG(a.indicators) freeze_flag,
            RPRO_RC_HEAD_PKG.GET_POSTED_FLAG(a.indicators) posted_flag,
            obj_version,
            book_id
          INTO --l_freeze_flag,
            l_posted_flag,
            l_obj_version,
            l_book_id
          FROM rpro_rc_head_g a
          WHERE id = l_new_rc_id;

        rpro_rc_action_pkg.LINK_RC_LEVEL (P_RC_ID_STR => ltrim(l_final_pob_str,':') ,P_LINKED_TO_RC_ID => l_new_rc_id ,P_BOOK_ID =>l_book_id ,P_OBJECT_VERSION =>l_obj_version ,P_COMMENTS => 'Link RC Splk Custom Contract mod' ,P_RETCODE =>l_retcode ,P_ERRBUF =>l_errbuf ) ;

      END IF;
      p_new_rc_id := l_new_rc_id;

    EXCEPTION

    WHEN l_unfreeze_exception THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'Unable to unFreeze Rc';

    WHEN l_delink_exception THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'Unable to Delink RC :'||l_rc_id;

    WHEN l_createrc_exception THEN
      p_new_rc_id := -1.1;
      NULL;

    WHEN l_freezeflag_exception THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'Unable to derive Freeze flag existing Rc';

    WHEN l_pob_str_exception THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'Unable to form POB strig for New Rc';

    WHEN l_tmplid_exception THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'No Active Grouping Templates';

    WHEN l_rc_exception THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'Existing POBs are delinked';

    WHEN OTHERS THEN
      p_new_rc_id := -1.1;
      l_err_msg   := 'Exception Rasied :'||sqlerrm;

    END;
  -- This Procedure is called from the pre/post processor After FV Calculation stage
  -- populates the level2 percentage based on the lookup for Splunk specific RSSP percentages
  -- Between Lincense and support which are identified as bundle

  PROCEDURE Merge_rc(
      p_errbuf OUT VARCHAR2,
      p_retcode OUT NUMBER,
      p_batch_id IN NUMBER,
      p_rc_id    IN NUMBER)
  IS

    CURSOR c_mrg_rc
    IS

      SELECT   *
        FROM rpro_rc_head_g
        WHERE batch_id     = NVL(p_batch_id,batch_id)
          AND id           = NVL(p_rc_id,id)
          AND id           > 0
          AND CRTD_PRD_ID IN
          (SELECT ID FROM RPRO_PERIOD_G WHERE STATUS = 'OPEN'
          )
        -- AND 1 = 2
      ORDER BY id DESC;

    l_line_cnt    NUMBER;
    l_stackid_cnt NUMBER;
    l_new_rc_id rpro_rc_line.rc_id%type;
    l_parameter_exception EXCEPTION;
    l_batch_id            NUMBER;
    l_period_name          rpro_calendar_g.period_name%type;
  BEGIN
    rpro_utility_pkg.set_revpro_context;
    rpro_utility_pkg.record_log_act ( 'MERGE_RC' ,'Entered Procedure' ,2 );

    begin

    select period_name into l_period_name from rpro_period_g a, rpro_calendar_g b
     where b.id = a.id;
    exception
    when others
    then
    rpro_utility_pkg.record_log_act ( 'MERGE_RC' ,'Entered Procedure' ,2 );
    l_period_name := null;
    end;
    IF p_batch_id IS NULL THEN
      BEGIN

        SELECT MAX(id) INTO l_batch_id FROM rpro_rc_batch_g;

      EXCEPTION

      WHEN OTHERS THEN
        l_batch_id := NULL;

      END;

    ELSE
      l_batch_id := p_batch_id;

    END IF;

    IF l_batch_id IS NULL AND p_rc_id IS NULL THEN
      rpro_utility_pkg.record_log_act ( 'MERGE_RC' ,'Pass atleast one parameter. Exiting the Proc' ,4 );
      raise l_parameter_exception;

    END IF;

    FOR i IN c_mrg_rc
    LOOP

      SELECT   COUNT(*)
        INTO l_stackid_cnt
        FROM rpro_rc_line_g
        WHERE atr3 IN
          (SELECT atr3 FROM rpro_rc_line_g WHERE rc_id = i.id
          )
          AND rc_id <> i.id;

      IF l_stackid_cnt > 0 THEN

        SELECT   COUNT(*)
          INTO l_line_cnt
          FROM rpro_rc_line_g
          WHERE rpro_rc_line_pkg.get_cv_eligible_flag(indicators)                           = 'Y'
           and num1 is not null
           -- To handle single line RC's. Added in UAT2
           AND ((alctbl_xt_prc < num1*term*ord_qty*.8) or (alctbl_xt_prc > num1*term*ord_qty*1.2) )
           --Removed in UAT2
          --  DECODE(num1,NULL,'Y',RPRO_RC_LINE_PKG.GET_WITHIN_FV_RANGE_FLAG(indicators)) = 'N'
            AND rc_id = i.id;

        IF l_line_cnt > 0 THEN
          rpro_splk_mrg_rc ( i.id, i.batch_id, l_new_rc_id);

          UPDATE rpro_rc_line_g SET ATR2 = 'CONTRACT MOD' ,
                 ATR42 = L_PERIOD_NAME
        WHERE rc_id = l_new_rc_id;

        ELSE

          UPDATE rpro_rc_line_g SET atr2 = 'WITH IN RANGE' WHERE RC_ID = I.ID;

        END IF;

      END IF;

    END LOOP;

  EXCEPTION

  WHEN l_parameter_exception THEN
    rpro_utility_pkg.record_log_act ( 'MERGE_RC' ,'PARAMETER EXCEPTION RAISED' ,4 );

  WHEN OTHERS THEN
    NULL;

  END;

  PROCEDURE SECOND_LVL_ALLOCATION(
      p_rc_id    IN VARCHAR2 DEFAULT NULL,
      p_batch_id IN NUMBER )
  AS
    /**************************************************************************************************
    * Name : SECOND_LVL_ALLOCATION *
    * Author : LEEYO *
    * Date   : 10-Jan-2017 *
    * Description : *
    * Version : *
    * $Header: SECOND_LVL_ALLOCATION 1 2017-03-10                                          admin $ *
    * Modifications History: *
    * *
    * Modified By           Date        Version     Description *
    * --------------      ----------- ------------  -----------------------------------------*
    Leeyo                 10-Mar-2017   1.0         This logic enables the lvl2 allocaiton flag to 'Y'
    if lvl2 identifier Num3 is populated part of
    before validate
    For two line bundle revenue share should be following
    License = 1/1.2
    Support = 1-1/1.2
    For three line bundle is as following
    License = 1/1.4
    Support = (1-1/1.4)*.2
    Content Subscription = (1-1/1.4)*.2
    For two line bundle with premium support the revenue share should be following
    License = .8
    Support = 1-.8
    If this is not working make sure the sum of this lvl2_alloc_pct
    should be zero
    **************************************************************************************************/
    l_num_l_index        NUMBER;
    l_num_fv_index       NUMBER;
    l_num_reset_allc     NUMBER;
    l_item_index         rpro_rc_line_g.atr41%TYPE := NULL;
    l_2_line_pct         NUMBER;
    l_3_line_pct         NUMBER;
    l_l2_premium_pct     NUMBER;
    l_lkp_license        NUMBER                    := 0;                 --#03192019
    l_lkp_main           NUMBER                    := 0;                 --#03192019
    l_lkp_sub            NUMBER                    := 0;                 --#03192019
    l_sum_val_siem       NUMBER                    := 0;                 --#03192019
    l_temp_dervation     NUMBER                    := 0;                 --#01132020
    l_rounding_off       NUMBER                    := 10;                --#13022020 20200415
  TYPE item_count_rec
IS
  RECORD
  (
    item_count NUMBER ,
    bundle_id  VARCHAR2(240));
TYPE item_count_tab
IS
  TABLE OF item_count_rec INDEX BY VARCHAR2(250);
  l_item_count_tab item_count_tab;
  l_pct_missing_error EXCEPTION;
BEGIN
   rpro_utility_pkg.set_revpro_context;

   rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'INSIDE SECOND_LVL_ALLOCATION :  ' ||' RC ID: ' || p_rc_id ,4 );
  BEGIN

    -- Derive split Bundle percentages from lookups
    --13022020
    SELECT ROUND(to_number(rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', '2-LINE-BUNDLE')),l_rounding_off), --LV20200401
           ROUND(to_number(rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', '3-LINE-BUNDLE')),l_rounding_off),
           ROUND(to_number(rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', '2-LINE-PREMIUM-BNDL')),l_rounding_off)
    INTO   l_2_line_pct,
           l_3_line_pct,
           l_l2_premium_pct
    FROM DUAL;

    IF l_2_line_pct IS NULL OR l_3_line_pct IS NULL
    THEN
       RAISE l_pct_missing_error;
    END IF;
  EXCEPTION

  WHEN OTHERS THEN
    raise l_pct_missing_error;

  END;

  BEGIN                                           --#03192019
      SELECT ROUND(lkpv.lookup_value,l_rounding_off)  --13022020 LV20200401
      INTO   l_lkp_license
      FROM   rpro_lkp     lkp
            ,rpro_lkp_val lkpv
      WHERE  UPPER(name)        = 'SPLK LVL2 PERCENTAGE'
      AND    lkp.id             = lkpv.lookup_id
      AND    UPPER(lookup_code) = 'LICENSE - TERM'
      AND    UPPER(sub_group1)  = 'SIEM';
   EXCEPTION
   WHEN OTHERS THEN
      write_error(   'Exception in lookup value for licence : '||SQLERRM
                                                               ||CHR(10)
                                                               ||'Error Queue : '||dbms_utility.format_error_backtrace);

   END;                                --#03192019

   BEGIN                                           --#03192019
      SELECT ROUND(lkpv.lookup_value,l_rounding_off)  --13022020 LV20200401
      INTO   l_lkp_main
      FROM   rpro_lkp     lkp
            ,rpro_lkp_val lkpv
      WHERE  UPPER(name)        = 'SPLK LVL2 PERCENTAGE'
      AND    lkp.id             = lkpv.lookup_id
      AND    UPPER(lookup_code) = 'MAINTENANCE - NEW'
      AND    UPPER(sub_group1)  = 'SIEM';
   EXCEPTION
   WHEN OTHERS THEN
      write_error(   'Exception in lookup value for maintenance - new : '||SQLERRM
                                                                         ||CHR(10)
                                                                         ||'Error Queue : '||dbms_utility.format_error_backtrace);

   END;                                --#03192019

   BEGIN                                           --#03192019
      SELECT ROUND(lkpv.lookup_value,l_rounding_off)  --13022020 LV20200401
      INTO   l_lkp_sub
      FROM   rpro_lkp     lkp
            ,rpro_lkp_val lkpv
      WHERE  UPPER(name)        = 'SPLK LVL2 PERCENTAGE'
      AND    lkp.id             = lkpv.lookup_id
      AND    UPPER(lookup_code) = 'CONTENT SUBSCRIPTION'
      AND    UPPER(sub_group1)  = 'SIEM';
   EXCEPTION
   WHEN OTHERS THEN
      write_error(   'Exception in lookup value for content subscription : '||SQLERRM
                                                                         ||CHR(10)
                                                                         ||'Error Queue : '||dbms_utility.format_error_backtrace);

   END;                                --#03192019

   l_sum_val_siem := NVL(l_lkp_license,0) + NVL(l_lkp_main,0) + NVL(l_lkp_sub,0); --#03192019

rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || 'Batch ID: ' || p_batch_id || ' RC ID: ' || p_rc_id||' : l_sum_val_siem: '||l_sum_val_siem ,4 );
-- first loop  to reset  the 2nd  level allocation
-- Fetching the first index in RC Line data
---l_num_reset_allc := rpro_rc_collect_pkg.g_rc_line_data.FIRST;
---
---WHILE l_num_reset_allc IS NOT NULL
---LOOP
---  rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).indicators,'N');
---  rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).lvl2_alloc_pct := NULL;
--- --- rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).num7           := NULL;
---  l_num_reset_allc                                                     := rpro_rc_collect_pkg.g_rc_line_data.NEXT (l_num_reset_allc);
---
---END LOOP;
-- end  of first loop
-- Second loop to count the eligiblie records ( RSSP and bundle id)
-- Fetching the first index in RC Line data
l_num_fv_index := rpro_rc_collect_pkg.g_rc_line_data.FIRST;
/* The below WHILE Loop finds the total count of each non SSP type SKUs*/

WHILE l_num_fv_index IS NOT NULL
LOOP
  rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'INSIDE MULTIPLE BUNDLE :  ' ||' RC ID: ' || p_rc_id ,4 );

  IF 1 = 1
    --rpro_rc_collect_pkg.g_rc_line_data (l_num_fv_index).fv_type = 'RSSP'
    -- commented above on 1262017 to accomidate the combination
    -- refere to combinations in BRD
    AND rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_fv_index).indicators) = 'Y'
    AND rpro_rc_collect_pkg.g_rc_line_data (l_num_fv_index).atr41 IS NOT NULL
    AND rpro_rc_collect_pkg.g_rc_line_data (l_num_fv_index).atr40 = 'Y'
    THEN
    rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'INSIDE MULTIPLE BUNDLE  FROM IF CONDITION:  ' ||' RC ID: ' || p_rc_id ,4 );
    l_item_index := rpro_rc_collect_pkg.g_rc_line_data (l_num_fv_index).atr41;

    IF NOT l_item_count_tab.EXISTS(l_item_index) THEN
      rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'INSIDE MULTIPLE BUNDLE NOT EXIST:  ' ,4 );
      -- variable to track total item count
      l_item_count_tab(l_item_index).item_count := 1;

    ELSE
      l_item_count_tab(l_item_index).item_count := l_item_count_tab(l_item_index).item_count + 1;
      l_item_count_tab(l_item_index).bundle_id  := rpro_rc_collect_pkg.g_rc_line_data (l_num_fv_index).atr41;
      --bundle id
      rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'INSIDE MULTIPLE BUNDLE COUNT:  ' || ' COUNT: ' || l_item_count_tab(l_item_index).item_count ||'atr41 '|| l_item_count_tab(l_item_index).bundle_id || ' RC ID: ' || p_rc_id ,4 );

    END IF;
    --  IF NOT item_count_tab.EXISTS(l_item_index)

  END IF;
--  IF NOT item_count_tab.EXISTS(l_item_index)
l_num_fv_index := rpro_rc_collect_pkg.g_rc_line_data.NEXT (l_num_fv_index);

END LOOP;
-- WHILE l_num_l_index IS NOT NULL
-- end  of  loop to count the eligiblie records ( RSSP and bundle id)
-- Third loop to assign the 2nd level flag and 2nd level allocation percentage for eligible records

IF l_item_count_tab.COUNT > 0 THEN
  l_item_index           := NULL;
  -- Fetching the first index of the non-SSP item lines
  l_item_index := l_item_count_tab.FIRST;

  WHILE l_item_index IS NOT NULL
  LOOP
    l_num_l_index := NULL;
    l_temp_dervation := 0;
    -- Fetching the first index in RC Line data
    l_num_l_index := rpro_rc_collect_pkg.g_rc_line_data.FIRST;

    WHILE l_num_l_index IS NOT NULL
    LOOP
      rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL Main loop:  ' || ' Maintence new  atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).rc_id || ' RC ID: ' || p_rc_id ,4 );
      rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'l_item_count_tab(l_item_index).item_count:  '||l_item_count_tab(l_item_index).item_count
                                       ||' Prod ctgry :' ||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry
                                       ,4 );
      -- AND rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).fv_type = 'RSSP'
      -- commented above on 1262017 to accomidate the combination
      -- refere to combinations in BRD
      -- Following is for combination of License and maintainence  with Split Flag(atr40) ='Y'
      -- Parent License id(atr41) is license sales order line id to find the link
      -- This logic is for count two

      IF rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators) = 'Y'
      AND rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41 IS NOT NULL
      AND ( l_item_count_tab(l_item_index).item_count = 2 -- Both License and Maintenance count is 2 where parent License id is license sales order line id
      AND rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41     = l_item_count_tab(l_item_index).bundle_id )
      THEN
        rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'INSIDE ALLCT_SCND_LEVEL Main loop: and if condition  ' || ' Maintence new  atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).rc_id || ' RC ID: ' || p_rc_id||' Bundle id ' || l_item_count_tab(l_item_index).bundle_id || ' count ' ||l_item_count_tab(l_item_index).item_count ,4 );

        IF rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry LIKE 'License%' THEN
          -- Setting the Level 2 allocation Indicator to Y
          rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
          IF  NVL(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr48,'N') = 'N'
          THEN
             -- Allocate total allocated amount between the bundle based on
             -- LVL2 identifier atr41
             -- License is 1/l_2_line_pct

             rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := ROUND(100/(l_2_line_pct),l_rounding_off);--ROUND(100/(l_license_pct),7) ;--13022020 LV20200401
             rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Licence Term atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41
                                             ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                             || ' RC ID: ' || p_rc_id ,4 );
          ELSIF NVL(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr48,'N') = 'Y'
          THEN
             -- Allocate total allocated amount between the bundle based on
             -- LVL2 identifier atr41
             -- License is l_l2_premium_pct
             rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := l_l2_premium_pct;--ROUND(100/(l_license_pct),7) ;
             rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Licence Term atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41
                                             ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                             || ' RC ID: ' || p_rc_id ,4 );
          END IF;
        ELSIF rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry = 'Maintenance - New' THEN
          -- Setting the Level 2 allocation Indicator to Y
          rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
          IF  NVL(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr48,'N') = 'N'
          THEN
             -- Allocate total allocated amount between the bundle based on
             -- LVL2 identifier atr41
             -- Mantainance is 1-1/l_2_line_pct
         --13022020  LV20200401
             rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := ROUND(100-100/(l_2_line_pct),l_rounding_off);--100 - (ROUND(100/(l_license_pct),7));
             -- (100-83.33);
             rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Maintence new  NUM3: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).NUM3
                                             ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                             || ' RC ID: ' || p_rc_id ,4 );
          ELSIF NVL(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr48,'N') = 'Y'
          THEN
             -- Allocate total allocated amount between the bundle based on
             -- LVL2 identifier atr41
             -- Mantainance is 1-l_l2_premium_pct
         --13022020  LV20200401
             rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := ROUND(100-l_l2_premium_pct,l_rounding_off);--100 - (ROUND(100/(l_license_pct),7));
             -- (100-83.33);
             rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Maintence new  NUM3: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).NUM3
                                             ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                             || ' RC ID: ' || p_rc_id ,4 );
          END IF;
        ELSE
          NULL;

        END IF;
        --  IF NOT item_count_tab.EXISTS(l_item_index)

      END IF;
      -- AND rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).fv_type = 'RSSP'
      -- commented above on 1262017 to accomidate the combination
      -- refere to combinations in BRD
      -- Following is for combination of License and maintainence  with Split Flag(atr40) ='Y'
      -- Parent License id(atr41) is license sales order line id to find the link
      -- This logic is for count two
      --l_temp_dervation := 0;
      IF  rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators) = 'Y'
      AND rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41 IS NOT NULL
      AND ( l_item_count_tab(l_item_index).item_count                                                          = 3
      AND rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41                                             = l_item_count_tab(l_item_index).bundle_id )
      THEN
         IF   NVL(UPPER(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr43),'X')= 'SIEM'              --#03192019
         AND  l_sum_val_siem  = 100
         THEN
            IF UPPER(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry) = 'LICENSE - TERM'
            THEN
               write_log ('LICENSE - TERM PCT ~ ' || l_lkp_license);
               rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
               rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := l_lkp_license;
            ELSIF UPPER(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry) = 'MAINTENANCE - NEW'
            THEN
               write_log ('MAINTENANCE - NEW PCT ~ ' || l_lkp_main);
               rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
               rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := l_lkp_main;
            ELSIF UPPER(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry) = 'CONTENT SUBSCRIPTION'
            THEN
               write_log ('CONTENT SUBSCRIPTION PCT ~ ' || l_lkp_sub);
               rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
               rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := l_lkp_sub;
            END IF;                                     --#03192019
         ELSE
            IF rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry LIKE 'License%' THEN
              -- Setting the Level 2 allocation Indicator to Y
              rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
              l_temp_dervation := ROUND(100/(l_3_line_pct),l_rounding_off); --13022020  LV20200401
              rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := ROUND(100/(l_3_line_pct),l_rounding_off);--ROUND(100/(l_license_pct2),7) ;  --LV20200401
              -- Allocate total allocated amount between the bundle based on
              -- LVL2 identifier = atr41
              -- License is 1/l_3_line_pct
              rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Licence Term atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41
                                              ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                              ||'~l_temp_dervation~'||l_temp_dervation
                                              || ' RC ID: ' || p_rc_id ,4 );

            ELSIF rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry = 'Maintenance - New' THEN
              -- Setting the Level 2 allocation Indicator to Y
              rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
              -- Allocate total allocated amount between the bundle based on
              -- LVL2 identifier = atr41
              -- Maintenance is (1-1/l_3_line_pct )/2
          --13022020 LV20200401
              l_temp_dervation := l_temp_dervation + ROUND((100 - (100/(l_3_line_pct)))/2,l_rounding_off) ;
              rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := ROUND((100 - (100/(l_3_line_pct)))/2,l_rounding_off);--round((100 - (100/(l_license_pct2)))/2,7);
              rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Maintence new  atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41
                                              ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                              ||'~l_temp_dervation~'||l_temp_dervation
                                              || ' RC ID: ' || p_rc_id ,4 );

            ELSIF rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).prod_ctgry = 'Content Subscription' THEN
              -- Setting the Level 2 allocation Indicator to Y
              rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'Y');
              -- Allocate total allocated amount between the bundle based on
              -- LVL2 identifier = atr41
              -- Content Subscription is (1-1/l_3_line_pct )/2
          --13022020
              rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Maintence new1  atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41
                                              ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                              ||'~l_temp_dervation~'||l_temp_dervation
                                              || ' RC ID: ' || p_rc_id ,4 );
              l_temp_dervation :=ROUND(100/(l_3_line_pct),l_rounding_off)+ROUND((100 - (100/(l_3_line_pct)))/2,l_rounding_off);  --16012020 LV20200401
              l_temp_dervation := ROUND(100 - l_temp_dervation,l_rounding_off);
              rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct := l_temp_dervation;--round((100 - (100/(l_license_pct2)))/2,7);
              rpro_utility_pkg.record_log_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL:  ' || ' Maintence new  atr41: ' || rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).atr41
                                              ||'Lvl2 alloc pct : '||rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).lvl2_alloc_pct
                                              ||'~l_temp_dervation~'||l_temp_dervation
                                              || ' RC ID: ' || p_rc_id ,4 );

            END IF;
         END IF;
      END IF;
      rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators := rpro_rc_line_pkg.set_update_or_insert_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_l_index).indicators,'U');
      l_num_l_index                                                 := rpro_rc_collect_pkg.g_rc_line_data.NEXT (l_num_l_index);

    END LOOP;
    -- WHILE l_num_l_index IS NOT NULL
    l_item_index := l_item_count_tab.NEXT (l_item_index);

  END LOOP;
  -- WHILE l_num_l_index IS NOT NULL

END IF;
-- IF item_count_tab.COUNT > 0
-- End of Third loop to assign the 2nd level flag and 2nd level allocation percentage for eligible records
   rpro_lvl2_pct_drv ( p_rc_id      => p_rc_id
                      ,p_batch_id   => p_batch_id );
EXCEPTION

WHEN l_pct_missing_error THEN
  rpro_utility_pkg.record_err_act ( 'ALLCT_SCND_LEVEL' ,'Check lookup value in SPLK LVL2 PERCENTAGE');
  rpro_utility_pkg.record_err_act ( 'ALLCT_SCND_LEVEL' ,'Unable to assign LVL2_PCT');

WHEN OTHERS THEN
  rpro_utility_pkg.record_err_act ( 'ALLCT_SCND_LEVEL' ,'Inside ALLCT_SCND_LEVEL Exception:  ' || SQLERRM ||' ERROR QUEUE : '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

END second_lvl_allocation;
-- This procedure is called from rc collection, before validation stage
-- This procedure does following
-- 1. Populate atr41 (lvl2 identifier) for 2nd step allocaiton/rev share
-- lvl2 identifier for 2nd step allocation
-- bundles's that dont need to go thru the 2nd stage allocation will not be populated
-- lvl2 identifier
-- 2. populate sales order info for rebill invoice based on opp id and other attributes
-- 3. Populate unbill flag for all the lines by default as 'Y' for 606

PROCEDURE before_validate(
    p_rc_id    VARCHAR2 ,
    p_batch_id NUMBER )
AS


  CURSOR c_stg
  IS
    SELECT a.rowid row_id,a.* FROM RPRO_LINE_STG_G a;

  TYPE l_stage_tab
   IS
     TABLE OF c_stg%ROWTYPE;
   L_SO                     VARCHAR2(240);
   L_SO_ID                  VARCHAR2(240);
   L_SO_LINE_ID             VARCHAR2(240);
   L_PROCESS_LOG            VARCHAR2 (1);
   L_PROC_NAME              VARCHAR2(30) := 'BEFORE_VALIDATE' ;
   L_LOG_ID                 NUMBER;
   L_LOG_LEVEL              NUMBER;
   L_STATUS                 BOOLEAN;
   l_line_amt               NUMBER;
   l_stack_id_exists        NUMBER;
   l_stage_table l_stage_tab ;
   l_outofrange_line_exists NUMBER;
   l_rssp_lines_exists      NUMBER;
   --  l_unique_id              VARCHAR2(50);
   --  l_new_rc_id rpro_rc_line_g.rc_id%type;
   l_err_msg                VARCHAR2(3500);
   /*                                                 --20200506
   l_unbill_flag            VARCHAR2(250);

  --- Declaration for unbill liability account ----LV20200401
   CURSOR cur_stg_unbill IS
   SELECT ls.rowid row_id, ls.*
   FROM   rpro_line_stg_g ls
   WHERE  1 = 1
   ----AND   ls.unbill_flag = 'Y'
   AND   NVL(ls.processed_flag, 'N') = 'N';

   TYPE typ_stg_ub IS TABLE OF cur_stg_unbill%ROWTYPE;
   l_stg_ub        typ_stg_ub;

   TYPE typ_lkp_vals IS TABLE of rpro_lkp_val_g%ROWTYPE INDEX BY PLS_INTEGER;
   l_lkp_vals      typ_lkp_vals;

   TYPE r_lkp_val IS RECORD (
     lkp_code                rpro_lkp_val_g.lookup_code%TYPE,
     lkp_value               rpro_lkp_val_g.lookup_value%TYPE);

   TYPE typ_lkp_val IS TABLE OF r_lkp_val INDEX BY VARCHAR2(240);
   l_lkp_val      typ_lkp_val;

   l_ub_acc     rpro_rc_line_g.ub_liab_acct%TYPE;
   */
BEGIN
  --  SELECT TO_CHAR(sysdate,'mmddyyyyhhmmss') INTO l_unique_id FROM dual;
  rpro_utility_pkg.record_err_act ( l_proc_name ,'start populate bundle id :  ' || 'p_rc_id ' || p_rc_id || ' p_batch_id ' || p_batch_id );
  -- Disable allocation on PS lines where event requried is Y and PS Upfront billing is N
   OPEN c_stg;

  FETCH c_stg BULK COLLECT INTO l_stage_table;

  CLOSE c_stg;

  FOR i IN 1..l_stage_table.count
  LOOP
    -- Mark all lines that are coming into system as unbill eligible
    l_stage_table(i).unbill_flag := 'Y';
    l_stage_table(i).return_flag := null;
    -- Disable allocation on lines where event requried is Y and ps upfront billing is 'N'
    -- Allocation will be disabled for all conversion lines

    IF l_stage_table(i).atr29    = 'Y' -- Event Required
      AND l_stage_table(i).atr23 = 'N' -- PS upfront Billing
      THEN
      l_stage_table(i).cv_eligible_flag := 'N';

    END IF;
    -- Populating split flag for revshare or level 2 allocation

    IF NVL(l_stage_table(i).atr40,'X') <> 'Y' THEN
      l_stage_table(i).atr40           := 'N';

    END IF;
    -- Pouplate parent line where its null and split flag is 'Y'

    IF NVL(l_stage_table(i).ATR9,'N') ='N' AND l_stage_table(i).atr40 = 'Y' AND trim(upper(l_stage_table(i).PROD_CTGRY)) = 'LICENSE - TERM' THEN
      l_stage_table(i).atr41         := NVL(l_stage_table(i).atr41,l_stage_table(i).so_line_id);

    END IF;

    IF NVL(l_stage_table(i).atr46,'N') ='Y' AND
      TRIM(upper(l_stage_table(i).PROD_CTGRY)) IN ( 'LICENSE - TERM','SUBSCRIPTION')
    THEN
      l_stage_table(i).atr41         := NVL(l_stage_table(i).atr41,l_stage_table(i).so_line_id);
    END IF;
    -- Rebill invoice customization started

    IF l_stage_table(i).type = 'INV' AND NVL (l_stage_table(i).atr36,'N') = 'Y'
      -- credit rebil order flag
      AND NVL(l_stage_table(i).atr60, 'N') = 'N'
      -- converstion flag
      THEN
      -- Derive original sales order line id based on opportunity id and opportunity line id and original sales order info
      -- if not found then check in stage table as it might be in the same batch
      BEGIN

        SELECT   doc_num ,
            doc_line_id
          INTO l_so,
            l_so_line_id
          FROM rpro_rc_line_g
          WHERE doc_num = l_stage_table(i).atr37  --Original sales order number
            AND atr18   = l_stage_table(i).atr18
            AND atr19   = l_stage_table(i).atr19
            AND TYPE    = 'SO';

        rpro_utility_pkg.record_log_act (p_type => l_proc_name ,p_text =>'step2'|| l_so_line_id ,p_log_level => 3 );

      EXCEPTION

      WHEN NO_DATA_FOUND THEN
        BEGIN

          SELECT   so_num ,
              so_line_id
            INTO l_so,
              l_so_line_id
            FROM rpro_line_stg_g
            WHERE so_num = l_stage_table(i).atr37  --Original sales order number
              AND atr18  = l_stage_table(i).atr18
              AND atr19  = l_stage_table(i).atr19
              AND type   = 'SO';
          --          AND NVL(atr60, 'N')  = 'N'
          --          AND cv_eligible_flag = 'Y';
          rpro_utility_pkg.record_log_act (p_type => l_proc_name ,p_text => 'step3'|| l_so_line_id ,p_log_level => 3 );

        EXCEPTION

        WHEN OTHERS THEN
          rpro_utility_pkg.record_log_act (p_type => l_proc_name ,p_text => 'step4'|| sqlerrm(SQLCODE) ,p_log_level => 3 );
          l_so         := NULL;
          l_so_line_id := NULL;

        END;

      WHEN OTHERS THEN
        rpro_utility_pkg.record_log_act (p_type => l_proc_name ,p_text => 'step5'|| sqlerrm(SQLCODE) ,p_log_level => 3 );
        l_so         := NULL;
        l_so_line_id := NULL;

      END;

      IF l_so_line_id IS NOT NULL THEN
        rpro_utility_pkg.record_log_act (p_type => l_proc_name ,p_text => 'so~so line id ~ so id '||l_so||'~'||l_so_line_id||'~'||l_so_line_id ,p_log_level => 3 );
        l_stage_table(i).atr50      :=l_stage_table(i).so_num;
        l_stage_table(i).atr52      := l_stage_table(i).so_line_id;
        l_stage_table(i).so_num     := l_so;
        l_stage_table(i).so_line_id := l_so_line_id;

      END IF;
      -- Rebill inovice need to be stopped if the line has not been credited using the CM-C
      -- or CM-C might be in the same batch hence stop the rebill inovice
      -- The stuck rebill invoice will be collected in the next batch.
   IF l_stage_table(i).type = 'INV'
   then
      BEGIN

        SELECT   SUM(ext_sll_prc-(bld_def_amt+bld_rec_amt))
          INTO l_line_amt
          FROM rpro_rc_line_g
          WHERE doc_line_id = l_stage_table(i).so_line_id
            AND doc_num     = l_stage_table(i).so_num ;

      EXCEPTION

      WHEN OTHERS THEN
        l_line_amt := 0;

      END;
      -- The following field will be used in data augumentation to stop the line or mark as error.

      IF l_line_amt            < l_stage_table(i).ext_sll_prc THEN
        l_stage_table(i).atr4 := 'REBILL_CHECK';

      ELSE
        l_stage_table(i).atr4 := NULL;

      END IF;
   END IF;

    END IF;

  END LOOP;
  RPRO_UTILITY_PKG.RECORD_LOG_ACT (P_TYPE => l_proc_name ,P_TEXT => 'End Credit Rebill Customization ' ,P_LOG_LEVEL => 3 );
  -- end of the code for Credit Rebill  customization
  --Final update on the attributes as part of the before validate.
  forall i IN 1..l_stage_table.count

  UPDATE rpro_line_stg_g
    SET unbill_flag    = l_stage_table(i).unbill_flag,
      cv_eligible_flag = l_stage_table(i).cv_eligible_flag,
   --   atr58            = l_stage_table(i).cv_eligible_flag,
   --   atr57            = l_stage_table(i).cv_eligible_flag,
      atr5             = l_stage_table(i).atr5,
      atr40            = l_stage_table(i).atr40,
      atr41            = l_stage_table(i).atr41,
      atr50            = l_stage_table(i).atr50,
      atr52            = l_stage_table(i).atr52,
      so_num           = l_stage_table(i).so_num,
      so_line_id       = l_stage_table(i).so_line_id,
      atr4             = l_stage_table(i).atr4,
      atr1             = l_stage_table(i).so_line_id,
      return_flag      = l_stage_table(i).return_flag,
      processed_flag   = 'N',
      err_msg          = NULL
    WHERE rowid        = l_stage_table(i).row_id;
  COMMIT;

   FOR r_rec IN (SELECT distinct atr41
                 FROM   rpro_line_stg_g rls
                 WHERE  atr40 = 'Y'   ----split flag
                 AND    atr48 = 'Y'
                 AND    type  = 'SO')  ---premium flag
   LOOP
      ---update premius flag to Y for License - Term to use the same
      ---in second level allocation.
      UPDATE rpro_line_stg_g
      SET    atr48  = 'Y'
      WHERE  atr41  = r_rec.atr41
      AND    prod_ctgry = 'License - Term'
      AND    type    = 'SO';
   END LOOP;

   UPDATE rpro_line_stg_g a
   SET   -- cv_eligible_flag  = 'N'
         --,atr57             = 'N'
          num1              =  0
   WHERE  so_line_id       !=  atr41
   AND    atr46             =  'Y'
   AND    (   UPPER(prod_ctgry) LIKE '%SERVICES%'
           OR UPPER(prod_ctgry) LIKE '%TRAINING%')    --LV20200910
   AND    num1              IS NOT NULL
   AND    a.type            = 'SO'
   AND   EXISTS (SELECT 1
                 FROM   rpro_line_stg_g b
                 WHERE  b.so_line_id = b.atr41
                 AND    b.atr46      = 'Y'
                 AND    a.atr41      = b.atr41
                 AND    b.num1       IS NOT NULL
                 AND    a.so_num     = b.so_num
                 AND    a.type       = b.type );

    COMMIT;
   --LV20200401
   /*
   RPRO_UTILITY_PKG.RECORD_LOG_ACT (P_TYPE => l_proc_name ,P_TEXT => 'Start Unbill Acct' || TO_CHAR ( SYSDATE, 'DD-MON-YYYY HH24:MI:SS') ,P_LOG_LEVEL => 2 );

   SELECT *
   BULK COLLECT INTO l_lkp_vals
   FROM   rpro_lkp_val lv
   WHERE EXISTS ( SELECT 1
                    FROM rpro_lkp l
                   WHERE l.name = 'SPLK_UNBILL_ACC'
                     AND l.id = lv.lookup_id);

   FOR i IN 1..l_lkp_vals.COUNT
   LOOP
      IF NOT l_lkp_val.EXISTS (UPPER(l_lkp_vals(i).lookup_code))
      THEN
         l_lkp_val(UPPER(l_lkp_vals(i).lookup_code)).lkp_code        := UPPER(l_lkp_vals(i).lookup_code);
         l_lkp_val(UPPER(l_lkp_vals(i).lookup_code)).lkp_value       := l_lkp_vals(i).lookup_value;
      END IF;
   END LOOP;

   COMMIT;

   OPEN  cur_stg_unbill;
   FETCH cur_stg_unbill BULK COLLECT INTO l_stg_ub;
   CLOSE cur_stg_unbill;

   FOR i IN 1..l_stg_ub.count
   LOOP
      IF l_lkp_val.EXISTS(NVL(UPPER(TRIM(l_stg_ub(i).prod_ctgry)), '@'))
      THEN
         l_stg_ub(i).ub_liab_acct := l_lkp_val(NVL(UPPER(TRIM(l_stg_ub(i).prod_ctgry)), '@')).lkp_value;
         RPRO_UTILITY_PKG.RECORD_LOG_ACT (P_TYPE => l_proc_name ,P_TEXT => 'Stage prod_ctgry : ' || l_stg_ub(i).prod_ctgry || '~~ ub liab acct : ' || l_stg_ub(i).ub_liab_acct ,P_LOG_LEVEL => 2 );
      ELSE
         l_stg_ub(i).ub_liab_acct := '1101';
         RPRO_UTILITY_PKG.RECORD_LOG_ACT (P_TYPE => l_proc_name ,P_TEXT => 'No prod_ctgry : 1101' || l_stg_ub(i).prod_ctgry ,P_LOG_LEVEL => 2 );
      END IF;

      BEGIN
         SELECT rl.ub_liab_acct
               ,rpro_rc_line_pkg.get_unbill_flag(rl.indicators) unbill_flag
         INTO   l_ub_acc
               ,l_unbill_flag
         FROM   rpro_rc_line_g rl
         WHERE  rl.doc_line_id = l_stg_ub(i).so_line_id
         AND    ROWNUM = 1;
      EXCEPTION
      WHEN OTHERS THEN
         l_ub_acc       := NULL;
         l_unbill_flag  := NULL;
      END;
      RPRO_UTILITY_PKG.RECORD_LOG_ACT (P_TYPE => l_proc_name ,P_TEXT => 'l_ub_acc: ' || l_ub_acc ,P_LOG_LEVEL => 2 );

      IF l_ub_acc IS NOT NULL
      THEN
         l_stg_ub(i).ub_liab_acct :=  l_ub_acc;
         l_stg_ub(i).unbill_flag  :=  l_unbill_flag;
      END IF;

      IF  l_stg_ub(i).prod_ctgry = 'MARKETING' AND  l_unbill_flag IS NULL
      THEN
         l_stg_ub(i).unbill_flag :=  'N';
      END IF;

      UPDATE rpro_line_stg_g
      SET    ub_liab_acct    = l_stg_ub(i).ub_liab_acct
            ,unbill_flag     = l_stg_ub(i).unbill_flag
      WHERE  ROWID           = l_stg_ub(i).row_id;
   END LOOP;
   COMMIT;

   UPDATE rpro_line_stg_g                   --#20191112
   SET    prod_class      = 'Core : Core'
   WHERE  prod_class IS NULL;               --#20191112

   COMMIT;
   RPRO_UTILITY_PKG.RECORD_LOG_ACT (P_TYPE => l_proc_name ,P_TEXT => 'End Unbill Acct' || TO_CHAR ( SYSDATE, 'DD-MON-YYYY HH24:MI:SS') ,P_LOG_LEVEL => 2 );
   */
EXCEPTION

WHEN OTHERS THEN
  rpro_utility_pkg.record_err_act (l_proc_name,'Inside BEFORE_VALIDATE,From others Exception:  ' || SQLERRM );

END before_validate;
-- This procedure used to populate net suite account id's
-- These ids will be stored in cust ui tables and will be
-- populated using integration
--********************-- This procedure New and used for 606***************************************

PROCEDURE rpro_gl_batch_split_by_rc(p_rc_id    IN VARCHAR2 DEFAULT NULL
                                   ,p_batch_id IN NUMBER)
   IS
      /**************************************************************************************************
      * Name : rpro_gl_batch_split_by_rc *
      * Author : LEEYO *
      * Date   : 10-Jan-2017 *
      * Description : *
      * Version : *
      * $Header: rpro_gl_batch_split_by_rc 1 2017-03-10                                          admin $ *
      * Modifications History: *
      * *
      * Modified By           Date        Version     Description *
      * --------------      ----------- ------------  -----------------------------------------*
      Leeyo                 10-Mar-2017   1.0         Splunk Customization for RC Split logic 606
      -- Split the Transfer batch to 1000 or less size batch
      -- without breaking an RC in two seperate batches
      **************************************************************************************************/

      CURSOR   c_group_id_cur (p_batch_id IN NUMBER)
      IS
      SELECT   batch_id
              ,reference22 rc_id
              ,COUNT(*)    cnt
              ,0           group_id
      FROM     rpro_gl_int_stage
      WHERE    batch_id    = p_batch_id
      AND      reference17 = '606' -- This is hard coded in GL interface mapping
      GROUP BY reference22
              ,batch_id
      ORDER BY 3;

      TYPE l_group_id_tab IS TABLE OF c_group_id_cur%ROWTYPE;
      l_group_id             l_group_id_tab;

      CURSOR   c_gl_sub    --#03062019
      IS
      SELECT   gl.reference23                       --#04042019
              ,cust.netsuite_internal_id
      FROM     rpro_gl_int_stage      gl
              ,rpro_cust_ui_tb10349_1 cust
      WHERE    gl.batch_id                        =  p_batch_id
      AND      gl.object_type                     = 'GL'
      AND      UPPER( cust.subsidiary_name )      =  UPPER(gl.reference23)
      AND      cust.batch_id                      =( SELECT MAX(cust1.batch_id)
                                                     FROM   rpro_cust_ui_tb10349_1 cust1
                                                     WHERE  UPPER( cust1.subsidiary_name) =  UPPER(gl.reference23))
      GROUP BY gl.reference23
              ,cust.netsuite_internal_id;

      TYPE typ_gl_sub IS TABLE OF c_gl_sub%ROWTYPE
      INDEX BY PLS_INTEGER;   --#03062019
      t_gl_sub                     typ_gl_sub;

      CURSOR   c_mje_sub    --#03182019
      IS
      SELECT   gl.reference23                            --#04042019
              ,cust.netsuite_internal_id
      FROM     rpro_gl_int_stage      gl
              ,rpro_cust_ui_tb10349_1 cust
      WHERE    gl.batch_id                        = p_batch_id
      AND      gl.object_type                     = 'MANUAL-JE'
      AND      UPPER( cust.subsidiary_name )      =  UPPER(gl.reference23)
      AND      cust.batch_id                      =( SELECT MAX(cust1.batch_id)
                                                     FROM   rpro_cust_ui_tb10349_1 cust1
                                                     WHERE  UPPER(cust1.subsidiary_name)  =  UPPER(gl.reference23))
      GROUP BY gl.reference23
              ,cust.netsuite_internal_id;

      TYPE typ_mje_sub IS TABLE OF c_mje_sub%ROWTYPE
      INDEX BY PLS_INTEGER;   --#03182019
      t_mje_sub                     typ_mje_sub;

      --HAVING SUM (entered_dr) - SUM (entered_cr) = 0;
      l_grp_cnt           NUMBER := 0;
      l_group_id_s        NUMBER := 1;
      l_updated_lines     NUMBER := 0;
      l_log_id            NUMBER;
      l_log_level         NUMBER;
      l_process_log       VARCHAR2 (1);
      l_object_name       VARCHAR2 (30);
      l_err_msg           VARCHAR2 (4000);
      l_split_lines_lkp   NUMBER :=1000;
      l_limit             NUMBER := 50000;                             --#03062019

   BEGIN
      l_object_name := 'RPRO_GL_BATCH_SPLIT_BY_RC';

      IF l_process_log = 'Y' AND l_log_level >= 3
      THEN
         rpro_log_pkg.record_log ( -1
                                  , l_object_name
                                  , l_log_id
                                  , 'Log: Inside RC Split code ');
      END IF;

      OPEN  c_group_id_cur(p_batch_id);
      FETCH c_group_id_cur
      BULK COLLECT INTO l_group_id;
      CLOSE c_group_id_cur;

      FOR i IN 1..l_group_id.COUNT
      LOOP
         l_grp_cnt := l_grp_cnt+l_group_id(i).cnt;

         IF l_grp_cnt    > l_split_lines_lkp
         THEN
            l_grp_cnt    := l_group_id(i).cnt;
            l_group_id_s := l_group_id_s+1;
         END IF;
         l_group_id(i).group_id := l_group_id_s;
      END LOOP;

      FORALL i IN 1..l_group_id.COUNT
      UPDATE rpro_gl_int_stage
      SET    reference8    =  l_group_id(i).group_id
      WHERE  reference8    IS NULL
      AND    batch_id      =  l_group_id(i).batch_id
      AND    reference17   =  '606'
      AND    reference22   =  l_group_id(i).rc_id;
      COMMIT;

      IF l_process_log = 'Y' AND l_log_level >= 3
      THEN
         rpro_log_pkg.record_log ( -1
                                  , l_object_name
                                  , l_log_id
                                  , 'Log: RC Split Lines Completed sucessfully');
      END IF;

      BEGIN
         UPDATE rpro_gl_int_stage stg
         SET    reference15 =(SELECT DISTINCT netsuite_internal_id
                              FROM   rpro_cust_ui_tb10002_1
                              WHERE  account_number = stg.segment1)
               ,reference12 =(SELECT DISTINCT netsuite_internal_id
                              FROM   rpro_cust_ui_tb10003_1
                              WHERE  class_name = stg.reference19)
               ,reference11 =(SELECT DISTINCT netsuite_internal_id
                              FROM   rpro_cust_ui_tb10004_1
                              WHERE  department_name = stg.reference20)
               ,reference13 =(SELECT DISTINCT netsuite_internal_id
                              FROM   rpro_cust_ui_tb10005_1
                              WHERE  location_name = stg.reference21)
         WHERE  batch_id    = p_batch_id
         AND    reference17 ='606';

         rpro_utility_pkg.record_log_act (-1
                                         , l_object_name
                                         , l_log_id
                                         , 'Successfully updated -  ' ||SQL%rowcount|| ' records' ,3 );
      EXCEPTION
      WHEN OTHERS THEN
         rpro_utility_pkg.record_log_act (-1
                                         , l_object_name
                                         , l_log_id
                                         , 'Updating Internal ID IN RPRO_GL_INT_STAGE failed with error ' ||SUBSTR(SQLERRM,1,200) ,3 );
      END;

      OPEN  c_gl_sub;
      LOOP
         FETCH c_gl_sub BULK COLLECT
         INTO  t_gl_sub
         LIMIT l_limit;
         EXIT WHEN t_gl_sub.COUNT() = 0;
         write_log ('t_gl_sub.COUNT ~ ' || t_gl_sub.COUNT);
         BEGIN
            FORALL j IN 1..t_gl_sub.COUNT                         --#04042019
            SAVE EXCEPTIONS
            UPDATE rpro_gl_int_stage
            SET    reference2   =  t_gl_sub(j).netsuite_internal_id
            WHERE  batch_id     =  p_batch_id
            AND    object_type  = 'GL'
            AND    reference23  =  t_gl_sub(j).reference23  ;
         EXCEPTION
         WHEN le_dml_exp  THEN
            FOR gl IN 1..SQL%bulk_exceptions.COUNT
            LOOP
               write_error(' Update GL netsuit id  '
                           ||CHR(10)
                           ||'Error Message : '|| SQLERRM (-SQL%bulk_exceptions (gl).error_code)
                          );
            END LOOP;
         END;
         COMMIT;
      END LOOP;
      CLOSE c_gl_sub;
      t_gl_sub.DELETE;    --#03062019

      OPEN  c_mje_sub;     --#03182019
      LOOP
         FETCH c_mje_sub BULK COLLECT
         INTO  t_mje_sub
         LIMIT l_limit;
         EXIT WHEN t_mje_sub.COUNT() = 0;
         write_log ('t_mje_sub.COUNT ~ ' || t_mje_sub.COUNT);
         BEGIN
            FORALL mje_acc IN 1..t_mje_sub.COUNT                        --#04042019
            SAVE EXCEPTIONS
            UPDATE rpro_gl_int_stage
            SET    reference2          =  t_mje_sub(mje_acc).netsuite_internal_id
            WHERE  batch_id            =  p_batch_id
            AND    object_type         =  'MANUAL-JE'
            AND    reference23         =  t_mje_sub(mje_acc).reference23  ;
         EXCEPTION
         WHEN le_dml_exp  THEN
            FOR mje IN 1..SQL%bulk_exceptions.COUNT
            LOOP
               write_error(' Update MJE netsuit id  '
                           ||CHR(10)
                           ||'Error Message : '|| SQLERRM (-SQL%bulk_exceptions (mje).error_code)
                          );
            END LOOP;
         END;
         COMMIT;
      END LOOP;
      CLOSE c_mje_sub;
      t_mje_sub.DELETE;    --#03182019
   EXCEPTION
   WHEN OTHERS THEN
      rpro_log_pkg.record_log ( p_batch_id, l_object_name, l_log_id, 'Exception in RC split logic at line ' || DBMS_UTILITY.format_error_backtrace || '. Error - ' || SQLERRM);
   END rpro_gl_batch_split_by_rc;
-------------------------------------------------------------
--LV20200401
PROCEDURE create_mje_from_unbill_rep(  p_errbuf OUT VARCHAR2
                                    ,  p_retcode OUT NUMBER )  --20190424   20190516
IS
   CURSOR c_rc_id
   IS
      WITH  pending_bill_rcs AS
                     (SELECT rc_id
                            ,SUM(alctbl_xt_prc)                  alctbl_xt_prc
                            ,SUM(bld_rec_amt)                    bld_rec_amt
                            ,SUM(bld_def_amt)                    bld_def_amt
                            ,SUM(bld_rec_amt) + SUM(bld_def_amt) total_bill_amt
                      FROM   rpro_rc_line_g rc
                      WHERE 1 =1
                      AND   EXISTS ( SELECT /*+ cardinality( rrl1, 1 ) */ 1  --20200310
                                     FROM   rpro_rc_line_g rrl1
                                     WHERE  1 = 1
                                     AND    rc.rc_id   = rrl1.rc_id
                                     AND    rrl1.atr32 = 'Y')
                      GROUP BY rc_id
                      HAVING SUM(alctbl_xt_prc) !=  SUM(bld_rec_amt) + SUM(bld_def_amt)
                      )
                      SELECT rrl.rc_id
                      FROM   rpro_rc_line_g    rrl
                            ,pending_bill_rcs  pbr
                      WHERE 1=1
                      ---AND   NOT EXISTS(SELECT 1
                      ---                 FROM   rpro_je_line_g rjl
                      ---                       ,rpro_je_head_g rjh
                      ---                 WHERE  rjh.id                                           = rjl.header_id
                      ---                 AND    rpro_je_head_pkg.get_status_flag(rjh.indicators) IN ('A','P')
                      ---                 AND    rjh.atr1                                         = 'UNBILL_MJE_CUSTOM'
                      ---                 AND    NVL(rjl.reference15,1)                           = rrl.rc_id
                      ---                 AND    rjh.prd_id                                       = (SELECT id
                      ---                                                                            FROM   rpro_period_g
                      ---                                                                            WHERE  status = 'OPEN')
                      ---                                                                            GROUP BY reference15)     --20190618   exclude the rc which is already approved for the current period
                      AND   rrl.rc_id                                             = pbr.rc_id
                      AND   rrl.doc_num                                           IS NOT NULL
                      --AND   rrl.atr32                                             = 'Y'   --20200310
                      AND   rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators) = 'Y'
                      GROUP BY rrl.rc_id
                      order by 1;

   CURSOR c_mje_lines(p_prd_id IN NUMBER,p_rc_id IN NUMBER)  --LV20200401
   IS
     SELECT  rc_id
         ,prod_ctgry
         ,total_pc_bld_amt
         ,total_bld_amt
         ,alctd_pc
         ,alctbl_pc
         ,cum_rev_pc
         ,ROUND(pct_alc*total_bld_amt)                                          pct_bld_amt
         ,DECODE(UPPER(prod_ctgry),'MARKETING',alctd_pc,tot_cl_end_bal )                                                def_tot_end_bal
         ,DECODE(UPPER(prod_ctgry),'MARKETING'
                                  ,ROUND((pct_alc*total_bld_amt),2)
                                  ,'LICENSE - TERM'
                                  ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0        --20190913
                                        THEN  0
                                        ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                    END)
                                  ,'LICENSE - PERPETUAL'
                                  ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0        --20190913
                                         THEN  0
                                         ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                    END)
                                  ,(CASE WHEN (ROUND((pct_alc*total_bld_amt),2) - cum_rev_pc)<0
                                         THEN  0
                                         ELSE  (ROUND((pct_alc*total_bld_amt),2) - cum_rev_pc)
                                    END)
                ) expected_def
         ,ROUND((DECODE(UPPER(prod_ctgry),'MARKETING'
                                         ---,ROUND((pct_alc*total_bld_amt),2)
                                         ,ROUND(((pct_alc*total_bld_amt)-alctd_pc),2)
                                         ,'LICENSE - TERM'
                                         ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0
                                                THEN  0
                                                ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                           END)
                                         ,'LICENSE - PERPETUAL'
                                         ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0
                                                THEN  0
                                                ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                           END)
                                         ,(CASE WHEN (ROUND(pct_alc*total_bld_amt,2) - cum_rev_pc)<0
                                               THEN  0
                                               ELSE  (ROUND((pct_alc*total_bld_amt),2) - cum_rev_pc)
                                           END)
                        )- (tot_cl_end_bal)),2) net_value  --modified by sera  20190524
         FROM
    ( SELECT DISTINCT rc_id
            ,prod_ctgry
            ,NVL(SUM(alctd_xt_prc)            OVER (PARTITION BY rc_id,prod_ctgry)       ,0)                                                     AS net_all
            ,NVL(SUM(bld_def_amt+bld_rec_amt) OVER (PARTITION BY rc_id,prod_ctgry)       ,0)                                                     AS total_pc_bld_amt
            ,NVL(SUM(bld_def_amt+bld_rec_amt) OVER (PARTITION BY rc_id)                  ,0)                                                     AS total_bld_amt
            ,NVL(SUM(alctd_xt_prc)            OVER (PARTITION BY rc_id,prod_ctgry)       ,0)                                                     AS alctd_pc
            ,NVL(SUM(alctbl_xt_prc)           OVER (PARTITION BY rc_id,prod_ctgry)       ,0)                                                     AS alctbl_pc
            ,(CASE WHEN COUNT(1)                        OVER (PARTITION BY rc_id) = 1         --20200309
                   THEN 1
                   ELSE  NVL(SUM(alctd_xt_prc)          OVER (PARTITION BY rc_id,prod_ctgry),0)
               END/
              CASE WHEN NVL(SUM(alctd_xt_prc) OVER (PARTITION BY rc_id),0) = 0
              THEN 1
              ELSE SUM(alctd_xt_prc) OVER (PARTITION BY rc_id)
              END
              )                                                                                                                           AS pct_alc
            ,SUM(NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Revenue',p_prd_id),0)
              + NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Adjustment Revenue',p_prd_id),0) )OVER (PARTITION BY rc_id,prod_ctgry) AS cum_rev_pc,
              SUM(NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Contract Liability',p_prd_id),0)
                            + NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Adjustment Liability',p_prd_id),0) )OVER (PARTITION BY rc_id,prod_ctgry) AS tot_cl_end_bal
     FROM rpro_rc_line_g
     WHERE doc_num                                           IS NOT NULL
     --AND   atr32                                             = 'Y'   --20200310
     AND   rpro_rc_line_pkg.get_cv_eligible_flag(indicators) = 'Y'
     AND   rc_id                                             = p_rc_id
    ) a;

   TYPE tab_mje_lines IS TABLE OF c_mje_lines%ROWTYPE
   INDEX BY PLS_INTEGER;

   l_je_head_ins      rpro_je_head_g%rowtype                                ;
   l_je_line_ins      rpro_je_line_g%ROWTYPE                                ;
   l_prd_id           NUMBER                                                ;
   l_run_cnt          NUMBER                                                ;
   l_rvsl_prd         NUMBER                                                ;
   l_prd_name         VARCHAR2(1000)                                        ;
   l_retcode          PLS_INTEGER                                           ;
   l_errbuf           VARCHAR2(1000)                                        ;
   l_ub_liab_accnt    VARCHAR2(1000)                                        ;
   l_liab_accnt       VARCHAR2(1000)                                        ;
   l_cur_prd_end_date DATE                                                  ;
   l_license_amt      NUMBER            := 0                                ;
   l_f_curr           rpro_rc_line_g.f_cur%type                             ;
   l_f_ex_rate        NUMBER                                                ;
   t_mje_lines        tab_mje_lines                                         ;
   l_limit            NUMBER            := 20000                            ;
   g_sec_atr_val      VARCHAR2(150)     := rpro_utility_pkg.g_sec_atr_val   ;
   g_user             VARCHAR2(240)     := rpro_utility_pkg.g_user          ;
   g_book_id          NUMBER            := rpro_utility_pkg.g_book_id       ;
   g_client_id        NUMBER            := rpro_utility_pkg.g_client_id     ;
   g_crtd_prd_id      NUMBER            := rpro_utility_pkg.get_crtd_prd_id ;
   l_crt_je_hdr_flag  VARCHAR2(150)     := 'Y'                              ;
   l_business_unit    rpro_rc_line_g.business_unit%type                     ;
   l_cstmr_nm         rpro_rc_line_g.cstmr_nm%type                          ;
   l_atr10            rpro_rc_line_g.atr10%type                             ;
   l_atr11            rpro_rc_line_g.atr11%type                             ;
   l_atr18            rpro_rc_line_g.atr18%type                             ;
   l_atr8             rpro_rc_line_g.atr8%type                              ;
   l_rc_id            NUMBER                                                ;
   l_je_head_id       rpro_je_head_g.id%TYPE                                ;
   l_schd_idx         NUMBER                                                ;
   l_rc_schd_data        ALL_TAB_PKG.RC_SCHD_DATA_TAB                       ;
   l_rc_schd_updt_data   ALL_TAB_PKG.RC_SCHD_DATA_TAB                       ;
   l_id                NUMBER;
   l_cur_prd_id        rpro_calendar_g.id%TYPE                              ;
BEGIN
   rpro_utility_pkg.set_revpro_context;

   /* Fetching current period id and name */
   BEGIN
      SELECT rc.id
            ,rc.period_name
            ,rc.end_date
      INTO   l_prd_id
            ,l_prd_name
            ,l_cur_prd_end_date
      FROM rpro_period_g rp ,
           rpro_calendar_g rc
      WHERE rp.status = 'OPEN'
      AND rp.id       = rc.id;
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error: Unable to fetch period id/period name: '||SQLERRM);
      RETURN;
   END;
   l_cur_prd_id  := l_prd_id;
   /*Deleting the existing mje which is not approved/validated for the current period*/  --201906
   FOR i IN (SELECT id header_id
             FROM   rpro_je_head_g
             WHERE  prd_id                                       =  l_prd_id
             AND    rpro_je_head_pkg.get_status_flag(indicators) IN ('E','V','N','P')
             AND    atr1                                         = 'UNBILL_MJE_CUSTOM')
   LOOP
   --deleting je lines
      write_log('Log: Deleting the existing rcs');
      DELETE FROM rpro_je_line_g
      WHERE  header_id = i.header_id;
   --deleting je head
      DELETE FROM rpro_je_head_g
      WHERE  id = i.header_id;
      COMMIT;
   END LOOP;
   l_je_head_id := NULL;
   SELECT MAX(id)
   INTO   l_je_head_id
   FROM   rpro_je_head_g
   WHERE  prd_id   =  l_prd_id
   AND    rpro_je_head_pkg.get_status_flag(indicators) = 'A'
   AND    atr1                                         = 'UNBILL_MJE_CUSTOM';

   write_log(' Reversal JE_BATCH_ID : '||l_je_head_id);
   IF l_je_head_id IS NOT NULL
   THEN
      SELECT *
      BULK COLLECT
      INTO   l_rc_schd_data
      FROM   rpro_rc_schd_g   rrs
      WHERE  rrs.je_batch_id  = l_je_head_id;

      l_schd_idx := l_rc_schd_data.FIRST;
      WHILE l_schd_idx IS NOT NULL
      LOOP
         IF rpro_rc_schd_pkg.get_interfaced_flag (l_rc_schd_data (l_schd_idx).indicators ) = 'Y'
         THEN
            l_id :=  rpro_utility_pkg.generate_id ('RPRO_RC_SCHD_ID_S',  rpro_utility_pkg.g_client_id);
            l_rc_schd_updt_data (l_id) := l_rc_schd_data (l_schd_idx);
            l_rc_schd_updt_data (l_id).id := l_id;
            IF l_rc_schd_data (l_schd_idx).rel_pct <> 0
            THEN
               l_rc_schd_updt_data (l_id).rel_pct :=
               l_rc_schd_data (l_schd_idx).rel_pct * -1;
            ELSE
               l_rc_schd_updt_data (l_id).amount :=
               l_rc_schd_data (l_schd_idx).amount * -1;
            END IF;
            l_rc_schd_updt_data (l_id).post_batch_id := NULL;
            l_rc_schd_updt_data (l_id).post_date := NULL;
            l_rc_schd_updt_data (l_id).indicators := rpro_rc_schd_pkg.set_interfaced_flag (l_rc_schd_updt_data (l_id).indicators,'N');
            IF l_rc_schd_updt_data (l_id).prd_id < l_cur_prd_id AND
               l_rc_schd_updt_data (l_id).prd_id <> l_cur_prd_id
            THEN
               l_rc_schd_updt_data (l_id).prd_id      := l_cur_prd_id;
               l_rc_schd_updt_data (l_id).post_prd_id := l_cur_prd_id;
               l_rc_schd_updt_data (l_id).crtd_prd_id := l_cur_prd_id;
            ELSIF l_rc_schd_updt_data (l_id).prd_id = l_cur_prd_id
            THEN
               l_rc_schd_updt_data (l_id).prd_id      := l_cur_prd_id;
               l_rc_schd_updt_data (l_id).post_prd_id := l_cur_prd_id;
               l_rc_schd_updt_data (l_id).crtd_prd_id := l_cur_prd_id;
            END IF;
            l_rc_schd_updt_data (l_id).indicators :=    rpro_rc_schd_pkg.set_update_or_insert_flag ( l_rc_schd_updt_data (l_id).indicators, 'I');
         ELSE
            DELETE
            FROM    rpro_rc_schd_g
            WHERE   id     =  l_rc_schd_data(l_schd_idx).id;
         END IF;
         l_schd_idx := l_rc_schd_data.NEXT (l_schd_idx);
      END LOOP;
      l_rc_schd_data.DELETE;
      all_tab_pkg.sync_rc_schd_data (l_rc_schd_updt_data);
   END IF;

   UPDATE rpro_je_head_g  --20200506
   SET    indicators = rpro_je_head_pkg.set_status_flag(indicators,'R')
   WHERE  id = l_je_head_id;

   COMMIT;
   /*Getting the reversal period*/
   BEGIN
      SELECT MIN(id)
      INTO   l_rvsl_prd
      FROM  rpro_calendar_g
      WHERE id >( SELECT id
                  FROM   rpro_period_g );
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error: Unable to fetch l_rvsl_prd: '||SQLERRM);
   END;
   write_log('Log: l_prd_id : '||l_prd_id
           ||CHR(10)
           ||'l_prd_name   : '||l_prd_name
           ||CHR(10)
           ||'l_rvsl_prd   : '||l_rvsl_prd);

   l_crt_je_hdr_flag  := 'Y';

   FOR dt IN c_rc_id
   LOOP
      /*Creating JE lines*/
      write_log('Log: rc_id : '||dt.rc_id);
      OPEN c_mje_lines(l_prd_id,dt.rc_id);
      LOOP
         FETCH c_mje_lines
         BULK  COLLECT
         INTO  t_mje_lines
         LIMIT l_limit;
         EXIT WHEN t_mje_lines.COUNT = 0;
         write_log('Log: Total Count: '||t_mje_lines.COUNT);

         FOR rec IN t_mje_lines.FIRST..t_mje_lines.LAST
         LOOP

            IF l_crt_je_hdr_flag = 'Y'
            THEN
               /* Creating JE Header*/
               l_je_head_ins.id            := rpro_utility_pkg.generate_id ('RPRO_RC_HEAD_ID_S',g_client_id)            ;
               l_je_head_ins.name          := 'Expected Deferred ~ '||l_prd_name||' '||SYSDATE                          ;
               l_je_head_ins.description   := 'Expected Deferred ~ '||l_prd_name||' '||SYSDATE                          ;
               l_je_head_ins.category_code := rpro_utility_pkg.get_lkp_val('CATEGORY_CODE','Category Code1')            ;
               l_je_head_ins.ex_rate_type  := 'User'                                                                    ;
               l_je_head_ins.sob_id        :=  1                                                                        ;
               l_je_head_ins.sob_name      := '1'                                                                       ;
               l_je_head_ins.fn_cur        := 'USD'                                                                     ;
               l_je_head_ins.rvsl_prd_id   := l_rvsl_prd                                                                ;
               l_je_head_ins.prd_id        := l_prd_id                                                                  ;
               l_je_head_ins.client_id     := g_client_id                                                               ;
               l_je_head_ins.crtd_prd_id   := l_prd_id                                                                  ;
               l_je_head_ins.sec_atr_val   := g_sec_atr_val                                                             ;
               l_je_head_ins.crtd_by       := g_user                                                                    ;
               l_je_head_ins.crtd_dt       := SYSDATE                                                                   ;
               l_je_head_ins.updt_by       := g_user                                                                    ;
               l_je_head_ins.updt_dt       := SYSDATE                                                                   ;
               l_je_head_ins.indicators    := rpro_je_head_pkg.set_status_flag(rpro_je_head_pkg.g_set_default_ind,'N')  ;
               l_je_head_ins.indicators    := rpro_je_head_pkg.set_summary_flag(rpro_je_head_pkg.g_set_default_ind,'Y') ;
               l_je_head_ins.book_id       := g_book_id                                                                 ;
               l_je_head_ins.rev_rec_type  := 'DR_APR'                                                                  ;
               l_je_head_ins.atr1          := 'UNBILL_MJE_CUSTOM'                                                       ;

               INSERT INTO rpro_je_head_g
               VALUES l_je_head_ins;
               write_log('Log: Created head entry count : '||SQL%ROWCOUNT);
               l_crt_je_hdr_flag  := 'N';
            END IF;
            IF t_mje_lines(rec).net_value <> 0
            THEN
               SELECT ub_liab_acct
                     ,def_segments
                     ,f_cur
                     ,f_ex_rate
                     ,business_unit
                     ,cstmr_nm
                     ,atr10
                     ,atr11
                     ,atr18
                     ,atr8
                     ,rc_id
               INTO   l_ub_liab_accnt
                     ,l_liab_accnt
                     ,l_f_curr
                     ,l_f_ex_rate
                     ,l_business_unit
                     ,l_cstmr_nm
                     ,l_atr10
                     ,l_atr11
                     ,l_atr18
                     ,l_atr8
                     ,l_rc_id
               FROM   rpro_rc_line_g
               WHERE  rc_id      = t_mje_lines(rec).rc_id
               AND    prod_ctgry = t_mje_lines(rec).prod_ctgry
               AND    rownum     = 1;

               IF l_ub_liab_accnt IS NULL --20190524
               THEN
                  BEGIN
                     SELECT source_value
                     INTO   l_ub_liab_accnt
                     FROM   rpro_account_g
                     WHERE  acct_type_id = 'U'
                     AND    source_type  = 'CONSTANT';
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error: Unble to find the unbilled account~Exiting..the current prod ctgry');
                     EXIT;
                  END;
               END IF;

               write_log('Log: rc_id :  '      ||t_mje_lines(rec).rc_id
                        ||CHR(10)
                        ||'JE head id : '      ||l_je_head_ins.id
                        ||CHR(10)
                        ||' prod_ctgry :  '||t_mje_lines(rec).prod_ctgry
                        ||CHR(10)
                        ||'~expected_def~'||t_mje_lines(rec).expected_def
                        ||CHR(10)
                        ||'~def_tot_end_bal~'||t_mje_lines(rec).def_tot_end_bal
                        ||CHR(10)
                        ||'~l_ub_liab_accnt:'    ||l_ub_liab_accnt
                        ||CHR(10)
                        ||'~l_liab_accnt:'    ||l_liab_accnt
                        ||CHR(10)
                        ||'Loop count : '||rec);

               IF (     UPPER(t_mje_lines(rec).prod_ctgry) <> UPPER('LICENSE - PERPETUAL')
                    AND UPPER(t_mje_lines(rec).prod_ctgry) <> UPPER('LICENSE - TERM')    )
               THEN
                  /*Creating Debit entry*/
                  l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
                  l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
                  l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
                  l_je_line_ins.curr             := l_f_curr                                                                 ;
                  l_je_line_ins.amount           := ABS(ROUND(t_mje_lines(rec).net_value,2))                                 ;
                  l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
                  l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
                  l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
                  l_je_line_ins.func_amount      := ABS(t_mje_lines(rec).net_value)*l_f_ex_rate                              ;
                  l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
                  l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.cr_cc_id         := NULL                                                                     ;
                  l_je_line_ins.cr_activity_type := NULL                                                                     ;
                  /*
                  l_je_line_ins.dr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Contract Liability'
                                                         ELSE 'Unbilled'
                                                    END                                                                      ;
                  l_je_line_ins.dr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_liab_accnt
                                                         ELSE l_ub_liab_accnt
                                                    END                                                                      ;
                                                    Ganga 11082019
                                                    */
                  l_je_line_ins.dr_cc_id         := 999                                                                      ;
                  l_je_line_ins.dr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Unbilled'
                                                         ELSE 'Contract Liability'
                                                    END                                                                      ;
                  l_je_line_ins.dr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_ub_liab_accnt
                                                         ELSE l_liab_accnt
                                                    END                                                                      ;
                  l_je_line_ins.client_id        := g_client_id                                                              ;
                  l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
                  l_je_line_ins.crtd_by          := g_user                                                                   ;
                  l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.updt_by          := g_user                                                                   ;
                  l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
                  l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
                  l_je_line_ins.book_id          := g_book_id                                                                ;
                  --20190618 Copying few more columns
                  l_je_line_ins.reference1       := l_business_unit                                                          ;
                  l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
                  l_je_line_ins.reference3       := l_atr10                                                                  ;
                  l_je_line_ins.reference4       := l_atr11                                                                  ;
                  l_je_line_ins.reference5       := 'Core : Core'                                                            ;
                  l_je_line_ins.reference6       := l_atr18                                                                  ;
                  l_je_line_ins.reference8       := l_atr8                                                                   ;
                  l_je_line_ins.reference9       := t_mje_lines(rec).prod_ctgry                                              ;
                  l_je_line_ins.reference15      := t_mje_lines(rec).rc_id                                                   ;


                  write_log('Log: dr_activity_type 1: '||l_je_line_ins.dr_activity_type
                           ||'~cr_activity_type    1:~'||l_je_line_ins.cr_activity_type
                           ||'~l_je_line_ins.amount  ~'||l_je_line_ins.amount
                           ||'~t_mje_lines(rec).net_value~'||t_mje_lines(rec).net_value);

                  INSERT INTO rpro_je_line_g
                  VALUES l_je_line_ins;
                  write_log('Log: Created debit line count : '||SQL%ROWCOUNT);
                  COMMIT;

                  /*Resetting the variable*/
                  l_je_line_ins  := NULL;

                  /*Creating Credit entry*/
                  l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
                  l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
                  l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
                  l_je_line_ins.curr             := l_f_curr                                                                 ;
                  l_je_line_ins.amount           := ABS(t_mje_lines(rec).net_value)                                          ;
                  l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
                  l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
                  l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
                  l_je_line_ins.func_amount      := ABS(t_mje_lines(rec).net_value)*l_f_ex_rate                              ;
                  l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
                  l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.dr_activity_type := NULL                                                                     ;
                  l_je_line_ins.dr_cc_id         := NULL                                                                     ;
                  /*
                  l_je_line_ins.cr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Unbilled'
                                                         ELSE 'Contract Liability'
                                                    END                                                                      ;
                  l_je_line_ins.cr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_ub_liab_accnt
                                                         ELSE l_liab_accnt
                                                    END                                                                      ;
                                                    Ganga 11082019 */
                  l_je_line_ins.cr_cc_id         := 999                                                                      ;
                  l_je_line_ins.cr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Contract Liability'
                                                         ELSE 'Unbilled'
                                                    END                                                                      ;
                  l_je_line_ins.cr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_liab_accnt
                                                         ELSE l_ub_liab_accnt
                                                    END                                                                      ;
                  l_je_line_ins.client_id        := g_client_id                                                              ;
                  l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
                  l_je_line_ins.crtd_by          := g_user                                                                   ;
                  l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.updt_by          := g_user                                                                   ;
                  l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
                  l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
                  l_je_line_ins.book_id          := g_book_id                                                                ;
                  --20190618 Copying few more columns
                  l_je_line_ins.reference1       := l_business_unit                                                          ;
                  l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
                  l_je_line_ins.reference3       := l_atr10                                                                  ;
                  l_je_line_ins.reference4       := l_atr11                                                                  ;
                  l_je_line_ins.reference5       := 'Core : Core'                                                            ;
                  l_je_line_ins.reference6       := l_atr18                                                                  ;
                  l_je_line_ins.reference8       := l_atr8                                                                   ;
                  l_je_line_ins.reference9       := t_mje_lines(rec).prod_ctgry                                              ;
                  l_je_line_ins.reference15      := t_mje_lines(rec).rc_id                                                   ;

                  write_log('Log: dr_activity_type 2: '||l_je_line_ins.dr_activity_type
                           ||'~cr_activity_type    2:~'||l_je_line_ins.cr_activity_type
                           ||'~l_je_line_ins.amount  ~'||l_je_line_ins.amount
                           ||'~t_mje_lines(rec).net_value~'||t_mje_lines(rec).net_value);

                  INSERT INTO rpro_je_line_g
                  VALUES l_je_line_ins;
                  write_log('Log: Created credit line count : '||SQL%ROWCOUNT);
                  COMMIT;
                  l_je_line_ins  := NULL;
               ELSIF  (    UPPER(t_mje_lines(rec).prod_ctgry) = UPPER('LICENSE - PERPETUAL')
                       OR UPPER(t_mje_lines(rec).prod_ctgry) = UPPER('LICENSE - TERM')    )
               THEN
                  l_license_amt := NVL(l_license_amt,0) + NVL(t_mje_lines(rec).net_value,0);
               END IF;
            END IF;
         END LOOP;
         IF l_license_amt <> 0
         THEN
            SELECT ub_liab_acct
                  ,def_segments
                  ,f_cur
                  ,f_ex_rate
                  ,business_unit
                  ,cstmr_nm
                  ,atr10
                  ,atr11
                  ,atr18
                  ,atr8
                  ,rc_id
            INTO   l_ub_liab_accnt
                  ,l_liab_accnt
                  ,l_f_curr
                  ,l_f_ex_rate
                  ,l_business_unit
                  ,l_cstmr_nm
                  ,l_atr10
                  ,l_atr11
                  ,l_atr18
                  ,l_atr8
                  ,l_rc_id
            FROM   rpro_rc_line_g
            WHERE  rc_id      = dt.rc_id
            AND    UPPER(prod_ctgry) IN ('LICENSE - PERPETUAL','LICENSE - TERM')
            AND    rownum     = 1;


            IF l_ub_liab_accnt IS NULL --20200130
            THEN
               BEGIN
                  SELECT source_value
                  INTO   l_ub_liab_accnt
                  FROM   rpro_account_g
                  WHERE  acct_type_id = 'U'
                  AND    source_type  = 'CONSTANT';
               EXCEPTION
               WHEN OTHERS THEN
                  write_error('Error: Unble to find the unbilled account~Exiting..the current prod ctgry');
                  EXIT;
               END;
            END IF;

            /*Creating Debit entry*/
            l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
            l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
            l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
            l_je_line_ins.curr             := l_f_curr                                                                 ;
            l_je_line_ins.amount           := ABS(l_license_amt)                                                       ;
            l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
            l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
            l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
            l_je_line_ins.func_amount      := ABS(l_license_amt)*l_f_ex_rate                                           ;
            l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
            l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.cr_cc_id         := NULL                                                                     ;
            l_je_line_ins.cr_activity_type := NULL                                                                     ;
            /*
            l_je_line_ins.dr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Contract Liability'
                                                   ELSE 'Unbilled'
                                              END                                                                      ;
            l_je_line_ins.dr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_liab_accnt
                                                   ELSE l_ub_liab_accnt
                                              END                                                                      ;
                                             Ganga 11082019 */
            l_je_line_ins.dr_cc_id         := 999                                                                      ;
            l_je_line_ins.dr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Unbilled'
                                                   ELSE 'Contract Liability'
                                              END                                                                      ;
            l_je_line_ins.dr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_ub_liab_accnt
                                                   ELSE l_liab_accnt
                                              END                                                                      ;
            l_je_line_ins.client_id        := g_client_id                                                              ;
            l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
            l_je_line_ins.crtd_by          := g_user                                                                   ;
            l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
            l_je_line_ins.updt_by          := g_user                                                                   ;
            l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
            l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
            l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
            l_je_line_ins.book_id          := g_book_id                                                                ;
            --20190618 Copying few more columns
            l_je_line_ins.reference1       := l_business_unit                                                          ;
            l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
            l_je_line_ins.reference3       := l_atr10                                                                  ;
            l_je_line_ins.reference4       := l_atr11                                                                  ;
            l_je_line_ins.reference5       := 'Core : Core'                                                            ;
            l_je_line_ins.reference6       := l_atr18                                                                  ;
            l_je_line_ins.reference8       := l_atr8                                                                   ;
            l_je_line_ins.reference9       := 'LICENSE - TERM'                                                         ;
            l_je_line_ins.reference15      := l_rc_id                                                                  ;

            write_log('Log: dr_activity_type 1: '||l_je_line_ins.dr_activity_type
                     ||'~cr_activity_type    1:~'||l_je_line_ins.cr_activity_type
                     ||'~l_je_line_ins.amount~'  ||l_je_line_ins.amount
                     ||'~l_license_amt~'         ||l_license_amt);

            INSERT INTO rpro_je_line_g
            VALUES l_je_line_ins;
            write_log('Log: Created debit line count : '||SQL%ROWCOUNT);
            COMMIT;

            /*Resetting the variable*/
            l_je_line_ins  := NULL;

            /*Creating Credit entry*/
            l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
            l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
            l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
            l_je_line_ins.curr             := l_f_curr                                                                 ;
            l_je_line_ins.amount           := ABS(l_license_amt)                                                       ;
            l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
            l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
            l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
            l_je_line_ins.func_amount      := ABS(l_license_amt)*l_f_ex_rate                                           ;
            l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
            l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.dr_activity_type := NULL                                                                     ;
            l_je_line_ins.dr_cc_id         := NULL                                                                     ;
            /*
            l_je_line_ins.cr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Unbilled'
                                                   ELSE 'Contract Liability'
                                              END                                                                      ;
            l_je_line_ins.cr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_ub_liab_accnt
                                                   ELSE l_liab_accnt
                                              END                                                                      ;
                                             11082019 */
            l_je_line_ins.cr_cc_id         := 999                                                                      ;
            l_je_line_ins.cr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Contract Liability'
                                                   ELSE 'Unbilled'
                                              END                                                                      ;
            l_je_line_ins.cr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_liab_accnt
                                                   ELSE l_ub_liab_accnt
                                              END                                                                      ;
            l_je_line_ins.client_id        := g_client_id                                                              ;
            l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
            l_je_line_ins.crtd_by          := g_user                                                                   ;
            l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
            l_je_line_ins.updt_by          := g_user                                                                   ;
            l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
            l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
            l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
            l_je_line_ins.book_id          := g_book_id                                                                ;
            --20190618 Copying few more columns
            l_je_line_ins.reference1       := l_business_unit                                                          ;
            l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
            l_je_line_ins.reference3       := l_atr10                                                                  ;
            l_je_line_ins.reference4       := l_atr11                                                                  ;
            l_je_line_ins.reference5       := 'Core : Core'                                                            ;
            l_je_line_ins.reference6       := l_atr18                                                                  ;
            l_je_line_ins.reference8       := l_atr8                                                                   ;
            l_je_line_ins.reference9       := 'LICENSE - TERM'                                                         ;
            l_je_line_ins.reference15      := l_rc_id                                                                  ;

            write_log('Log: dr_activity_type 2: '||l_je_line_ins.dr_activity_type
                     ||'~cr_activity_type    2:~'||l_je_line_ins.cr_activity_type
                     ||'~l_je_line_ins.amount  ~'||l_je_line_ins.amount
                     ||'~l_license_amt~'         ||l_license_amt);

            INSERT INTO rpro_je_line_g
            VALUES l_je_line_ins;
            write_log('Log: Created credit line count : '||SQL%ROWCOUNT);
            COMMIT;
            l_je_line_ins  := NULL;
         END IF;
         l_license_amt := NULL;
         l_rc_id       := NULL;
      END LOOP;
      CLOSE c_mje_lines;
   END LOOP;
   /*Calling JE validation*/
   rpro_je_validation_pkg.je_validation_wrapper  ( l_errbuf
                                                 , l_retcode
                                                 , l_je_head_ins.id );

   COMMIT;
EXCEPTION
WHEN OTHERS THEN
write_log('Log: create_mje_from_unbill_rep : Main Exception@'||DBMS_UTILITY.format_error_backtrace||'~Error~'||SQLERRM);
END create_mje_from_unbill_rep;
-------------------------------------------------------------

PROCEDURE create_mje_report_script  --20190424   20190516
IS
   CURSOR c_rc_id
   IS
      WITH  pending_bill_rcs AS
                     (SELECT rc_id
                            ,SUM(alctbl_xt_prc)                  alctbl_xt_prc
                            ,SUM(bld_rec_amt)                    bld_rec_amt
                            ,SUM(bld_def_amt)                    bld_def_amt
                            ,SUM(bld_rec_amt) + SUM(bld_def_amt) total_bill_amt
                      FROM   rpro_rc_line_g rc
                      WHERE 1 =1
                      AND   EXISTS ( SELECT /*+ cardinality( rrl1, 1 ) */ 1   --20200310
                                     FROM   rpro_rc_line_g rrl1
                                     WHERE  1 = 1
                                     AND    rc.rc_id   = rrl1.rc_id
                                     AND    rrl1.atr32 = 'Y')
                      GROUP BY rc_id
                      HAVING SUM(alctbl_xt_prc) !=  SUM(bld_rec_amt) + SUM(bld_def_amt)
                      )
                      SELECT rrl.rc_id
                      FROM   rpro_rc_line_g    rrl
                            ,pending_bill_rcs  pbr
                      WHERE 1=1
                      ----AND   NOT EXISTS(SELECT 1
                      ----                 FROM   rpro_je_line_g rjl
                      ----                       ,rpro_je_head_g rjh
                      ----                 WHERE  rjh.id                                           = rjl.header_id
                      ----                 AND    rpro_je_head_pkg.get_status_flag(rjh.indicators) IN ('A','P')
                      ----                 AND    rjh.atr1                                         = 'UNBILL_MJE_CUSTOM'
                      ----                 AND    NVL(rjl.reference15,1)                           = rrl.rc_id
                      ----                 AND    rjh.prd_id                                       = (SELECT id
                      ----                                                                            FROM   rpro_period_g
                      ----                                                                            WHERE  status = 'OPEN')
                      ----                                                                            GROUP BY reference15)     --20190618   exclude the rc which is already approved for the current period
                      AND   rrl.rc_id                                             = pbr.rc_id
                      AND   rrl.doc_num                                           IS NOT NULL
                      --AND   rrl.atr32                                             = 'Y'  --20200310
                      AND   rpro_rc_line_pkg.get_cv_eligible_flag(rrl.indicators) = 'Y'
                      GROUP BY rrl.rc_id
                      order by 1;

   CURSOR c_mje_lines(p_prd_id IN NUMBER,p_rc_id IN NUMBER)
   IS
     SELECT  rc_id
         ,prod_ctgry
         ,total_pc_bld_amt
         ,total_bld_amt
         ,alctd_pc
         ,alctbl_pc
         ,cum_rev_pc
         ,ROUND(pct_alc*total_bld_amt)                                          pct_bld_amt
         ,DECODE(UPPER(prod_ctgry),'MARKETING',alctd_pc,tot_cl_end_bal )                                                def_tot_end_bal
         ,DECODE(UPPER(prod_ctgry),'MARKETING'
                                  ,ROUND((pct_alc*total_bld_amt),2)
                                  ,'LICENSE - TERM'
                                  ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0        --20190913
                                        THEN  0
                                        ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                    END)
                                  ,'LICENSE - PERPETUAL'
                                  ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0        --20190913
                                         THEN  0
                                         ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                    END)
                                  ,(CASE WHEN (ROUND((pct_alc*total_bld_amt),2) - cum_rev_pc)<0
                                         THEN  0
                                         ELSE  (ROUND((pct_alc*total_bld_amt),2) - cum_rev_pc)
                                    END)
                ) expected_def
         ,ROUND((DECODE(UPPER(prod_ctgry),'MARKETING'
                                         ---,ROUND((pct_alc*total_bld_amt),2)
                                         ,ROUND(((pct_alc*total_bld_amt)-alctd_pc),2)
                                         ,'LICENSE - TERM'
                                         ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0
                                                THEN  0
                                                ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                           END)
                                         ,'LICENSE - PERPETUAL'
                                         ,(CASE WHEN ROUND(alctd_pc,2) - cum_rev_pc<0
                                                THEN  0
                                                ELSE  ROUND(alctd_pc,2) - cum_rev_pc
                                           END)
                                         ,(CASE WHEN (ROUND(pct_alc*total_bld_amt,2) - cum_rev_pc)<0
                                               THEN  0
                                               ELSE  (ROUND((pct_alc*total_bld_amt),2) - cum_rev_pc)
                                           END)
                        )- (tot_cl_end_bal)),2) net_value  --modified by sera  20190524
         ,ROUND( (DECODE(UPPER(prod_ctgry),'MARKETING'
                                          , 0
                                         ---,ROUND((pct_alc*total_bld_amt),2)
                                         --,ROUND(((pct_alc*total_bld_amt)-alctd_pc),2)
                                          , 'LICENSE - TERM'
                                          ,(CASE WHEN (alctd_pc - ROUND(pct_alc*total_bld_amt,2) ) < 0
                                                THEN  0
                                                ELSE  (alctd_pc - ROUND((pct_alc*total_bld_amt),2))
                                            END)
                                          ,'LICENSE - PERPETUAL'
                                          ,(CASE WHEN (alctd_pc - ROUND(pct_alc*total_bld_amt,2) ) < 0
                                                THEN  0
                                                ELSE  (alctd_pc - ROUND((pct_alc*total_bld_amt),2))
                                            END)
                                          ,(CASE WHEN (cum_rev_pc - ROUND(pct_alc*total_bld_amt,2) ) < 0
                                                THEN  0
                                                ELSE  (cum_rev_pc - ROUND((pct_alc*total_bld_amt),2))
                                            END)
                        ) ), 2) expected_unbill_amt
         ,ROUND( (DECODE(UPPER(prod_ctgry),'MARKETING'
                                          , 0
                                -- ,ROUND((pct_alc*total_bld_amt),2)
                                -- ,ROUND(((pct_alc*total_bld_amt)-alctd_pc),2)
                                          , 'LICENSE - TERM'
                                          ,(CASE WHEN (alctd_pc - ROUND(pct_alc*total_bld_amt,2) ) < 0
                                                THEN  0
                                                ELSE  (alctd_pc - ROUND((pct_alc*total_bld_amt),2))
                                            END)
                                          ,'LICENSE - PERPETUAL'
                                          ,(CASE WHEN (alctd_pc - ROUND(pct_alc*total_bld_amt,2) ) < 0
                                                THEN  0
                                                ELSE  (alctd_pc - ROUND((pct_alc*total_bld_amt),2))
                                            END)
                                          ,(CASE WHEN (cum_rev_pc - ROUND(pct_alc*total_bld_amt,2) ) < 0
                                                THEN  0
                                                ELSE  (cum_rev_pc - ROUND((pct_alc*total_bld_amt),2))
                                            END)
                        ) ) - unbilled_amount ,2) net_unbilled_amt
         ,unbilled_amount
         FROM
    ( SELECT DISTINCT rc_id
            ,prod_ctgry
            ,NVL(SUM(alctd_xt_prc)            OVER (PARTITION BY rc_id,prod_ctgry)     , 0)                                                       AS net_all
            ,NVL(SUM(bld_def_amt+bld_rec_amt) OVER (PARTITION BY rc_id,prod_ctgry)     , 0)                                                       AS total_pc_bld_amt
            ,NVL(SUM(bld_def_amt+bld_rec_amt) OVER (PARTITION BY rc_id)                , 0)                                                       AS total_bld_amt
            ,NVL(SUM(alctd_xt_prc)            OVER (PARTITION BY rc_id,prod_ctgry)     , 0)                                                       AS alctd_pc
            ,NVL(SUM(alctbl_xt_prc)           OVER (PARTITION BY rc_id,prod_ctgry)     , 0)                                                       AS alctbl_pc
            ,( CASE WHEN COUNT(1)                        OVER (PARTITION BY rc_id) = 1         --20200309
                   THEN 1
                   ELSE  NVL(SUM(alctd_xt_prc)           OVER (PARTITION BY rc_id,prod_ctgry),0)
               END /
              CASE WHEN NVL(SUM(alctd_xt_prc) OVER (PARTITION BY rc_id),0) = 0
              THEN 1
              ELSE SUM(alctd_xt_prc) OVER (PARTITION BY rc_id)
              END
              )                                                                                                                           AS pct_alc
            ,SUM(NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Revenue',p_prd_id),0)
              + NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Adjustment Revenue',p_prd_id),0) )OVER (PARTITION BY rc_id,prod_ctgry) AS cum_rev_pc,
              SUM(NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Contract Liability',p_prd_id),0)
                            + NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(ID,'Adjustment Liability',p_prd_id),0) )OVER (PARTITION BY rc_id,prod_ctgry) AS tot_cl_end_bal
           ,SUM(rpro_splk_fgaap_custom_pkg.get_unbilled_amt (id,'Revenue',p_prd_id) ) OVER (PARTITION BY rc_id, prod_ctgry)   AS unbilled_amount
     FROM rpro_rc_line_g
     WHERE doc_num                                           IS NOT NULL
     --AND   atr32                                             = 'Y'  --20200310
     AND   rpro_rc_line_pkg.get_cv_eligible_flag(indicators) = 'Y'
     AND   rc_id                                             = p_rc_id
    ) a;

   TYPE tab_mje_lines IS TABLE OF c_mje_lines%ROWTYPE
   INDEX BY PLS_INTEGER;

   l_je_head_ins      rpro_splunk_custom_je_head_g%rowtype                                ;
   l_je_line_ins      rpro_splunk_custom_je_line_g%ROWTYPE                                ;
   l_prd_id           NUMBER                                                ;
   l_run_cnt          NUMBER                                                ;
   l_rvsl_prd         NUMBER                                                ;
   l_prd_name         VARCHAR2(1000)                                        ;
   l_retcode          PLS_INTEGER                                           ;
   l_errbuf           VARCHAR2(1000)                                        ;
   l_ub_liab_accnt    VARCHAR2(1000)                                        ;
   l_liab_accnt       VARCHAR2(1000)                                        ;
   l_cur_prd_end_date DATE                                                  ;
   l_license_amt      NUMBER            := 0                                ;
   l_f_curr           rpro_rc_line_g.f_cur%type                             ;
   l_f_ex_rate        NUMBER                                                ;
   t_mje_lines        tab_mje_lines                                         ;
   l_limit            NUMBER            := 20000                            ;
   g_sec_atr_val      VARCHAR2(150)     := rpro_utility_pkg.g_sec_atr_val   ;
   g_user             VARCHAR2(240)     := rpro_utility_pkg.g_user          ;
   g_book_id          NUMBER            := rpro_utility_pkg.g_book_id       ;
   g_client_id        NUMBER            := rpro_utility_pkg.g_client_id     ;
   g_crtd_prd_id      NUMBER            := rpro_utility_pkg.get_crtd_prd_id ;
   l_crt_je_hdr_flag  VARCHAR2(150)     := 'Y'                              ;
   l_business_unit    rpro_rc_line_g.business_unit%type                     ;
   l_cstmr_nm         rpro_rc_line_g.cstmr_nm%type                          ;
   l_atr10            rpro_rc_line_g.atr10%type                             ;
   l_atr11            rpro_rc_line_g.atr11%type                             ;
   l_atr18            rpro_rc_line_g.atr18%type                             ;
   l_atr8             rpro_rc_line_g.atr8%type                              ;
   l_rc_id            NUMBER                                                ;
   l_tot_net_unbill   NUMBER                                                ;
   l_tot_net_value    NUMBER                                                ;

BEGIN
   rpro_utility_pkg.set_revpro_context;


   /* Fetching current period id and name */
   BEGIN
      SELECT rc.id
            ,rc.period_name
            ,rc.end_date
      INTO   l_prd_id
            ,l_prd_name
            ,l_cur_prd_end_date
      FROM rpro_period_g rp ,
           rpro_calendar_g rc
      WHERE rp.status = 'OPEN'
      AND rp.id       = rc.id;
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error: Unable to fetch period id/period name: '||SQLERRM);
      RETURN;
   END;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE rpro_splunk_custom_je_head_g';
   EXECUTE IMMEDIATE 'TRUNCATE TABLE rpro_splunk_custom_je_line_g';

   /*Getting the reversal period*/
   BEGIN
      SELECT MIN(id)
      INTO   l_rvsl_prd
      FROM  rpro_calendar_g
      WHERE id >( SELECT id
                  FROM   rpro_period_g );
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error: Unable to fetch l_rvsl_prd: '||SQLERRM);
   END;
   write_log('Log: l_prd_id : '||l_prd_id
           ||CHR(10)
           ||'l_prd_name   : '||l_prd_name
           ||CHR(10)
           ||'l_rvsl_prd   : '||l_rvsl_prd);

   l_crt_je_hdr_flag  := 'Y';

   FOR dt IN c_rc_id
   LOOP
      /*Creating JE lines*/
      write_log('Log: rc_id : '||dt.rc_id);
      OPEN c_mje_lines(l_prd_id,dt.rc_id);
      LOOP
         FETCH c_mje_lines
         BULK  COLLECT
         INTO  t_mje_lines
         LIMIT l_limit;
         EXIT WHEN t_mje_lines.COUNT = 0;
         write_log('Log: Total Count: '||t_mje_lines.COUNT);

         l_tot_net_unbill  := 0;
         l_tot_net_value   := 0;

         FOR rec IN t_mje_lines.FIRST..t_mje_lines.LAST
         LOOP
            IF l_crt_je_hdr_flag = 'Y'
            THEN
               /* Creating JE Header*/
               l_je_head_ins.id            := rpro_utility_pkg.generate_id ('RPRO_RC_HEAD_ID_S',g_client_id)            ;
               l_je_head_ins.name          := 'Expected Deferred ~ '||l_prd_name||' '||SYSDATE                          ;
               l_je_head_ins.description   := 'Expected Deferred ~ '||l_prd_name||' '||SYSDATE                          ;
               l_je_head_ins.category_code := rpro_utility_pkg.get_lkp_val('CATEGORY_CODE','Category Code1')            ;
               l_je_head_ins.ex_rate_type  := 'User'                                                                    ;
               l_je_head_ins.sob_id        :=  1                                                                        ;
               l_je_head_ins.sob_name      := '1'                                                                       ;
               l_je_head_ins.fn_cur        := 'USD'                                                                     ;
               l_je_head_ins.rvsl_prd_id   := l_rvsl_prd                                                                ;
               l_je_head_ins.prd_id        := l_prd_id                                                                  ;
               l_je_head_ins.client_id     := g_client_id                                                               ;
               l_je_head_ins.crtd_prd_id   := l_prd_id                                                                  ;
               l_je_head_ins.sec_atr_val   := g_sec_atr_val                                                             ;
               l_je_head_ins.crtd_by       := g_user                                                                    ;
               l_je_head_ins.crtd_dt       := SYSDATE                                                                   ;
               l_je_head_ins.updt_by       := g_user                                                                    ;
               l_je_head_ins.updt_dt       := SYSDATE                                                                   ;
               l_je_head_ins.indicators    := rpro_je_head_pkg.set_status_flag(rpro_je_head_pkg.g_set_default_ind,'N')  ;
               l_je_head_ins.indicators    := rpro_je_head_pkg.set_summary_flag(rpro_je_head_pkg.g_set_default_ind,'Y') ;
               l_je_head_ins.book_id       := g_book_id                                                                 ;
               l_je_head_ins.rev_rec_type  := 'DR_APR'                                                                  ;
               l_je_head_ins.atr1          := 'UNBILL_MJE_CUSTOM'                                                       ;

               INSERT INTO rpro_splunk_custom_je_head_g
               VALUES l_je_head_ins;
               write_log('Log: Created head entry count : '||SQL%ROWCOUNT);
               l_crt_je_hdr_flag  := 'N';
            END IF;
            IF t_mje_lines(rec).net_value <> 0
            THEN
               SELECT ub_liab_acct
                     ,def_segments
                     ,f_cur
                     ,f_ex_rate
                     ,business_unit
                     ,cstmr_nm
                     ,atr10
                     ,atr11
                     ,atr18
                     ,atr8
                     ,rc_id
               INTO   l_ub_liab_accnt
                     ,l_liab_accnt
                     ,l_f_curr
                     ,l_f_ex_rate
                     ,l_business_unit
                     ,l_cstmr_nm
                     ,l_atr10
                     ,l_atr11
                     ,l_atr18
                     ,l_atr8
                     ,l_rc_id
               FROM   rpro_rc_line_g
               WHERE  rc_id      = t_mje_lines(rec).rc_id
               AND    prod_ctgry = t_mje_lines(rec).prod_ctgry
               AND    rownum     = 1;

               IF l_ub_liab_accnt IS NULL --20190524
               THEN
                  BEGIN
                     SELECT source_value
                     INTO   l_ub_liab_accnt
                     FROM   rpro_account_g
                     WHERE  acct_type_id = 'U'
                     AND    source_type  = 'CONSTANT';
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error: Unble to find the unbilled account~Exiting..the current prod ctgry');
                     EXIT;
                  END;
               END IF;

               write_log('Log: rc_id :  '      ||t_mje_lines(rec).rc_id
                        ||CHR(10)
                        ||'JE head id : '      ||l_je_head_ins.id
                        ||CHR(10)
                        ||' prod_ctgry :  '||t_mje_lines(rec).prod_ctgry
                        ||CHR(10)
                        ||'~expected_def~'||t_mje_lines(rec).expected_def
                        ||CHR(10)
                        ||'~def_tot_end_bal~'||t_mje_lines(rec).def_tot_end_bal
                        ||CHR(10)
                        ||'~l_ub_liab_accnt:'    ||l_ub_liab_accnt
                        ||CHR(10)
                        ||'~l_liab_accnt:'    ||l_liab_accnt
                        ||CHR(10)
                        ||'Loop count : '||rec);

               l_tot_net_unbill  := l_tot_net_unbill + t_mje_lines(rec).net_unbilled_amt;
               l_tot_net_value   := l_tot_net_value  + t_mje_lines(rec).net_value;

               IF (     UPPER(t_mje_lines(rec).prod_ctgry) <> UPPER('LICENSE - PERPETUAL')
                    AND UPPER(t_mje_lines(rec).prod_ctgry) <> UPPER('LICENSE - TERM')    )
               THEN
                  /*Creating Debit entry*/
                  l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
                  l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
                  l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
                  l_je_line_ins.curr             := l_f_curr                                                                 ;
                  l_je_line_ins.amount           := ABS(ROUND(t_mje_lines(rec).net_value,2))                                 ;
                  l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
                  l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
                  l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
                  l_je_line_ins.func_amount      := ABS(t_mje_lines(rec).net_value)*l_f_ex_rate                              ;
                  l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
                  l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.cr_cc_id         := NULL                                                                     ;
                  l_je_line_ins.cr_activity_type := NULL                                                                     ;
                  /*
                  l_je_line_ins.dr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Contract Liability'
                                                         ELSE 'Unbilled'
                                                    END                                                                      ;
                  l_je_line_ins.dr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_liab_accnt
                                                         ELSE l_ub_liab_accnt
                                                    END                                                                      ;
                                                    Ganga 11082019
                                                    */
                  l_je_line_ins.dr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Unbilled'
                                                         ELSE 'Contract Liability'
                                                    END                                                                      ;
                  l_je_line_ins.dr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_ub_liab_accnt
                                                         ELSE l_liab_accnt
                                                    END                                                                      ;
                  l_je_line_ins.client_id        := g_client_id                                                              ;
                  l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
                  l_je_line_ins.crtd_by          := g_user                                                                   ;
                  l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.updt_by          := g_user                                                                   ;
                  l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
                  l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
                  l_je_line_ins.book_id          := g_book_id                                                                ;
                  --20190618 Copying few more columns
                  l_je_line_ins.reference1       := l_business_unit                                                          ;
                  l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
                  l_je_line_ins.reference3       := l_atr10                                                                  ;
                  l_je_line_ins.reference4       := l_atr11                                                                  ;
                  l_je_line_ins.reference5       := 'Core : Core'                                                            ;
                  l_je_line_ins.reference6       := l_atr18                                                                  ;
                  l_je_line_ins.reference8       := l_atr8                                                                   ;
                  l_je_line_ins.reference9       := t_mje_lines(rec).prod_ctgry                                              ;
                  l_je_line_ins.reference14      := 'Y'                                                                      ;
                  l_je_line_ins.reference15      := t_mje_lines(rec).rc_id                                                   ;

                  write_log('Log: dr_activity_type 1: '||l_je_line_ins.dr_activity_type
                           ||'~cr_activity_type    1:~'||l_je_line_ins.cr_activity_type
                           ||'~l_je_line_ins.amount  ~'||l_je_line_ins.amount
                           ||'~t_mje_lines(rec).net_value~'||t_mje_lines(rec).net_value);

                  INSERT INTO rpro_splunk_custom_je_line_g
                  VALUES l_je_line_ins;
                  write_log('Log: Created debit line count : '||SQL%ROWCOUNT);
                  COMMIT;

                  /*Resetting the variable*/
                  l_je_line_ins  := NULL;

                  /*Creating Credit entry*/
                  l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
                  l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
                  l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
                  l_je_line_ins.curr             := l_f_curr                                                                 ;
                  l_je_line_ins.amount           := ABS(t_mje_lines(rec).net_value)                                          ;
                  l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
                  l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
                  l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
                  l_je_line_ins.func_amount      := ABS(t_mje_lines(rec).net_value)*l_f_ex_rate                              ;
                  l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
                  l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
                  l_je_line_ins.dr_activity_type := NULL                                                                     ;
                  l_je_line_ins.dr_cc_id         := NULL                                                                     ;
                  /*
                  l_je_line_ins.cr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Unbilled'
                                                         ELSE 'Contract Liability'
                                                    END                                                                      ;
                  l_je_line_ins.cr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_ub_liab_accnt
                                                         ELSE l_liab_accnt
                                                    END                                                                      ;
                                                    Ganga 11082019 */
                  l_je_line_ins.cr_activity_type := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN 'Contract Liability'
                                                         ELSE 'Unbilled'
                                                    END                                                                      ;
                  l_je_line_ins.cr_segment1      := CASE WHEN t_mje_lines(rec).net_value>0
                                                         THEN l_liab_accnt
                                                         ELSE l_ub_liab_accnt
                                                    END                                                                      ;
                  l_je_line_ins.client_id        := g_client_id                                                              ;
                  l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
                  l_je_line_ins.crtd_by          := g_user                                                                   ;
                  l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.updt_by          := g_user                                                                   ;
                  l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
                  l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
                  l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
                  l_je_line_ins.book_id          := g_book_id                                                                ;
                  --20190618 Copying few more columns
                  l_je_line_ins.reference1       := l_business_unit                                                          ;
                  l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
                  l_je_line_ins.reference3       := l_atr10                                                                  ;
                  l_je_line_ins.reference4       := l_atr11                                                                  ;
                  l_je_line_ins.reference5       := 'Core : Core'                                                            ;
                  l_je_line_ins.reference6       := l_atr18                                                                  ;
                  l_je_line_ins.reference8       := l_atr8                                                                   ;
                  l_je_line_ins.reference9       := t_mje_lines(rec).prod_ctgry                                              ;
                  l_je_line_ins.reference14      := 'Y'                                                                      ;
                  l_je_line_ins.reference15      := t_mje_lines(rec).rc_id                                                   ;

                  write_log('Log: dr_activity_type 2: '||l_je_line_ins.dr_activity_type
                           ||'~cr_activity_type    2:~'||l_je_line_ins.cr_activity_type
                           ||'~l_je_line_ins.amount  ~'||l_je_line_ins.amount
                           ||'~t_mje_lines(rec).net_value~'||t_mje_lines(rec).net_value);

                  INSERT INTO rpro_splunk_custom_je_line_g
                  VALUES l_je_line_ins;
                  write_log('Log: Created credit line count : '||SQL%ROWCOUNT);
                  COMMIT;
                  l_je_line_ins  := NULL;
               ELSIF  (    UPPER(t_mje_lines(rec).prod_ctgry) = UPPER('LICENSE - PERPETUAL')
                       OR UPPER(t_mje_lines(rec).prod_ctgry) = UPPER('LICENSE - TERM')    )
               THEN
                  l_license_amt := NVL(l_license_amt,0) + NVL(t_mje_lines(rec).net_value,0);
               END IF;
            END IF;
         END LOOP;
         IF l_license_amt <> 0
         THEN
            SELECT ub_liab_acct
                  ,def_segments
                  ,f_cur
                  ,f_ex_rate
                  ,business_unit
                  ,cstmr_nm
                  ,atr10
                  ,atr11
                  ,atr18
                  ,atr8
                  ,rc_id
            INTO   l_ub_liab_accnt
                  ,l_liab_accnt
                  ,l_f_curr
                  ,l_f_ex_rate
                  ,l_business_unit
                  ,l_cstmr_nm
                  ,l_atr10
                  ,l_atr11
                  ,l_atr18
                  ,l_atr8
                  ,l_rc_id
            FROM   rpro_rc_line_g
            WHERE  rc_id      = dt.rc_id
            AND    UPPER(prod_ctgry) IN ('LICENSE - PERPETUAL','LICENSE - TERM')
            AND    rownum     = 1;

            IF l_ub_liab_accnt IS NULL --20200130
               THEN
                  BEGIN
                     SELECT source_value
                     INTO   l_ub_liab_accnt
                     FROM   rpro_account_g
                     WHERE  acct_type_id = 'U'
                     AND    source_type  = 'CONSTANT';
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error: Unble to find the unbilled account~Exiting..the current prod ctgry');
                     EXIT;
                  END;
               END IF;

            /*Creating Debit entry*/
            l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
            l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
            l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
            l_je_line_ins.curr             := l_f_curr                                                                 ;
            l_je_line_ins.amount           := ABS(l_license_amt)                                                       ;
            l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
            l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
            l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
            l_je_line_ins.func_amount      := ABS(l_license_amt)*l_f_ex_rate                                           ;
            l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
            l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.cr_cc_id         := NULL                                                                     ;
            l_je_line_ins.cr_activity_type := NULL                                                                     ;
            /*
            l_je_line_ins.dr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Contract Liability'
                                                   ELSE 'Unbilled'
                                              END                                                                      ;
            l_je_line_ins.dr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_liab_accnt
                                                   ELSE l_ub_liab_accnt
                                              END                                                                      ;
                                             Ganga 11082019 */
            l_je_line_ins.dr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Unbilled'
                                                   ELSE 'Contract Liability'
                                              END                                                                      ;
            l_je_line_ins.dr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_ub_liab_accnt
                                                   ELSE l_liab_accnt
                                              END                                                                      ;
            l_je_line_ins.client_id        := g_client_id                                                              ;
            l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
            l_je_line_ins.crtd_by          := g_user                                                                   ;
            l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
            l_je_line_ins.updt_by          := g_user                                                                   ;
            l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
            l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
            l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
            l_je_line_ins.book_id          := g_book_id                                                                ;
            --20190618 Copying few more columns
            l_je_line_ins.reference1       := l_business_unit                                                          ;
            l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
            l_je_line_ins.reference3       := l_atr10                                                                  ;
            l_je_line_ins.reference4       := l_atr11                                                                  ;
            l_je_line_ins.reference5       := 'Core : Core'                                                            ;
            l_je_line_ins.reference6       := l_atr18                                                                  ;
            l_je_line_ins.reference8       := l_atr8                                                                   ;
            l_je_line_ins.reference9       := 'LICENSE - TERM'                                                         ;
            l_je_line_ins.reference14      := 'Y'                                                                      ;
            l_je_line_ins.reference15      := l_rc_id                                                                  ;

            write_log('Log: dr_activity_type 1: '||l_je_line_ins.dr_activity_type
                     ||'~cr_activity_type    1:~'||l_je_line_ins.cr_activity_type
                     ||'~l_je_line_ins.amount~'  ||l_je_line_ins.amount
                     ||'~l_license_amt~'         ||l_license_amt);

            INSERT INTO rpro_splunk_custom_je_line_g
            VALUES l_je_line_ins;
            write_log('Log: Created debit line count : '||SQL%ROWCOUNT);
            COMMIT;

            /*Resetting the variable*/
            l_je_line_ins  := NULL;

            /*Creating Credit entry*/
            l_je_line_ins.id               := rpro_utility_pkg.generate_id('RPRO_JE_LINE_ID_S',g_client_id)            ;
            l_je_line_ins.header_id        := l_je_head_ins.id                                                         ;
            l_je_line_ins.activity_type    := rpro_utility_pkg.get_lkp_val('ACTIVITY_TYPE_CODE','Activity Type1')      ;
            l_je_line_ins.curr             := l_f_curr                                                                 ;
            l_je_line_ins.amount           := ABS(l_license_amt)                                                       ;
            l_je_line_ins.ex_rate          := l_f_ex_rate                                                              ;--need to discuss
            l_je_line_ins.g_ex_rate        := 1                                                                        ;--need to discuss
            l_je_line_ins.ex_rate_date     := l_cur_prd_end_date                                                       ;--need to discuss
            l_je_line_ins.func_amount      := ABS(l_license_amt)*l_f_ex_rate                                           ;
            l_je_line_ins.reason_code      := rpro_utility_pkg.get_lkp_val('REASON_CODE','Reason Code1')               ;
            l_je_line_ins.description      := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.comments         := 'Expected Deferred '||l_prd_name                                          ;
            l_je_line_ins.dr_activity_type := NULL                                                                     ;
            l_je_line_ins.dr_cc_id         := NULL                                                                     ;
            /*
            l_je_line_ins.cr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Unbilled'
                                                   ELSE 'Contract Liability'
                                              END                                                                      ;
            l_je_line_ins.cr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_ub_liab_accnt
                                                   ELSE l_liab_accnt
                                              END                                                                      ;
                                             11082019 */
            l_je_line_ins.cr_activity_type := CASE WHEN l_license_amt>0
                                                   THEN 'Contract Liability'
                                                   ELSE 'Unbilled'
                                              END                                                                      ;
            l_je_line_ins.cr_segment1      := CASE WHEN l_license_amt>0
                                                   THEN l_liab_accnt
                                                   ELSE l_ub_liab_accnt
                                              END                                                                      ;
            l_je_line_ins.client_id        := g_client_id                                                              ;
            l_je_line_ins.crtd_prd_id      := l_prd_id                                                                 ;
            l_je_line_ins.crtd_by          := g_user                                                                   ;
            l_je_line_ins.crtd_dt          := SYSDATE                                                                  ;
            l_je_line_ins.updt_by          := g_user                                                                   ;
            l_je_line_ins.updt_dt          := SYSDATE                                                                  ;
            l_je_line_ins.indicators       := rpro_je_line_pkg.set_active_flag(rpro_je_line_pkg.g_set_default_ind,'Y') ;
            l_je_line_ins.sec_atr_val      := g_sec_atr_val                                                            ;
            l_je_line_ins.book_id          := g_book_id                                                                ;
            --20190618 Copying few more columns
            l_je_line_ins.reference1       := l_business_unit                                                          ;
            l_je_line_ins.reference2       := l_cstmr_nm                                                               ;
            l_je_line_ins.reference3       := l_atr10                                                                  ;
            l_je_line_ins.reference4       := l_atr11                                                                  ;
            l_je_line_ins.reference5       := 'Core : Core'                                                            ;
            l_je_line_ins.reference6       := l_atr18                                                                  ;
            l_je_line_ins.reference8       := l_atr8                                                                   ;
            l_je_line_ins.reference9       := 'LICENSE - TERM'                                                         ;
            l_je_line_ins.reference14      := 'Y'                                                                      ;
            l_je_line_ins.reference15      := l_rc_id                                                                  ;

            write_log('Log: dr_activity_type 2: '||l_je_line_ins.dr_activity_type
                     ||'~cr_activity_type    2:~'||l_je_line_ins.cr_activity_type
                     ||'~l_je_line_ins.amount  ~'||l_je_line_ins.amount
                     ||'~l_license_amt~'         ||l_license_amt);

            INSERT INTO rpro_splunk_custom_je_line_g
            VALUES l_je_line_ins;
            write_log('Log: Created credit line count : '||SQL%ROWCOUNT);
            COMMIT;
            l_je_line_ins  := NULL;
         END IF;
         l_license_amt := NULL;
         l_rc_id       := NULL;
      END LOOP;
      CLOSE c_mje_lines;
      IF  l_tot_net_unbill <> l_tot_net_value
      THEN
         UPDATE  rpro_splunk_custom_je_line_g    --20200331
         SET     reference14  = 'N'
         WHERE   reference15  = dt.rc_id;
      END IF;
      COMMIT;
   END LOOP;
   COMMIT;
EXCEPTION
WHEN OTHERS THEN
write_log('Log: create_mje_report_script : Main Exception@'||DBMS_UTILITY.format_error_backtrace||'~Error~'||SQLERRM);
END create_mje_report_script;

--------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE rpro_splk_unbill_rep(
    p_errbuf OUT VARCHAR2,
    p_retcode OUT NUMBER)
AS

  l_prd_id  NUMBER;
  l_proc    VARCHAR2(30):= 'RPRO_SPLK_UNBILL_REP';
  l_log_lvl NUMBER      :=2;
BEGIN
  rpro_utility_pkg.record_log_act ( L_PROC , 'Entered...Before Period deriviation' ,l_log_lvl );

  SELECT id
  INTO   l_prd_id
  FROM rpro_period_g;
  rpro_utility_pkg.record_log_act ( L_PROC , 'Derived Period '||l_prd_id ,l_log_lvl );

  IF l_prd_id IS NOT NULL THEN
    rpro_utility_pkg.record_log_act ( L_PROC , 'Cleaning data for Period'||l_prd_id ,l_log_lvl );

    DELETE FROM RPRO_SPLK_UNBILL_REP WHERE CRTD_PRD_ID = L_PRD_ID;

    rpro_utility_pkg.record_log_act ( L_PROC , 'Extracting data for Period'||l_prd_id ,l_log_lvl );

    INSERT
      INTO RPRO_SPLK_UNBILL_REP
        (
          RC_ID,
          LINE_ID,
          CRTD_PRD_ID,
          t_bld_amt,
          cv_amt,
          bld_pct,
          bld_cv_amt,
          EXT_SLL_PRC_TO_DT,
          CV_REC_AMT_TO_DT,
          T_BLD_CV_AMT,
          T_CV_AMT
        )
        (SELECT   a.rc_id,
                  a.id,
                  l_prd_id,
                  NVL((bld_rec_amt+bld_def_amt),0),
                  NVL(cv_amt,0),
                  CASE
                    WHEN NVL((bld_rec_amt+bld_def_amt),0) = 0
                    THEN 0
                    ELSE ROUND(100*(bld_rec_amt+bld_def_amt)/DECODE(ALCTBL_XT_PRC,0,1,ALCTBL_XT_PRC),2) --20190530
                  END,
                  NVL((a.cv_amt/DECODE(tot_cv,0,1,tot_cv))*tot_bld_cv,0),   --20190530
                  NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(id,'Revenue',l_prd_id),0),
                  NVL(rpro_splk_fgaap_custom_pkg.rpro_splk_schd_amt(id,'Adjustment Revenue',l_prd_id),0),
                  tot_bld_cv,
                  tot_cv
         FROM rpro_rc_line_g a ,
           ( WITH pending_bill_rcs AS
             (SELECT rc_id
              FROM   rpro_rc_line_g
              WHERE  1 =1
              GROUP BY rc_id
              HAVING SUM(alctbl_xt_prc) !=  SUM(bld_rec_amt) + SUM(bld_def_amt)
              )
             SELECT   a.rc_id,
                     SUM(ROUND(100*(bld_rec_amt+bld_def_amt)/DECODE(alctbl_xt_prc,0,1,alctbl_xt_prc),2)*cv_amt/100)tot_bld_cv,  --20190530
                     SUM(CV_AMT) tot_cv
             FROM rpro_rc_line_g a
                 ,pending_bill_rcs pbr
             WHERE 1       = 1
             AND   a.rc_id = pbr.rc_id
             AND   a.cv_amt  < 0
             GROUP BY a.rc_id

           ) b
         WHERE a.rc_id = b.rc_id
         AND   atr32                                             = 'Y'  --added as requested by sera  20190524
         AND   rpro_rc_line_pkg.get_cv_eligible_flag(indicators) = 'Y'  --added as requested by sera  20190524
        );

    rpro_utility_pkg.record_log_act ( L_PROC , 'Update total Values for Period '||l_prd_id ,l_log_lvl );

    UPDATE RPRO_SPLK_UNBILL_REP
      SET bld_cv_amt    = bld_pct*cv_amt/100
      WHERE cv_amt      < 0
        AND crtd_prd_id = L_PRD_ID;

    UPDATE RPRO_SPLK_UNBILL_REP
      SET bld_cv_amt    = (cv_amt/t_cv_amt)*t_bld_cv_amt
      WHERE cv_amt      > 0
        AND crtd_prd_id = L_PRD_ID;

    UPDATE RPRO_SPLK_UNBILL_REP
      SET T_REV_TO_DT       = NVL(EXT_SLL_PRC_TO_DT,0)+NVL(CV_REC_AMT_TO_DT,0) ,
        C_T_ALLOC_DREV_EBAL = case when ABS(CV_REC_AMT_TO_DT)>ABS(bld_cv_amt)
                               then
                               0
                               else
                                NVL(BLD_CV_AMT,0)-NVL(CV_REC_AMT_TO_DT,0)
                              end,
        C_ALLOC_UBILL_BAL   = case when ABS(CV_REC_AMT_TO_DT) > ABS(bld_cv_amt)
                               then
                               NVL(CV_REC_AMT_TO_DT,0)-NVL(BLD_CV_AMT,0)
                               else
                                0
                              end,
        T_TEDREV_EBAL       = NVL(CV_AMT,0)           -NVL(CV_REC_AMT_TO_DT,0)
      WHERE CRTD_PRD_ID     = L_PRD_ID;
      UPDATE RPRO_SPLK_UNBILL_REP
      SET T_DEF_CV_JE         =C_T_ALLOC_DREV_EBAL - T_TEDREV_EBAL,
        UNBILL_CV_JE        = C_ALLOC_UBILL_BAL
        WHERE crtd_prd_id = L_PRD_ID;
    COMMIT;

  END IF;

EXCEPTION

WHEN OTHERS THEN
  rpro_utility_pkg.record_log_act ( L_PROC , 'Exception raised while running report for '||l_prd_id||'~'||sqlerrm ,2 );

END;
-------------------------------------------------------------------
   PROCEDURE call_lvl2_pct_calc (p_atr41  VARCHAR2 )
   AS

      CURSOR c_rec IS  SELECT *
                       FROM   rpro_splunk_gtt_tbl
                       WHERE  1            =  1
                       AND    atr41        =  p_atr41     ---Reference sales order line id
                       AND    atr60        IS NULL
                       ORDER BY num15                     ---Internal column used in the GTT table to store the sequence for processing
                               ,atr41 ASC
                               ;

      TYPE typ_rec IS TABLE OF c_rec%ROWTYPE
      INDEX BY PLS_INTEGER;
      l_prod_ctgry            VARCHAR2(240);
      l_total_fv_amt          NUMBER :=0 ;
      l_srvc_line             NUMBER :=0 ;
      l_pct                   NUMBER :=0 ;
      l_maintenance_cnt       NUMBER :=0 ;
      l_parent_sum            NUMBER :=0 ;
      l_overall_sum           NUMBER :=0 ;
      l_chld_sum              NUMBER :=0 ;
      l_get_off_bndl_val      NUMBER ;
      l_chk_prm_bndl          VARCHAR2(3);
      l_get_sub_bndl_val      NUMBER :=0 ;
      l_bundle_count          NUMBER :=NULL ;
      l_chk_lp_cnt            NUMBER :=0 ;
      l_sum_pct               NUMBER  ;
      l_succ_off_bndl_main    NUMBER := rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', 'SUCCESS_OFF_MAIN') ; ---Need To Create Lookup In Splunk Instance
      l_succ_off_bndl_sub     NUMBER := rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', 'SUCCESS_OFF_SUB') ; ---Need To Create Lookup In Splunk Instance
      l_premium_off_bndl_main NUMBER := rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', 'PREM_OFF_MAIN');---Need To Create Lookup In Splunk Instance
      l_premium_off_bndl_sub  NUMBER := rpro_utility_pkg.GET_LKP_VAL('SPLK LVL2 PERCENTAGE', 'PREM_OFF_SUB') ;---Need To Create Lookup In Splunk Instance
      l_rec TYP_rec;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      BEGIN
         SELECT UPPER(prod_ctgry)
         INTO   l_prod_ctgry
         FROM   rpro_splunk_gtt_tbl
         WHERE  doc_line_id  = atr41
         AND    atr41        = p_atr41;
      EXCEPTION
      WHEN OTHERS THEN
         l_prod_ctgry :='SUBSCRIPTION';
      END;

      IF  l_prod_ctgry = 'SUBSCRIPTION'
      THEN
         UPDATE rpro_splunk_gtt_tbl gtt
         SET    num15       =    ( SELECT NVL(rlv.lookup_value,10000)   --LV20200824
                                   FROM   rpro_lkp_g     rl
                                         ,rpro_lkp_val_g rlv
                                   WHERE rl.name         IN ('PRODUCT CATEGORY SUBSCRIPTION')
                                   AND   rl.id           = rlv.lookup_id
                                   AND   rlv.lookup_code = UPPER(gtt.prod_ctgry)
                                   AND   rlv.active_flag = 'Y' )
               ,atr60       =   NULL
         WHERE atr41        =   p_atr41
         AND   num15        IS NULL;
         --20200826
         UPDATE rpro_splunk_gtt_tbl gtt
         SET    num15       =    10000
               ,atr60       =   NULL
         WHERE atr41        =   p_atr41
         AND   num15        IS NULL;
      ELSIF  l_prod_ctgry = 'LICENSE - TERM'
      THEN
         UPDATE rpro_splunk_gtt_tbl gtt
         SET    num15       =   ( SELECT NVL(rlv.lookup_value,10000) --LV20200824
                                   FROM   rpro_lkp_g    rl
                                         ,rpro_lkp_val_g rlv
                                   WHERE rl.name         IN ('PRODUCT CATEGORY LICENSE - TERM')
                                   AND   rl.id           = rlv.lookup_id
                                   AND   rlv.lookup_code = UPPER(gtt.prod_ctgry)
                                   AND   rlv.active_flag = 'Y' )
               ,atr60       =   NULL
         WHERE atr41        =   p_atr41
         AND   num15        IS NULL;
         --20200826
         UPDATE rpro_splunk_gtt_tbl gtt
         SET    num15       =    10000
               ,atr60       =   NULL
         WHERE atr41        =   p_atr41
         AND   num15        IS NULL;
      END IF;
      
      
       --UPDATE rpro_splunk_gtt_tbl gtt
       --SET    num7       =    ext_fv_prc
       --WHERE  atr41        =   p_atr41
       --AND    num7 IS NULL;
         
      FOR rec IN c_rec
      LOOP
         --rec.num7 := NVL(rec.num7,rec.ext_fv_prc);
         IF l_bundle_count IS NULL
         THEN
            write_log('Log : num15  : '||rec.num15
                       ||CHR(10)
                       ||'Prod ctgry : '||rec.prod_ctgry);
            BEGIN
               ---Checking for total number of lines exists for that lines.
               SELECT  COUNT(1)
               INTO    l_bundle_count
               FROM    rpro_splunk_gtt_tbl
               WHERE   atr41             = rec.atr41;

               write_log('Log : total count  : '||l_bundle_count);
            EXCEPTION
            WHEN OTHERS THEN
               l_bundle_count := 0;
            END;

            BEGIN
               ---Checking for number of maintenance lines exists
               SELECT  COUNT(1)
               INTO    l_maintenance_cnt
               FROM    rpro_splunk_gtt_tbl
               WHERE   UPPER(prod_ctgry) IN ('MAINTENANCE - NEW','SUPPORT')
               AND     atr41             = rec.atr41;

               write_log('Log : l_maintenance_cnt  : '||l_maintenance_cnt);
            EXCEPTION
            WHEN OTHERS THEN
               l_maintenance_cnt := 0;
            END;

            BEGIN
               ---Checking for number of maintenance lines exists
               SELECT  NVL(atr48,'N')
               INTO    l_chk_prm_bndl
               FROM    rpro_splunk_gtt_tbl
               WHERE   UPPER(prod_ctgry) IN ( 'MAINTENANCE - NEW','SUPPORT')
               AND     atr41             = rec.atr41
               GROUP   BY  atr48;

               write_log('Log : l_chk_prm_bndl  : '||l_chk_prm_bndl);

            EXCEPTION
            WHEN OTHERS THEN
               l_chk_prm_bndl := 'N';
            END;

            ---Taking the sum of parent  num7 value(num7 have lvl1 fv price value)
            BEGIN
               write_log('Log : rec.atr41  : '||rec.atr41);
               SELECT  SUM(num7)
               INTO    l_parent_sum
               FROM    rpro_splunk_gtt_tbl
               WHERE   atr41             = rec.atr41
               AND     doc_line_id       = atr41 ;   ---Parent line

               write_log('Log : l_parent_sum  : '||l_parent_sum);

            EXCEPTION
            WHEN OTHERS THEN
               l_parent_sum := NULL;
            END;

            ---Taking the sum of  child num7 value(num7 have lvl1 fv price value)
            BEGIN
               SELECT  SUM(num7)
               INTO    l_chld_sum
               FROM    rpro_splunk_gtt_tbl
               WHERE   atr41              = rec.atr41
               AND     doc_line_id       != atr41 ;

               write_log('Log : l_chld_sum  : '||l_chld_sum);
            EXCEPTION
            WHEN OTHERS THEN
               l_chld_sum    := 0;
            END;

            ---Adding both parent and child amount to find the overall sum amount of that set of parent and child
            l_overall_sum := l_chld_sum+l_parent_sum;

            write_log('Log : Amount : '||CHR(10)
                                       ||'l_parent_sum  : ' ||l_parent_sum
                                       ||CHR(10)
                                       ||'l_chld_sum  : '   ||l_chld_sum
                                       ||CHR(10)
                                       ||'l_overall_sum  : '||l_overall_sum);
         END IF;
         ---Deriving the divisor value
         ---Based on success and premium bndl flag divisor value for subscription and maintenance lines.
         ---If there is no maintenance available for that set then divisor value will ve always 1
         IF  l_maintenance_cnt  >  0
         AND l_chk_prm_bndl     =  'N'
         THEN
            l_get_off_bndl_val  :=  l_succ_off_bndl_main;
            l_get_sub_bndl_val  :=  l_succ_off_bndl_sub;
            write_log('Log : Subscription cloud bundle : '||CHR(10)
                                       ||'l_get_off_bndl_val  : ' ||l_get_off_bndl_val
                                       ||CHR(10)
                                       ||'l_get_sub_bndl_val  : ' ||l_get_sub_bndl_val );
         ELSIF l_maintenance_cnt  >  0
         AND   l_chk_prm_bndl     =  'Y'
         THEN
            l_get_off_bndl_val  :=  l_premium_off_bndl_main;
            l_get_sub_bndl_val  :=  l_premium_off_bndl_sub;
            write_log('Log : premium cloud bundle : '||CHR(10)
                                       ||'l_get_off_bndl_val  : ' ||l_get_off_bndl_val
                                       ||CHR(10)
                                       ||'l_get_sub_bndl_val  : ' ||l_get_sub_bndl_val );
         ELSE
            l_get_off_bndl_val  := 1;
            l_get_sub_bndl_val  := 1;
            write_log('Log : only service line : '||CHR(10)
                                       ||'l_get_off_bndl_val  : ' ||l_get_off_bndl_val
                                       ||CHR(10)
                                       ||'l_get_sub_bndl_val  : ' ||l_get_sub_bndl_val );
         END IF;

         ---Calculating the pct amount for each product category
         IF  UPPER(rec.prod_ctgry)  IN ('LICENSE - TERM', 'SUBSCRIPTION')
         THEN
            write_log('Log : SUB l_get_sub_bndl_val: '||l_get_sub_bndl_val);
           --- l_pct:=((rec.num7/l_overall_sum)/l_get_sub_bndl_val)*100;
            l_pct:=((rec.num7/l_overall_sum) * l_get_sub_bndl_val)*100;
            write_log('Log : subscription lines percentage: '||l_pct);
         ELSIF UPPER(rec.prod_ctgry)  IN( 'MAINTENANCE - NEW','SUPPORT')
         THEN
            write_log('Log : Maintenance l_get_sub_bndl_val: '||l_get_sub_bndl_val);
            l_pct:=((l_parent_sum/l_overall_sum)*l_get_off_bndl_val)*100;
            write_log('Log : Maintenance new lines percentage: '||l_pct);
         ELSE--IF  UPPER(rec.prod_ctgry) LIKE '%SERVICES%'
        -- THEN
            l_pct:=(rec.num7/l_overall_sum)*100;
            write_log('Log : service lines percentage: '||l_pct);
         END IF;
         l_chk_lp_cnt := l_chk_lp_cnt+1;

         IF  l_bundle_count = l_chk_lp_cnt
         THEN
            l_pct := 100-ROUND(l_sum_pct,15);  --16112018
         ELSE
            l_pct := ROUND(l_pct,15);          --16112018
            l_sum_pct := nvl(l_sum_pct,0)+l_pct;
         END IF;
        write_log('derived pct:'||l_pct||'~atr41~'||rec.atr41);
         UPDATE rpro_splunk_gtt_tbl
         SET    lvl2_alloc_pct = l_pct,
                atr60          = 'PCT Calc for this bundle completed successfully'
         WHERE  id             =  rec.id;
         l_pct := 0; --LV20200903
      END LOOP;
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error : Main Exception in call_lvl2_pct_calc : '
                           ||'Error : '      ||SQLERRM
                           ||CHR(10)
                           ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END  call_lvl2_pct_calc;

   FUNCTION get_rssp_flag (p_atr41 IN VARCHAR2)
   RETURN VARCHAR2
   IS
      l_rssp_flag  VARCHAR2(10);
   BEGIN
      SELECT 'Y'
      INTO   l_rssp_flag
      FROM   rpro_splunk_gtt_tbl
      WHERE  atr41   =   p_atr41
      AND    fv_type =  'RSSP'
      AND    ROWNUM  =   1;
      write_log( '  l_rssp_flag  : '||l_rssp_flag);
      RETURN (l_rssp_flag);
   EXCEPTION
   WHEN OTHERS THEN
      l_rssp_flag  := 'N';
      write_log(' Exception marking l_rssp_flag as N ');
      RETURN (l_rssp_flag);
   END get_rssp_flag;

   /*Taking the backup of the orig ext fv price that derived during the lv1 allocation and resetting
     the child lines ext fv price as 0$*/
   PROCEDURE rpro_fv_bkp_prc ( p_rc_id       NUMBER
                              ,p_batch_id    NUMBER )
   AS
      r_idx                   NUMBER;
      l_rssp_flag             VARCHAR2(150);
      l_term                  rpro_rc_line_g.term%TYPE;
      l_qty                   rpro_rc_line_g.ord_qty%TYPE;
      l_fv_prc                rpro_rc_line_g.num1%TYPE;
   BEGIN
      write_log('********************************************************************');
      write_log('Start time rpro_fv_bkp_prc : '||SYSDATE);
      g_rc_id  := p_rc_id;
      rpro_utility_pkg.set_revpro_context;
      --Initializing the index for looping
      r_idx := rpro_rc_collect_pkg.g_rc_line_data.FIRST;


      WHILE r_idx IS NOT NULL
      LOOP
         l_rssp_flag   :=   get_rssp_flag (rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41 )  ;
         write_log('Log : Inside loop  : '||CHR(10)
                                     ||'Success offering bundle : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46
                                     ||CHR(10)
                                     ||'Premium offering bundle : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr48
                                     ||CHR(10)
                                     ||'RC ID : '                  ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).rc_id
                                     ||CHR(10)
                                     ||'RC level line id : '       ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
                                     ||CHR(10)
                                     ||'Product category : '       ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry
                                     ||CHR(10)
                                     ||'so line id : '             ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id
                                     ||CHR(10)
                                     ||'ref so line id : '         ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
                                     ||CHR(10)
                                     ||'  l_rssp_flag        :     '||l_rssp_flag);

         IF   rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46          = 'Y'          ---Checking line is success offering bundle  LV20200401
         AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr58          = 'Y'
         AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id    =  rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
         AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).err_msg        IS NULL
         AND  l_rssp_flag                                               =  'N'
         THEN
           --- rpro_rc_collect_pkg.g_rc_line_data(r_idx).lvl2_alloc_pct := NULL;
           -- rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators,'N');
           --- rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41         := rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id;
            l_term   := rpro_rc_collect_pkg.g_rc_line_data (r_idx).term          ;
            l_qty    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty       ;
            l_fv_prc := rpro_rc_collect_pkg.g_rc_line_data (r_idx).num1          ;
            ---rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc;  ---Taking the backup of parent line ext fv price to num7 column
            rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := l_fv_prc * l_term * l_qty;
            ----rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7          := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc;   ---Taking the backup of parent line ext fv price to num7 column
            write_log(' ID  : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
                      || CHR(10)
                      ||' num7 '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7
                      || CHR(10)
                      ||' ext_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc
                      || CHR(10)
                      ||' below_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc );
         END IF;

         IF  rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id                                       != rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41   ---Reference sales order line id
         AND NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46,'N')                                    = 'Y'       ---Checking child line
         ---AND rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators) = 'Y'
         ---AND rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators) = 'Y'
            AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr58                                                = 'Y'
         AND rpro_rc_collect_pkg.g_rc_line_data (r_idx).err_msg                                           IS NULL
         AND  l_rssp_flag                                                                                 =  'N'
         THEN
            IF  UPPER(rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry) IN ( 'MAINTENANCE - NEW','SUPPORT')
            THEN
               rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := 0;
            ELSE
               rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc;---Taking the backup of child lines  below ext fv price to num7 column
            END IF;
            write_log(' ID  : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
                      || CHR(10)
                      ||' num7 '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7
                      || CHR(10)
                      ||' ext_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc
                      || CHR(10)
                      ||' below_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc );
            ---rpro_rc_collect_pkg.g_rc_line_data(r_idx).lvl2_alloc_pct := NULL;
            ---rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators,'N');
            rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc    := 0;                                                       ---Defaulting the child line ext fv price to 0$
            rpro_rc_collect_pkg.g_rc_line_data (r_idx).fv_prc        := 0;

         END IF;

         r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);

      END LOOP;

      write_log('End time rpro_fv_bkp_prc : '||SYSDATE);
      write_log('********************************************************************');
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error : Main Exception in rpro_fv_bkp_prc : '
                           ||'Error : '      ||SQLERRM
                           ||CHR(10)
                           ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END rpro_fv_bkp_prc;

   /*calculating the lvl2 percentage*/
   PROCEDURE rpro_lvl2_pct_drv ( p_rc_id       NUMBER
                                ,p_batch_id    NUMBER )
   AS
      r_idx                   NUMBER ;
      l_pct                   NUMBER :=0 ;
      l_srvc_line             NUMBER :=0;
      l_rssp_chk              NUMBER := 0;
      l_term                  rpro_rc_line_g.term%TYPE;
      l_qty                   rpro_rc_line_g.ord_qty%TYPE;
      l_fv_prc                rpro_rc_line_g.num1%TYPE;

   BEGIN
      write_log('********************************************************************');
      write_log('Start time rpro_lvl2_pct_drv : '||SYSDATE);
      g_rc_id  := p_rc_id;
      rpro_utility_pkg.set_revpro_context;
      --Initializing the index for looping

     --- r_idx:=rpro_rc_collect_pkg.g_rc_line_data.FIRST;
     ---
     ---
     --- ---Inserting the records in the GTT table for future calculation
     --- ---Used two gtt table one for parent and another for child
     --- WHILE r_idx IS NOT NULL
     --- LOOP
     ---    ---Inserting the parent lines
     ---    IF   rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46                                             = 'Y'          ---Checking line is success offering bundle
     ---    AND  rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators) = 'Y'
     ---    AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).err_msg                                           IS NULL
     ---    ---AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).fv_type                                           = 'SSP'
     ---    THEN
     ---       write_log('Log : Parent line insertion  : '||CHR(10)
     ---                                                  ||'Success offering bundle : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46
     ---                                                  ||CHR(10)
     ---                                                  ||'Premium offering bundle : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr48
     ---                                                  ||CHR(10)
     ---                                                  ||'RC ID : '                  ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).rc_id
     ---                                                  ||CHR(10)
     ---                                                  ||'RC level line id : '       ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
     ---                                                  ||CHR(10)
     ---                                                  ||'Product category : '       ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry
     ---                                                  ||CHR(10)
     ---                                                  ||'so line id : '             ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id
     ---                                                  ||CHR(10)
     ---                                                  ||'ref so line id : '         ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41);
     ---       IF  rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41                                             IS NOT NULL     ---Reference sales order line id
     ---       AND NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46,'N')                                    = 'Y'
     ---       AND rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators) = 'Y'
     ---       AND rpro_rc_collect_pkg.g_rc_line_data (r_idx).err_msg                                           IS NULL
     ---     ---  AND  get_rssp_flag (rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41 )                           =  'N'
     ---       ---AND rpro_rc_collect_pkg.g_rc_line_data (r_idx).fv_type                                           = 'SSP'
     ---       THEN
     ---          write_log(' ID  : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
     ---                    || CHR(10)
     ---                    ||' ext_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc
     ---                    || CHR(10)
     ---                    ||' below_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc );
     ---          IF  UPPER(rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry) IN ( 'MAINTENANCE - NEW','SUPPORT') AND
     ---              rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id                                       != rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
     ---          THEN
     ---             rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := 0;
     ---          ELSE
     ---             IF rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id   != rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
     ---             THEN
     ---               -- l_term   := rpro_rc_collect_pkg.g_rc_line_data (r_idx).term          ;
     ---               -- l_qty    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty       ;
     ---               -- l_fv_prc := rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc  ;
     ---               -- rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := l_fv_prc * l_term * l_qty;  ---Taking the backup of child lines below ext fv price to num7 column
     ---                rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc;  ---Taking the backup of child lines below ext fv price to num7 column
     ---             ELSIF  rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id  = rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
     ---             THEN
     ---                l_term   := rpro_rc_collect_pkg.g_rc_line_data (r_idx).term          ;
     ---                l_qty    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty       ;
     ---                l_fv_prc := rpro_rc_collect_pkg.g_rc_line_data (r_idx).num1  ;
     ---                ---rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc;  ---Taking the backup of parent line ext fv price to num7 column
     ---                rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := l_fv_prc * l_term * l_qty;
     ---             END IF;
     ---          END IF;
     ---
     ---         ---- IF rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id             =   rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
     ---         ----
     ---         ---- END IF;
     ---       END IF;
     ---       ---Inserting the parent record
     ---       INSERT INTO rpro_splunk_gtt_tbl
     ---       VALUES      rpro_rc_collect_pkg.g_rc_line_data (r_idx);
     ---
     ---    END IF;
     ---    r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);
     --- END LOOP;
     ---
     --- FOR r_rec IN (SELECT DISTINCT atr41 parent_doc_line_id
     ---               FROM   rpro_splunk_gtt_tbl
     ---               WHERE  fv_type != 'SSP')
     --- LOOP
     ---    DELETE
     ---    FROM   rpro_splunk_gtt_tbl
     ---    WHERE  atr41 = r_rec.parent_doc_line_id;
     --- END LOOP;
     --- ----Step2 take backup
     --- write_log(' Before calling backup rpro_fv_bkp_prc ' );
     --- rpro_fv_bkp_prc ( p_rc_id      => p_rc_id
     ---                  ,p_batch_id   => p_batch_id );
     --- write_log(' After calling backup rpro_fv_bkp_prc ' );
      ---Calculating the lvl2 fv pct
    ---- r_idx:=rpro_rc_collect_pkg.g_rc_line_data.FIRST;
    ---- WHILE r_idx IS NOT NULL
    ---- LOOP
    ----    rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators  := rpro_rc_line_pkg.set_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators,rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr58);
    ----    r_idx                                                  := rpro_rc_collect_pkg.g_rc_line_data.NEXT (r_idx);
    ---- END LOOP;

      r_idx :=  rpro_rc_collect_pkg.g_rc_line_data.FIRST;

      WHILE r_idx IS NOT NULL
      LOOP
         ---Checking for maintenance count for that group of sales order line id

         call_lvl2_pct_calc(rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41);

         BEGIN
            SELECT lvl2_alloc_pct
            INTO   l_pct
            FROM   rpro_splunk_gtt_tbl
            WHERE  1 = 1
            AND    doc_line_id = rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_line_id;
            write_log('Log : l_pct  : '||l_pct
                       ||CHR(10)
                       || ' doc_line_id  : '||rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_line_id);
         EXCEPTION
         WHEN OTHERS THEN
            l_pct := NULL;
         END;

         BEGIN
            SELECT  COUNT(1)
            INTO    l_srvc_line
            FROM    rpro_splunk_gtt_tbl
            WHERE   atr41              = rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41
            AND     UPPER(prod_ctgry)  LIKE '%SERVICES%';

            write_log('Log : l_srvc_line  : '||l_srvc_line);
         EXCEPTION
         WHEN OTHERS THEN
            l_srvc_line    := 0;
         END;

         BEGIN
            SELECT  COUNT(1)
            INTO    l_rssp_chk
            FROM    rpro_splunk_gtt_tbl
            WHERE   atr41              = rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41
            AND     fv_type            = 'RSSP';

            write_log('Log : l_rssp_chk  : '||l_rssp_chk);
         EXCEPTION
         WHEN OTHERS THEN
            l_rssp_chk    := 0;
         END;
         ---Reverting back to original
         ---IF  rpro_rc_collect_pkg.g_rc_line_data(r_idx).doc_line_id = rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41
         ---THEN
         ---   rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr41 := NULL;
         ---END IF;

         IF  l_srvc_line > 0
         AND l_rssp_chk  = 0---If Service line exists for that bundle then update else leave as is
         THEN
            write_log('Log :  lines percentage: '||l_pct);
            rpro_rc_collect_pkg.g_rc_line_data(r_idx).lvl2_alloc_pct := l_pct;
            rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators     := rpro_rc_line_pkg.set_update_or_insert_flag(rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators , 'U');
            rpro_rc_collect_pkg.g_rc_line_data(r_idx).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators,'Y');
         END IF;

         write_log('End time rpro_lvl2_pct_drv : '||rpro_rc_collect_pkg.g_rc_line_data(r_idx).NUM2);

         r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);

      END LOOP;

      write_log('End time rpro_lvl2_pct_drv : '||SYSDATE);
      write_log('********************************************************************');
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error : Main Exception in rpro_lvl2_pct_drv : '
                           ||'Error : '      ||SQLERRM
                           ||CHR(10)
                           ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END rpro_lvl2_pct_drv;


   PROCEDURE rpro_after_fv_calc ( p_rc_id       NUMBER
                                ,p_batch_id    NUMBER )
   AS
      r_idx                   NUMBER ;
      l_term                  rpro_rc_line_g.term%TYPE;
      l_qty                   rpro_rc_line_g.ord_qty%TYPE;
      l_fv_prc                rpro_rc_line_g.num1%TYPE;
      l_num_reset_allc        NUMBER;
      l_chk_ssp_count         NUMBER;                 --#03082019
      l_other_cnt             NUMBER;                 --#03082019
      l_cust_cnt              NUMBER :=0;

   BEGIN
      write_log('********************************************************************');
      write_log('Start time rpro_after_fv_calc : '||TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));
      g_rc_id  := p_rc_id;
      rpro_utility_pkg.set_revpro_context;

      r_idx:=rpro_rc_collect_pkg.g_rc_line_data.FIRST;

      l_num_reset_allc := rpro_rc_collect_pkg.g_rc_line_data.FIRST;

      l_chk_ssp_count:= 0;       --#03082019
      l_other_cnt    := 0;       --#03082019

      WHILE l_num_reset_allc IS NOT NULL
      LOOP
        rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).indicators     := rpro_rc_line_pkg.set_cv_eligible_lvl2_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).indicators,'N');
        rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).lvl2_alloc_pct := NULL;
        rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).num7           := NULL;
       --- rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).indicators     := rpro_rc_line_pkg.set_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).indicators,rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).atr58);

        ---#03082019
        IF rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).fv_type = 'SSP'
        THEN
           l_chk_ssp_count := l_chk_ssp_count + 1 ;
        ELSIF rpro_rc_collect_pkg.g_rc_line_data (l_num_reset_allc).fv_type <>'SSP'
        THEN
           l_other_cnt     := l_other_cnt + 1;
        END IF;
        ---#03082019

        l_num_reset_allc   := rpro_rc_collect_pkg.g_rc_line_data.NEXT (l_num_reset_allc);

      END LOOP;


      ---Inserting the records in the GTT table for future calculation
      ---Used two gtt table one for parent and another for child
      WHILE r_idx IS NOT NULL
      LOOP
         ---Inserting the parent lines
         IF   rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46                                             = 'Y'          ---Checking line is success offering bundle
         AND  rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators) = 'Y'
         AND  rpro_rc_collect_pkg.g_rc_line_data (r_idx).err_msg                                           IS NULL
         THEN
            write_log('Log : Parent line insertion  : '||CHR(10)
                                                       ||'Success offering bundle : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46
                                                       ||CHR(10)
                                                       ||'Premium offering bundle : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr48
                                                       ||CHR(10)
                                                       ||'RC ID : '                  ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).rc_id
                                                       ||CHR(10)
                                                       ||'RC level line id : '       ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
                                                       ||CHR(10)
                                                       ||'Product category : '       ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry
                                                       ||CHR(10)
                                                       ||'so line id : '             ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id
                                                       ||CHR(10)
                                                       ||'ref so line id : '         ||rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41);
            IF  rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41                                             IS NOT NULL     ---Reference sales order line id
            AND NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr46,'N')                                    = 'Y'
            AND rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators) = 'Y'
            AND rpro_rc_collect_pkg.g_rc_line_data (r_idx).err_msg                                           IS NULL
            THEN
               write_log(' ID  : '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
                         || CHR(10)
                         ||' ext_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_fv_prc
                         || CHR(10)
                         ||' below_fv_prc '||rpro_rc_collect_pkg.g_rc_line_data (r_idx).below_fv_prc );
               IF  UPPER(rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry)        IN ( 'MAINTENANCE - NEW','SUPPORT')
               AND rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id              != rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
               THEN
                  rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := 0;
               ELSE                                                                                                                          --#04022019
                  IF rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id            != rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
                  THEN

                     SELECT COUNT(1)
                     INTO   l_cust_cnt
                     FROM   RPRO_CUST_UI_TB10371_1
                     WHERE  UPPER(prod_ctgry)   = UPPER(rpro_rc_collect_pkg.g_rc_line_data(r_idx).prod_ctgry)
                     AND    UPPER(item_num)     = UPPER(rpro_rc_collect_pkg.g_rc_line_data(r_idx).item_num)
                     AND    TRUNC(SYSDATE)      BETWEEN NVL(eff_start_date,TRUNC(SYSDATE)) AND NVL(eff_end_date,TRUNC(SYSDATE));  ------#04122019

                     write_log(' l_cust_cnt :'||l_cust_cnt );
                     IF( rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr15                = 'Y'
                     AND l_chk_ssp_count                                                > 0   
                     --AND l_other_cnt                                                    = 0 LV20200914
                     )
                     AND l_cust_cnt                                                     >=1   ----#04122019
                     THEN
                        IF UPPER(rpro_rc_collect_pkg.g_rc_line_data(r_idx).prod_ctgry) = 'TRAINING'
                        THEN
                           rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := (rpro_rc_collect_pkg.g_rc_line_data (r_idx).num5) * rpro_rc_collect_pkg.g_rc_line_data (r_idx).term * 0.9*rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty;             -- LV20200915
                        ELSE
                           rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := (rpro_rc_collect_pkg.g_rc_line_data (r_idx).num5/12) * rpro_rc_collect_pkg.g_rc_line_data (r_idx).term * 0.9*rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty;             --#03082019  --#25042019  --LV20200914 LV20200915
                        END IF;
                        
                        write_log(' num7 :'||rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7 );
                        write_log('Prod ctgry : '||rpro_rc_collect_pkg.g_rc_line_data(r_idx).prod_ctgry            --#04122019
                                  || CHR(10)
                                  || 'Item_num :' ||rpro_rc_collect_pkg.g_rc_line_data(r_idx).item_num);
                     ELSE
                        rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ext_lst_prc;  ---Taking the backup of child lines below ext fv price to num7 column
                     END IF;
                  ELSIF  rpro_rc_collect_pkg.g_rc_line_data (r_idx).doc_line_id  = rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr41
                  THEN
                     l_term   := rpro_rc_collect_pkg.g_rc_line_data (r_idx).term          ;
                     l_qty    := rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty       ;
                     l_fv_prc := rpro_rc_collect_pkg.g_rc_line_data (r_idx).num1  ;
                     rpro_rc_collect_pkg.g_rc_line_data (r_idx).num7    := l_fv_prc * l_term * l_qty;
                  END IF;
               END IF;

            END IF;
            ---Inserting the parent record
            INSERT INTO rpro_splunk_gtt_tbl
            VALUES      rpro_rc_collect_pkg.g_rc_line_data (r_idx);

         END IF;
         r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);
      END LOOP;

      ----Step2 take backup
      write_log(' Before calling backup rpro_fv_bkp_prc ' );
      rpro_fv_bkp_prc ( p_rc_id      => p_rc_id
                       ,p_batch_id   => p_batch_id );
      write_log(' After calling backup rpro_fv_bkp_prc ' );
      write_log('End time rpro_after_fv_calc : '||TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));
      write_log('********************************************************************');
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error : Main Exception in rpro_after_fv_calc : '
                           ||'Error : '      ||SQLERRM
                           ||CHR(10)
                           ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END rpro_after_fv_calc;


   PROCEDURE before_fv ( p_rc_id       NUMBER
                        ,p_batch_id    NUMBER )
   AS
      r_idx                   NUMBER ;
      l_atr57                 rpro_rc_line_g.atr57%TYPE;
      l_cv_flag               rpro_rc_line_g.indicators%TYPE;
   BEGIN
      write_log('********************************************************************');
      write_log('Start time before_fv : '||SYSDATE);

      rpro_utility_pkg.set_revpro_context;
      --Initializing the index for looping

      r_idx:=rpro_rc_collect_pkg.g_rc_line_data.FIRST;

      ---Inserting the records in the GTT table for future calculation
      ---Used two gtt table one for parent and another for child
      WHILE r_idx IS NOT NULL
      LOOP
         l_atr57        :=   rpro_rc_collect_pkg.g_rc_line_data(r_idx).atr57;
         l_cv_flag      :=   rpro_rc_line_pkg.get_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators);
         rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators :=  rpro_rc_line_pkg.set_cv_eligible_flag(rpro_rc_collect_pkg.g_rc_line_data (r_idx).indicators, NVL(l_atr57,l_cv_flag));
         r_idx := rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);
      END LOOP;
      write_log('End time before_fv : '||TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));
      write_log('********************************************************************');
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error : Main Exception in before_fv : '
                           ||'Error : '      ||SQLERRM
                           ||CHR(10)
                           ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END before_fv;

   PROCEDURE mje_valdation   ( p_rc_id            NUMBER DEFAULT NULL
                              ,p_batch_id         NUMBER)
   IS

     l_errbuf              NUMBER;
     l_errmsg              VARCHAR2(2000);
     l_upload_id           rpro_upload.ID%TYPE;
     l_class_tab           rpro_upload.TABLE_NAME%TYPE;
     l_dept_tab            rpro_upload.TABLE_NAME%TYPE;
     l_loc_tab             rpro_upload.TABLE_NAME%TYPE;
     l_subs_tab            rpro_upload.TABLE_NAME%TYPE;
     l_acct_tab            rpro_upload.TABLE_NAME%TYPE;
     l_jerrcnt             NUMBER;
     l_error_msg           VARCHAR2 (32727);
     l_error_count         NUMBER := 0;

   BEGIN
      rpro_utility_pkg.set_revpro_context;

      write_log('Start mje_valdation with batch_id : ' || p_batch_id || ' at ' || TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));

      -- Validate and Populate DR_Activity_Type/CR_Activvity_Type with given DR_Segment_1/CR_Segement_1  ---
      BEGIN
         SELECT  ru.id
                ,ru.table_name
         INTO   l_upload_id
               ,l_acct_tab
         FROM  rpro_upload_g ru
         WHERE ru.name = 'SPLK Account Lookup Template'
         AND   ROWNUM  = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_acct_tab := NULL;
            write_error(' Error in SPLK Account Lookup Template : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
            -- Stamping Error of No Template defined
            l_error_count      := l_error_count + 1;
            log_je_error  (  p_header_id   => p_batch_id
                            ,p_line_id     => l_error_count
                            ,p_type        => 'ACCOUNT'
                            ,p_description => 'SPLK Account Lookup Template Not Found or Unable to Validate with SPLK Class Lookup Template'
                            ,p_client_id   => rpro_utility_pkg.g_client_id
                            ,p_sec_atr_val => rpro_utility_pkg.g_sec_atr_val
                            ,p_book_id     => rpro_utility_pkg.g_book_id );
       END;

      -- Class Validation ----

      BEGIN
         SELECT  ru.id
                ,ru.table_name
         INTO    l_upload_id
                ,l_class_tab
         FROM  rpro_upload_g ru
         WHERE ru.name = 'SPLK Class Lookup Template'
         AND   ROWNUM  = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_class_tab := NULL;
            write_error(' Error in SPLK Class Lookup Template : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
            -- Stamping Error of No Template defined
            l_error_count      := l_error_count + 1;
            log_je_error  (  p_header_id   => p_batch_id
                            ,p_line_id     => l_error_count
                            ,p_type        => 'CLASS'
                            ,p_description => 'SPLK Class Lookup Template Not Found or Unable to Validate with SPLK Class Lookup Template'
                            ,p_client_id   => rpro_utility_pkg.g_client_id
                            ,p_sec_atr_val => rpro_utility_pkg.g_sec_atr_val
                            ,p_book_id     => rpro_utility_pkg.g_book_id );
       END;

      -- Department Validation ----
       BEGIN
          SELECT  ru.id
                 ,ru.table_name
          INTO    l_upload_id
                 ,l_dept_tab
          FROM   rpro_upload_g ru
          WHERE  ru.name = 'SPLK Department Lookup Template'
          AND    ROWNUM  = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_dept_tab := NULL;
            write_error(' Error in SPLK Department Lookup Template : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
            -- Stamping Error of No Template defined
            l_error_count      := l_error_count + 1;
            log_je_error  (  p_header_id   => p_batch_id
                            ,p_line_id     => l_error_count
                            ,p_type        => 'DEPARTMENT'
                            ,p_description => 'SPLK Department Lookup Template Not Found or Unable to Validate with SPLK Department Lookup Template'
                            ,p_client_id   => rpro_utility_pkg.g_client_id
                            ,p_sec_atr_val => rpro_utility_pkg.g_sec_atr_val
                            ,p_book_id     => rpro_utility_pkg.g_book_id );
       END;

      -- Location Validation ----
       BEGIN
          SELECT  ru.id
                 ,ru.table_name
          INTO    l_upload_id
                 ,l_loc_tab
          FROM   rpro_upload_g ru
          WHERE  ru.name = 'SPLK Location Lookup Template'
          AND    ROWNUM  = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_loc_tab := NULL;
            write_error(' Error in SPLK Location Lookup Template : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
            -- Stamping Error of No Template defined
            l_error_count      := l_error_count + 1;
            log_je_error  (  p_header_id   => p_batch_id
                            ,p_line_id     => l_error_count
                            ,p_type        => 'LOCATION'
                            ,p_description => 'SPLK Location Lookup Template Not Found or Unable to Validate with SPLK Location Lookup Template'
                            ,p_client_id   => rpro_utility_pkg.g_client_id
                            ,p_sec_atr_val => rpro_utility_pkg.g_sec_atr_val
                            ,p_book_id     => rpro_utility_pkg.g_book_id );
       END;

      -- Subsidiary Validation ----
       BEGIN
          SELECT  ru.id
                 ,ru.table_name
          INTO    l_upload_id
                 ,l_subs_tab
          FROM   rpro_upload_g ru
          WHERE ru.name = 'SPLK Subsidiary Lookup Template'
          AND   ROWNUM  = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_subs_tab := NULL;
            write_error(' Error in SPLK Subsidiary Lookup Template : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
            -- Stamping Error of No Template defined
            l_error_count      := l_error_count + 1;
            log_je_error  (  p_header_id   => p_batch_id
                            ,p_line_id     => l_error_count
                            ,p_type        => 'SUBSIDIARY'
                            ,p_description => 'SPLK Subsidiary Lookup Template Not Found or Unable to Validate with SPLK Subsidiary Lookup Template'
                            ,p_client_id   => rpro_utility_pkg.g_client_id
                            ,p_sec_atr_val => rpro_utility_pkg.g_sec_atr_val
                            ,p_book_id     => rpro_utility_pkg.g_book_id );
       END;

       IF l_error_count > 0
       THEN
          UPDATE rpro_je_head
          SET    indicators = rpro_je_head_pkg.set_status_flag ( indicators, 'E')
                ,updt_by    = rpro_utility_pkg.g_user
                ,updt_dt    = SYSDATE
          WHERE id          = p_batch_id;
       END IF;

       validate_mje (     p_batch_id
                         ,l_class_tab
                         ,'SPLK Class Lookup Template'
                         ,l_dept_tab
                         ,'SPLK Department Lookup Template'
                         ,l_loc_tab
                         ,'SPLK Location Lookup Template'
                         ,l_subs_tab
                         ,'SPLK Subsidiary Lookup Template'
                         ,l_acct_tab
                         ,'SPLK Account Lookup Template'
                         ,l_errbuf
                         ,l_errmsg
                     );
         IF l_errbuf != 0
         THEN
            UPDATE rpro_je_head
            SET    indicators = rpro_je_head_pkg.set_status_flag ( indicators, 'E')
                  ,updt_by    = rpro_utility_pkg.g_user
                  ,updt_dt    = SYSDATE
            WHERE id          = p_batch_id;
         ELSIF l_errbuf = 0
         THEN
            UPDATE rpro_je_head
            SET    indicators     = rpro_je_head_pkg.set_status_flag ( indicators, 'V')
                  ,updt_by        = rpro_utility_pkg.g_user
                  ,updt_dt        = SYSDATE
            WHERE id              = p_batch_id;
         END IF;
       COMMIT;
       write_log('End mje_valdation with batch_id : ' || p_batch_id || ' at ' || TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));
    EXCEPTION
    WHEN OTHERS THEN
       ROLLBACK;
       write_error(' Error in gen_val : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
   END mje_valdation;

   PROCEDURE validate_mje (     p_batch_id               NUMBER
                               ,p_cui_class_table        VARCHAR2
                               ,p_cui_class_name         VARCHAR2
                               ,p_cui_dept_table         VARCHAR2
                               ,p_cui_dept_name          VARCHAR2
                               ,p_cui_loc_table          VARCHAR2
                               ,p_cui_loc_name           VARCHAR2
                               ,p_cui_subs_table         VARCHAR2
                               ,p_cui_subs_name          VARCHAR2
                               ,p_cui_acct_table         VARCHAR2
                               ,p_cui_acct_name          VARCHAR2
                               ,p_errbuf          IN OUT NUMBER
                               ,p_errmsg          IN OUT VARCHAR2)
   IS

      l_je_line_tab                            all_tab_pkg.je_line_tab;
      l_je_head_rec                            rpro_je_head%ROWTYPE;
      l_bulk_limit                             NUMBER;
      l_idx                                    NUMBER;
      l_sql_stmt                               VARCHAR2(4000);
      l_bccur                                  SYS_REFCURSOR;
      l_account_number                         VARCHAR2(240);
      l_dr_segment                             rpro_je_line_g.dr_segment1%TYPE;
      l_cr_segment                             rpro_je_line_g.cr_segment1%TYPE;
      l_jerrcnt                                NUMBER;
      l_error_msg                              VARCHAR2 (32727);
      l_error_count                            NUMBER := 0;
      l_dr_acc_num                             NUMBER;
      l_cr_acc_num                             NUMBER;
      l_class_fld                              VARCHAR2(100) := 'class_name';
      l_dept_fld                               VARCHAR2(100) := 'department_name';
      l_loc_fld                                VARCHAR2(100) := 'location_name';
      l_subs_fld                               VARCHAR2(100) := 'subsidiary_name';
      l_acct_fld                               VARCHAR2(100) := 'account_number';
      l_cnt                                    PLS_INTEGER;
      l_class_name                             VARCHAR2(240);
      l_department_name                        VARCHAR2(240);
      l_location_name                          VARCHAR2(240);
      l_subsidiary_name                        VARCHAR2(240);

      TYPE typ_custui IS TABLE OF VARCHAR2(240)  INDEX BY VARCHAR2(240);
      l_class_ui                             typ_custui;
      l_dept_ui                              typ_custui;
      l_loc_ui                               typ_custui;
      l_subs_ui                              typ_custui;
      l_acct_ui                              typ_custui;

      TYPE typ_bulkcust IS TABLE OF VARCHAR2(240) INDEX BY PLS_INTEGER;
      l_bulkcust                           typ_bulkcust;

      CURSOR c_je_line IS
      SELECT *
      FROM rpro_je_line
      WHERE header_id = p_batch_id
      AND rpro_je_line_pkg.get_active_flag (indicators) = 'Y';
   BEGIN
      p_errbuf                 := 0;
      p_errmsg                 := 'Success';
      l_bulk_limit             := 10000;

       l_sql_stmt := NULL;
       IF p_cui_class_table IS NOT NULL
       THEN
          l_sql_stmt := q'{  (SELECT }' || l_class_fld || q'{
                             FROM }' || p_cui_class_table ||  q'{ GROUP BY }' || l_class_fld || q'{)}';   --EK 11132018
          write_log ( ' Query Framed :  ' || l_sql_stmt);
          OPEN l_bccur FOR l_sql_stmt;
          LOOP
             FETCH l_bccur BULK COLLECT
             INTO  l_bulkcust
             LIMIT l_bulk_limit;
             EXIT WHEN l_bulkcust.COUNT() = 0;

             FOR j IN 1..l_bulkcust.COUNT
             LOOP
                IF NOT l_class_ui.EXISTS(l_bulkcust(j))
                THEN
                   l_class_ui(l_bulkcust(j))     :=  l_bulkcust(j);
                END IF;
             END LOOP;
             write_log ('Log: count of l_bulkcust ~ ' || l_bulkcust.COUNT);
          END LOOP;
       END IF;
       l_sql_stmt := NULL;
       IF p_cui_dept_table IS NOT NULL
       THEN
          l_sql_stmt := q'{ (SELECT  }' || l_dept_fld || q'{
                                           FROM }' || p_cui_dept_table ||  q'{ GROUP BY }' || l_dept_fld || q'{)}';    --EK 11132018
          write_log ( ' Query Framed :  ' || l_sql_stmt);
          OPEN l_bccur FOR l_sql_stmt;
          LOOP
             FETCH l_bccur BULK COLLECT
             INTO  l_bulkcust
             LIMIT l_bulk_limit;
             EXIT WHEN l_bulkcust.COUNT() = 0;

             FOR j IN 1..l_bulkcust.COUNT
             LOOP
                IF NOT l_dept_ui.EXISTS(l_bulkcust(j))
                THEN
                   l_dept_ui(l_bulkcust(j))     :=  l_bulkcust(j);
                END IF;
             END LOOP;
             write_log ('Log: count of l_bulkcust ~ ' || l_bulkcust.COUNT);
          END LOOP;
       END IF;
       l_sql_stmt := NULL;
       IF p_cui_loc_table IS NOT NULL
       THEN
          l_sql_stmt := q'{  (SELECT  }' || l_loc_fld || q'{
                                           FROM }' || p_cui_loc_table ||  q'{ GROUP BY }' || l_loc_fld || q'{)}';    --EK 11132018
          write_log ( ' Query Framed :  ' || l_sql_stmt);
          OPEN l_bccur FOR l_sql_stmt;
          LOOP
             FETCH l_bccur BULK COLLECT
             INTO  l_bulkcust
             LIMIT l_bulk_limit;
             EXIT WHEN l_bulkcust.COUNT() = 0;

             FOR j IN 1..l_bulkcust.COUNT
             LOOP
                IF NOT l_loc_ui.EXISTS(l_bulkcust(j))
                THEN
                   l_loc_ui(l_bulkcust(j))     :=  l_bulkcust(j);
                END IF;
             END LOOP;
             write_log ('Log: count of l_bulkcust ~ ' || l_bulkcust.COUNT);
          END LOOP;
       END IF;
       l_sql_stmt := NULL;
       IF p_cui_subs_table IS NOT NULL
       THEN
          l_sql_stmt := q'{ (SELECT  }' || l_subs_fld || q'{
                                           FROM }' || p_cui_subs_table ||  q'{ GROUP BY }' || l_subs_fld || q'{)}';   --EK 11132018
          write_log ( ' Query Framed :  ' || l_sql_stmt);
          OPEN l_bccur FOR l_sql_stmt;
          LOOP
             FETCH l_bccur BULK COLLECT
             INTO  l_bulkcust
             LIMIT l_bulk_limit;
             EXIT WHEN l_bulkcust.COUNT() = 0;

             FOR j IN 1..l_bulkcust.COUNT
             LOOP
                IF NOT l_subs_ui.EXISTS(l_bulkcust(j))
                THEN
                   l_subs_ui(l_bulkcust(j))     :=  l_bulkcust(j);
                END IF;
             END LOOP;
             write_log ('Log: count of l_bulkcust ~ ' || l_bulkcust.COUNT);
          END LOOP;
       END IF;
       l_sql_stmt := NULL;
       IF p_cui_acct_table IS NOT NULL
       THEN
          l_sql_stmt := q'{ (SELECT  }' || l_acct_fld || q'{
                                           FROM }' || p_cui_acct_table ||  q'{ GROUP BY }' || l_acct_fld || q'{)}';  --EK 11132018
          write_log ( ' Query Framed :  ' || l_sql_stmt);
          OPEN l_bccur FOR l_sql_stmt;
          LOOP
             FETCH l_bccur BULK COLLECT
             INTO  l_bulkcust
             LIMIT l_bulk_limit;
             EXIT WHEN l_bulkcust.COUNT() = 0;

             FOR j IN 1..l_bulkcust.COUNT
             LOOP
                IF NOT l_acct_ui.EXISTS(l_bulkcust(j))
                THEN
                   l_acct_ui(l_bulkcust(j))     :=  l_bulkcust(j);
                END IF;
             END LOOP;
             dbms_output.put_line ('Log: count of l_bulkcust ~ ' || l_bulkcust.COUNT);
          END LOOP;
       END IF;

      OPEN c_je_line;
      LOOP
         FETCH c_je_line  BULK COLLECT
         INTO l_je_line_tab
         LIMIT l_bulk_limit;
         EXIT WHEN l_je_line_tab.COUNT = 0;

         l_idx            := l_je_line_tab.FIRST;
         WHILE l_idx IS NOT NULL
         LOOP
            l_class_name      := NULL;
            l_department_name := NULL;
            l_location_name   := NULL;
            l_subsidiary_name := NULL;
                    -- Mapping Reference5 (Class) with Cust UI ----
            l_class_name := NVL(l_je_line_tab(l_idx).reference5, '@');
            write_log ('l_class_name ~ ' || l_class_name);

            IF NOT l_class_ui.EXISTS(l_class_name) THEN
               l_error_count      := l_error_count + 1;
               log_je_error  (  p_header_id   => p_batch_id
                               ,p_line_id     => l_je_line_tab (l_idx).ID
                               ,p_type        => 'CLASS'
                               ,p_description => 'Class Name ' || REPLACE(l_class_name, '@') || ' Not present in ' || p_cui_class_name || ' for JE Line ' || l_je_line_tab (l_idx).ID
                               ,p_client_id   => l_je_line_tab (l_idx).client_id
                               ,p_sec_atr_val => l_je_line_tab (l_idx).sec_atr_val
                               ,p_book_id     => l_je_line_tab (l_idx).book_id );
            END IF;
                  -- Mapping Reference3 ( Department) with Cust UI ----
            l_department_name := NVL(l_je_line_tab(l_idx).reference3, '@');
            write_log ('l_department_name ~ ' || l_department_name);

            IF NOT l_dept_ui.EXISTS(l_department_name) THEN
               l_error_count      := l_error_count + 1;
               log_je_error  (  p_header_id   => p_batch_id
                               ,p_line_id     => l_je_line_tab (l_idx).ID
                               ,p_type        => 'DEPARTMENT'
                               ,p_description => 'Department Name ' || REPLACE(l_department_name, '@') || ' Not present in ' || p_cui_dept_name || ' for JE Line ' || l_je_line_tab (l_idx).ID
                               ,p_client_id   => l_je_line_tab (l_idx).client_id
                               ,p_sec_atr_val => l_je_line_tab (l_idx).sec_atr_val
                               ,p_book_id     => l_je_line_tab (l_idx).book_id );
            END IF;
                  -- Mapping Reference4 (Location) with Cust UI ----
            l_location_name := NVL(l_je_line_tab(l_idx).reference4, '@');
            write_log ('l_location_name ~ ' || l_location_name);

            IF NOT l_loc_ui.EXISTS(l_location_name) THEN
               l_error_count      := l_error_count + 1;
               log_je_error  (  p_header_id   => p_batch_id
                               ,p_line_id     => l_je_line_tab (l_idx).ID
                               ,p_type        => 'LOCATION'
                               ,p_description => 'Location Name ' || REPLACE(l_location_name, '@') || ' Not present in ' || p_cui_loc_name || ' for JE Line ' || l_je_line_tab (l_idx).ID
                               ,p_client_id   => l_je_line_tab (l_idx).client_id
                               ,p_sec_atr_val => l_je_line_tab (l_idx).sec_atr_val
                               ,p_book_id     => l_je_line_tab (l_idx).book_id );
            END IF;
                 -- Mapping Reference1 (Subsidiary) with Cust UI ----
            l_subsidiary_name := NVL(l_je_line_tab(l_idx).reference1, '@');
            write_log ('l_subsidiary_name ~ ' || l_subsidiary_name);

            IF NOT l_subs_ui.EXISTS(l_subsidiary_name) THEN
               l_error_count      := l_error_count + 1;
               log_je_error  (  p_header_id   => p_batch_id
                               ,p_line_id     => l_je_line_tab (l_idx).ID
                               ,p_type        => 'SUBSIDIARY'
                               ,p_description => 'Subsidiary Name ' || REPLACE(l_subsidiary_name, '@') || ' Not present in ' || p_cui_subs_name || ' for JE Line ' || l_je_line_tab (l_idx).ID
                               ,p_client_id   => l_je_line_tab (l_idx).client_id
                               ,p_sec_atr_val => l_je_line_tab (l_idx).sec_atr_val
                               ,p_book_id     => l_je_line_tab (l_idx).book_id );
            END IF;
                    -- Mapping dr/cr Segment_1 with Cust UI ----
            l_dr_segment := NULL;
            l_cr_segment := NULL;
            IF l_je_line_tab(l_idx).dr_segment1 IS NOT NULL
            THEN
               l_dr_segment := l_je_line_tab(l_idx).dr_segment1;
               BEGIN
                  l_dr_acc_num := TO_NUMBER(l_je_line_tab(l_idx).dr_segment1);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_dr_acc_num := -1;
               END;
            END IF;
            IF l_je_line_tab(l_idx).cr_segment1 IS NOT NULL
            THEN
               l_cr_segment := l_je_line_tab(l_idx).cr_segment1;
               BEGIN
                  l_cr_acc_num := TO_NUMBER(l_je_line_tab(l_idx).cr_segment1);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_cr_acc_num := -1;
               END;
            END IF;
            write_log ('l_dr_segment ~ ' || l_dr_segment || '~ l_cr_segment ~ ' || l_cr_segment);

            IF NOT l_acct_ui.EXISTS(l_dr_segment) AND l_dr_segment IS NOT NULL
            THEN
               l_error_count      := l_error_count + 1;
               log_je_error  (  p_header_id   => p_batch_id
                               ,p_line_id     => l_je_line_tab (l_idx).ID
                               ,p_type        => 'ACCOUNT'
                               ,p_description => 'Debit Segment Account Number ' || l_dr_segment || ' Not present in ' || p_cui_acct_name || ' for JE Line ' || l_je_line_tab (l_idx).ID
                               ,p_client_id   => l_je_line_tab (l_idx).client_id
                               ,p_sec_atr_val => l_je_line_tab (l_idx).sec_atr_val
                               ,p_book_id     => l_je_line_tab (l_idx).book_id );
            END IF;
            IF NOT l_acct_ui.EXISTS(l_cr_segment)  AND l_cr_segment IS NOT NULL
            THEN
               l_error_count      := l_error_count + 1;
               log_je_error  (  p_header_id   => p_batch_id
                               ,p_line_id     => l_je_line_tab (l_idx).ID
                               ,p_type        => 'ACCOUNT'
                               ,p_description => 'Credit Segment Account Number ' || l_cr_segment || ' Not present in ' || p_cui_acct_name || ' for JE Line ' || l_je_line_tab (l_idx).ID
                               ,p_client_id   => l_je_line_tab (l_idx).client_id
                               ,p_sec_atr_val => l_je_line_tab (l_idx).sec_atr_val
                               ,p_book_id     => l_je_line_tab (l_idx).book_id );
            END IF;
            IF l_error_count = 0
            THEN
               IF l_dr_segment IS NOT NULL
               THEN
                  l_je_line_tab(l_idx).dr_activity_type := CASE
                                                              WHEN l_dr_acc_num BETWEEN 1000 AND 1999 THEN 'Unbilled'
                                                              WHEN l_dr_acc_num BETWEEN 2000 AND 2999 THEN 'Contract Liability'
                                                              WHEN l_dr_acc_num BETWEEN 3000 AND 9999 THEN 'Revenue'
                                                           END;
                  UPDATE rpro_je_line_g
                  SET    dr_activity_type = l_je_line_tab(l_idx).dr_activity_type
                  WHERE  id = l_je_line_tab(l_idx).id;
               END IF;
               IF l_cr_segment IS NOT NULL
               THEN
                  l_je_line_tab(l_idx).cr_activity_type := CASE
                                                              WHEN l_cr_acc_num BETWEEN 1000 AND 1999 THEN 'Unbilled'
                                                              WHEN l_cr_acc_num BETWEEN 2000 AND 2999 THEN 'Contract Liability'
                                                              WHEN l_cr_acc_num BETWEEN 3000 AND 9999 THEN 'Revenue'
                                                           END;
                  UPDATE rpro_je_line_g
                  SET    cr_activity_type = l_je_line_tab(l_idx).cr_activity_type
                  WHERE  id = l_je_line_tab(l_idx).id;
               END IF;
            END IF;
            l_idx      := l_je_line_tab.NEXT (l_idx);
         END LOOP;
      END LOOP;
	  ------#MS17032020  Starts
      FOR i IN (SELECT reference1
                      ,client_id
                      ,sec_atr_val
                      ,book_id
                      ,SUM(amount) AMOUNT
                FROM
                  (SELECT reference1
                    ,header_id
                    ,client_id
                    ,sec_atr_val
                    ,book_id
                    ,CASE
                       WHEN cr_segment1 IS NOT NULL
                       THEN amount
                       WHEN dr_segment1 IS NOT NULL
                       THEN -1*amount
                     END amount
                  FROM rpro_je_line_g a
                  WHERE header_id=p_batch_id
                  )
                HAVING SUM(amount)<>0
                GROUP BY reference1
                        ,client_id
                        ,sec_atr_val
                        ,book_id)
      LOOP
         l_error_count      := l_error_count + 1;
         log_je_error  (  p_header_id   => p_batch_id
                         ,p_line_id     => null
                         ,p_type        => 'CR DR Mis match'
                         ,p_description => 'CR Amount and DR Amount is not matching for '||i.reference1
                         ,p_client_id   => i.client_id
                         ,p_sec_atr_val => i.sec_atr_val
                         ,p_book_id     => i.book_id );


      END LOOP;
      ------#MS17032020  Ends
      IF l_error_count > 0
      THEN
        p_errbuf      := 2;
        p_errmsg       := l_error_msg;
      ELSE
         COMMIT;
      END IF;

      EXCEPTION
      WHEN OTHERS THEN
        p_errbuf                 := 2;
        p_errmsg                 := ' Error in validate_mje : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace;
        write_error(p_errmsg);
   END validate_mje;

   PROCEDURE log_je_error    (  p_header_id   IN NUMBER
                               ,p_line_id     IN NUMBER
                               ,p_type        IN VARCHAR2
                               ,p_description IN rpro_je_error_g.type%TYPE
                               ,p_client_id   IN NUMBER
                               ,p_sec_atr_val IN rpro_je_error_g.sec_atr_val%TYPE
                               ,p_book_id     IN rpro_je_error_g.book_id%TYPE  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
      INSERT INTO rpro_je_error_g ( header_id
                                  ,line_id
                                  ,type
                                  ,description
                                  ,crtd_by
                                  ,crtd_dt
                                  ,client_id
                                  ,sec_atr_val
                                  ,book_id
                                  )
      VALUES
                              ( p_header_id
                               ,p_line_id
                               ,p_type
                               ,p_description
                               ,NULL
                               ,sysdate
                               ,p_client_id
                               ,p_sec_atr_val
                               ,p_book_id
                              );
      COMMIT;
   END log_je_error;

   FUNCTION chk_within_assp_flg( p_ord_qty       IN    NUMBER
                                ,p_prod_fmly     IN    VARCHAR2
                                ,p_prod_ctgry    IN    VARCHAR2
                                ,p_alctd_xt_prc  IN    NUMBER
                                ,p_term          IN    NUMBER   ---Introduced term parameter 29112018
                                ,p_atr47         IN    NUMBER   ---Introduced term parameter 29112018
                                )
   RETURN VARCHAR2
   IS
      l_stmt                VARCHAR2(32627);
      l_cust_tab            VARCHAR2(30);
      l_assp_within_flg     VARCHAR2(1);
   BEGIN
      /* Fetching Cust UI upload table name */
      BEGIN
         SELECT table_name
         INTO   l_cust_tab
         FROM   rpro_upload_g
         WHERE  UPPER(name)                                  = 'SPLK ASSP RANGE TEMPLATE'
         AND    rpro_upload_pkg.get_enabled_flag(indicators) = 'Y'
         AND    ROWNUM                                       = 1;
      EXCEPTION
      WHEN OTHERS THEN
         write_error('Exception while fetching table name  '||SQLERRM
                                                            ||CHR(10)
                                                            ||'Error Queue : '||dbms_utility.format_error_backtrace);
         RETURN NULL;
      END;
      /* Sql statement construction to fetch ASSP within range flag
         by checking allocated ext prc is between high and low range */
      BEGIN
         IF UPPER(p_prod_ctgry) = 'LICENSE - TERM'
         THEN
            l_stmt:= 'SELECT ( CASE
                               WHEN '||nvl(p_alctd_xt_prc,0)||' BETWEEN '|| p_ord_qty ||'*'||p_term||' * low/12
                                                                AND     '|| p_ord_qty ||'*'||p_term||' * high/12
                               THEN ''Y''
                               ELSE ''N''
                               END
                              )
                      FROM  '||l_cust_tab||'
                      WHERE '|| p_atr47 ||' BETWEEN tier_min AND tier_max
                      AND   fcst_product_family   =  '||''''||p_prod_fmly||''''||'
                      AND   product_category      =  '||''''||p_prod_ctgry||''''||'
                      AND   low                   <>  0
                      AND   high                  <>  0
                      AND   (batch_id,line_number)    IN  ( SELECT MAX(batch_id)
                                                                  ,MAX(line_number)
                                                            FROM   '||l_cust_tab||'
                                                            WHERE  1           = 1
                                                            AND    '|| p_atr47 ||' BETWEEN tier_min AND tier_max
                                                            AND    fcst_product_family   = '||''''||p_prod_fmly||''''||'
                                                            AND    product_category      = '||''''||p_prod_ctgry||''''||'
                                                            AND    low                   <>  0
                                                            AND    high                  <>  0 )';
         ELSIF UPPER(p_prod_ctgry) = 'LICENSE - PERPETUAL'
         THEN
            l_stmt:= 'SELECT ( CASE
                               WHEN '||nvl(p_alctd_xt_prc,0)||' BETWEEN '|| p_ord_qty ||' * low
                                                                AND     '|| p_ord_qty ||' * high
                               THEN ''Y''
                               ELSE ''N''
                               END
                              )
                      FROM  '||l_cust_tab||'
                      WHERE '|| p_atr47 ||' BETWEEN tier_min AND tier_max
                      AND   fcst_product_family   =  '||''''||p_prod_fmly||''''||'
                      AND   product_category      =  '||''''||p_prod_ctgry||''''||'
                      AND   low                   <>  0
                      AND   high                  <>  0
                      AND   (batch_id,line_number)    IN  ( SELECT MAX(batch_id)
                                                                  ,MAX(line_number)
                                                            FROM   '||l_cust_tab||'
                                                            WHERE  1           = 1
                                                            AND    '|| p_atr47 ||' BETWEEN tier_min AND tier_max
                                                            AND    fcst_product_family   = '||''''||p_prod_fmly||''''||'
                                                            AND    product_category      = '||''''||p_prod_ctgry||''''||'
                                                            AND    low                   <>  0
                                                            AND    high                  <>  0 )';
         END IF;

         write_log('SQL STATEMENT '||l_stmt);

         EXECUTE IMMEDIATE l_stmt
         INTO              l_assp_within_flg;
         EXCEPTION
         WHEN OTHERS THEN
            RETURN NULL;
            write_error('Exception while fetching ASSP within range flag  '||SQLERRM
                                                                           ||CHR(10)
                                                                           ||'Error Queue : '||dbms_utility.format_error_backtrace);

      END;
      RETURN l_assp_within_flg;

   EXCEPTION
   WHEN OTHERS THEN
      write_error('Exception in chk_within_assp_flg Func : '||SQLERRM
                                                            ||CHR(10)
                                                            ||'Error Queue : '||dbms_utility.format_error_backtrace);
      RETURN NULL;
   END chk_within_assp_flg;

   PROCEDURE alctd_prc_within_assp ( p_rc_id      IN     VARCHAR2 DEFAULT NULL
                                    ,p_batch_id   IN     NUMBER )
   IS
      l_idx                                 NUMBER := 0;
      l_flg                                 VARCHAR2(10);
      l_atr49                               VARCHAR2(10);
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('alctd_prc_within_assp Proc Start : '|| TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));

      l_idx := rpro_rc_collect_pkg.g_rc_line_data.FIRST;
      WHILE l_idx IS NOT NULL
      LOOP

         /* Checking FV_TYPE is RSSP or not */
         IF rpro_rc_collect_pkg.g_rc_line_data (l_idx).fv_type = 'RSSP'
         THEN
             l_atr49 := chk_within_assp_flg( rpro_rc_collect_pkg.g_rc_line_data (l_idx).ord_qty
                                               ,rpro_rc_collect_pkg.g_rc_line_data (l_idx).atr43
                                               ,rpro_rc_collect_pkg.g_rc_line_data (l_idx).prod_ctgry
                                               ,rpro_rc_collect_pkg.g_rc_line_data (l_idx).alctd_xt_prc
                                               ,rpro_rc_collect_pkg.g_rc_line_data (l_idx).term
                                               ,rpro_rc_collect_pkg.g_rc_line_data (l_idx).atr47);
            write_log('ASSP Range (atr49) flag  : '|| l_atr49);
            /* Assigning within assp range flg to ATR49 */
            rpro_rc_collect_pkg.g_rc_line_data (l_idx).atr49 := l_atr49;

            write_log('Id  : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).id);
            write_log('ASSP Range (atr49) flag  : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).atr49
                               ||' ord_qty      : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).atr47
                               ||' prod_fmly    : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).prod_fmly
                               ||' prod_ctgry   : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).prod_ctgry
                               ||' alctd_xt_prc : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).alctd_xt_prc
                               ||' Term         : '|| rpro_rc_collect_pkg.g_rc_line_data (l_idx).term);
         END IF;
         l_idx :=rpro_rc_collect_pkg.g_rc_line_data.NEXT(l_idx);
      END LOOP;
      write_log('alctd_prc_within_assp Proc End : '|| TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));

   EXCEPTION
   WHEN OTHERS THEN
      write_error(   'Exception in alctd_prc_within_assp Proc : '||SQLERRM
                                                                 ||CHR(10)
                                                                 ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END alctd_prc_within_assp;

   PROCEDURE splunk_qtd_wf_rep ( p_report_id       IN     NUMBER
                                ,p_rep_layout_id   IN     NUMBER
                                ,p_no_tags         IN     VARCHAR2 DEFAULT 'N'
                                ,p_restrict_rows   IN     VARCHAR2 DEFAULT 'N'
                                ,p_query              OUT VARCHAR2
                                ,p_header             OUT VARCHAR2
                                ,p_num_rows           OUT NUMBER
                                  )
   AS
      CURSOR c_per( p_min_prd_id    NUMBER
                   ,p_max_prd_id    NUMBER
                   ,p_forecast_flag VARCHAR2)
      IS
      WITH prd_cal
      AS (SELECT id                                                     AS prd_id
                ,period_name                                            AS prd_name
                ,end_date                                               AS prd_end_date
                ,period_year                                            AS year
                ,year_end_dt                                            AS year_end_date
                ,ROW_NUMBER () OVER (ORDER BY id)                       AS rnum
                ,NVL(rpro_utility_pkg.get_profile ('MAX_PERIODS'),9999) AS mx_prd_num
          FROM rpro_calendar
          WHERE id BETWEEN p_min_prd_id AND p_max_prd_id)
          SELECT DISTINCT  CASE WHEN c.rnum <= c.mx_prd_num THEN 'MON' ELSE 'YEAR' END prd_type
                          ,CASE WHEN c.rnum <= c.mx_prd_num THEN c.prd_id ELSE TO_NUMBER (C.YEAR || '99') END prd_sort
                          ,CASE WHEN c.rnum <= c.mx_prd_num THEN c.prd_id ELSE TO_NUMBER (C.YEAR) END prd_id
                          ,NVL(mx.prd_id,205012) AS mx_prd_id
                          ,CASE WHEN c.rnum <= c.mx_prd_num THEN TO_CHAR (c.prd_name)
                                WHEN c.rnum > c.mx_prd_num AND c.prd_end_date <= mx.year_end_date
                                THEN TO_CHAR('=> ' || c.year)
                           ELSE TO_CHAR (C.YEAR)
                           END  prd_lbl
          FROM prd_cal c  CROSS JOIN
                      (SELECT prd_id
                             ,prd_name
                             ,prd_end_date
                             ,year
                             ,year_end_date
                             ,rnum
                             ,mx_prd_num
                       FROM  prd_cal
                       WHERE rnum = mx_prd_num + 1
                       UNION
                       SELECT NULL
                             ,NULL
                             ,NULL
                             ,NULL
                             ,NULL
                             ,NULL
                             ,NULL
                       FROM dual d
                       WHERE NOT EXISTS(SELECT 1
                                        FROM  prd_cal c
                                        WHERE c.rnum = c.mx_prd_num + 1)) mx
      UNION ALL
      SELECT *
      FROM  (SELECT 'MON'          prd_type
                   ,-999           prd_sort
                   ,-999           prd_id
                   ,-999           mx_prd_id
                   ,'Prior Amount' prd_lbl
             FROM dual)
      WHERE p_forecast_flag <> 'F'
      UNION ALL  --#20191112
      SELECT   'CUST'            prd_type
               ,cal.id           prd_sort
               ,cal.id           prd_id
               ,cal.id           mx_prd_id
               ,cal.period_name  prd_lbl
      FROM      all_tab_columns tab
               ,rpro_calendar_g cal
      WHERE table_name='RPRO_WF_PRD_QTD_TBL'
      AND   TO_CHAR(cal.id)   = tab.column_name
      ORDER BY 2;


      CURSOR c_fields (p_rep_layout_id IN NUMBER)
      IS
      SELECT *
      FROM  ( SELECT NVL (rrf.fld_name, rlb.col_name)                      AS   col_name
                    ,NVL (SUBSTR (rlb.report_label,1,30),rlb.col_name)          label
                    ,NVL (rlb.col_type, 'VARCHAR2')                             col_type
                    ,NVL2 (alias, alias || '.', NULL)                           alias
                    ,rpro_layout_field_pkg.get_zero_check_flag (rlf.indicators) zero_check
                    ,rpro_layout_field_pkg.get_order_by_flag (rlf.indicators)   order_by
                    ,rrf.id
                    ,rlf.seq
              FROM  rpro_rep_field    rrf
                   ,rpro_rp_layout    rl
                   ,rpro_layout_field rlf
                   ,rpro_label        rlb
              WHERE rrf.rep_id                                               = rl.rep_id
              AND   rl.id                                                    = rlf.layout_id
              AND   rlf.field_id                                             = rrf.id
              AND   rlb.id                                                   = rrf.label_id
              AND   rl.id                                                    = p_rep_layout_id
              AND   rpro_rp_layout_pkg.get_aggregate_rep_flag(rl.indicators) = 'N'
              UNION ALL
              SELECT NVL (rrs.fld_name, rrf.fld_name)                        as col_name
                    ,NVL (SUBSTR (rlb.report_label,1,30),rlb.col_name)          label
                    ,NVL (rlb.col_type, 'VARCHAR2')                             col_type
                    ,NULL                                                       alias
                    ,rpro_layout_field_pkg.get_zero_check_flag (rlf.indicators) zero_check
                    ,rpro_layout_field_pkg.get_order_by_flag (rlf.indicators)   order_by
                    ,rrf.id
                    ,rlf.seq
              FROM   rpro_rep_field rrf
                    ,rpro_rp_layout rl
                    ,rpro_layout_field rlf
                    ,rpro_label rlb
                    ,rpro_rep_ps_setup rrs
              WHERE rrf.rep_id                                               = rl.rep_id
              AND   rl.id                                                    = rlf.layout_id
              AND   rlf.field_id                                             = rrf.id
              AND   rlb.id                                                   = rrf.label_id
              AND   rl.id                                                    = p_rep_layout_id
              AND   rpro_rp_layout_pkg.get_aggregate_rep_flag(rl.indicators) = 'Y'
              AND   rrs.label_id(+)                                          = rrf.label_id)
      ORDER BY seq
              ,id;

      l_proc                     VARCHAR2 (30) := 'SPLUNK_QTD_WF_REP';
      l_report_limit             NUMBER;
      l_enable_totals            VARCHAR2 (1);
      l_period_overflow_buffer   NUMBER;
      i                          NUMBER;
      l_column_value             VARCHAR2 (240);
      l_period                   VARCHAR2 (50);
      l_column_value1            VARCHAR2 (240);
      l_column_value2            VARCHAR2 (240);
      l_currency_type            VARCHAR2 (20);
      l_where1                   VARCHAR2 (10000);
      l_start_date               DATE;
      l_end_date                 DATE;
      l_quarter_end_date         DATE;
      l_year_end_date            DATE;
      l_stmt                     VARCHAR2 (32627);
      l_stmt1                    VARCHAR2 (32627);
      l_stmt2                    VARCHAR2 (32627);
      l_where                    VARCHAR2 (2000);
      l_order_by1                VARCHAR2 (2000);
      l_count_stmt               VARCHAR2 (32767);
      l_group_by                 VARCHAR2 (32600);
      l_group_by1                VARCHAR2 (32600);
      j                          NUMBER :=1;
      l_cursor                   INTEGER DEFAULT DBMS_SQL.OPEN_CURSOR;
      l_count                    NUMBER;
      l_no_rows                  NUMBER;
      l_where2                   VARCHAR2 (10000);
      l_cost_flag                VARCHAR2(1);
      l_view_name                VARCHAR2(40);
      l_tab_name                 VARCHAR2(40);
      l_prd_id                   NUMBER;
      l_min_prd_id               NUMBER;
      l_max_prd_id               NUMBER;
      l_aggregate_rep_flag       VARCHAR2(1);
      l_open_period_id           NUMBER;
      l_book_id                  NUMBER;
      l_period_name              VARCHAR2(100);
      l_tab_count                NUMBER;
      l_client_id                NUMBER;
      l_sec_atr_val              VARCHAR2(1000);
      l_str                      VARCHAR2(4000);
      l_parallel_h               VARCHAR2(50);
      l_parallel_enabled         VARCHAR2(1);
      l_forecast_flag            VARCHAR2(2);
      l_fcsttab_name             VARCHAR2(40);
      l_fcsttab_count            NUMBER;
      l_max_fcst_prd_id          NUMBER;
      l_combine_tab              VARCHAR2(200);
      l_view_stmt                VARCHAR2 (32627);
      l_wf_close_cnt             NUMBER;
      l_prvious_prd              VARCHAR2 (32627);
      l_min_prd                  NUMBER;
      l_max_prd                  NUMBER;
      l_min_date                 NUMBER;
      l_max_date                 NUMBER;
      l_custom_stmt              VARCHAR2(32627);
      l_index                    VARCHAR2(32627);   --#20191112
      l_drop                     VARCHAR2(32627);   --#20191112
      l_lb_min_prd_id            NUMBER;            --#20191112
      l_lb_max_prd_id            NUMBER;            --#20191112
      l_role_id                  NUMBER       := rpro_utility_pkg.g_role_id;    --#20191112
      l_user                     VARCHAR2(300):= rpro_utility_pkg.g_user;       --#20191112
      l_prior_profile            VARCHAR2(300):= 'Prior Period Cust WF Report'; --#20191112
      l_con_prd_id               VARCHAR2(300);                                 --#20191112
      l_pr_profile               NUMBER;                                        --#20191112
      l_acctg_type_id            VARCHAR2(100);                                 --#20191112
      l_prior_prd                VARCHAR2(240);                                 --#20191112

   BEGIN
      rpro_utility_pkg.set_revpro_context;
      l_pr_profile := NVL(rpro_utility_pkg.get_profile(l_prior_profile,l_role_id,l_user),0); --#20191112
      BEGIN
        g_amt_format        := NVL (rpro_utility_pkg.get_profile ('AMT_FORMAT'), '999G999G999G999G990D00');
        g_qty_format        := NVL (rpro_utility_pkg.get_profile ('QTY_FORMAT'), '999G999G999G999G990D00');
        g_date_format       := NVL (rpro_utility_pkg.get_profile ('DATE_FORMAT'), 'DD-MON-YYYY');
        g_pct_format        := NVL (rpro_utility_pkg.get_profile ('PCT_FORMAT'), '999G999G999G999G990D00');
        g_num_format        := NVL (rpro_utility_pkg.get_profile ('NUM_FORMAT'),'999G999G999G999G990D00');
        l_report_limit      := NVL (rpro_utility_pkg.get_profile ('RUN_REPORT_LIMIT'), 10000);
      EXCEPTION
      WHEN OTHERS THEN
         l_report_limit      := 10000;
         g_amt_format        := '999G999G999G999G990D00';
         g_date_format       := 'DD-MON-RRRR';
         g_qty_format        := '999G999G999G999G990D00';
         g_num_format        := '999G999G999G999G990D00';
         g_pct_format        := '999G999G999G999G990D00';
      END;

      SELECT rpro_rp_layout_pkg.get_enabled_totals_flag (b.indicators) enable_totals
            ,rpro_rp_layout_pkg.get_aggregate_rep_flag(b.indicators) aggregate_rep_flag
            ,rpro_rep_pkg.get_parallel_enabled_flag(a.indicators) parallel_enabled
      INTO   l_enable_totals
            ,l_aggregate_rep_flag
            ,l_parallel_enabled
      FROM rpro_rep a
          ,rpro_rp_layout b
      WHERE a.id = b.rep_id
      AND   b.id = p_rep_layout_id;

      l_client_id := rpro_utility_pkg.get_client_id;
      l_sec_atr_val := '('''||REPLACE(rpro_utility_pkg.g_sec_atr_val,':',''',''')||''')';

      IF  NVL(rpro_utility_pkg.get_profile('RPRO_PARALLEL_QUERY'),'N') = 'Y'
      AND NVL(l_parallel_enabled,'N') = 'Y'
      THEN
         l_parallel_h := TO_CHAR(NVL(TRIM(rpro_utility_pkg.get_profile('RPRO_PARALLEL_DEGREE')),'AUTO'));
         l_parallel_h := CASE
                         WHEN l_parallel_h IS NULL
                         THEN ' /*+ PARALLEL */ ' ELSE ' /*+ PARALLEL('||l_parallel_h||') */ '
                         END;
      ELSE
         l_parallel_h := '';
      END IF;


      IF l_enable_totals = 'Y'
      THEN
         l_period_overflow_buffer:= 14500;
      ELSE
         l_period_overflow_buffer:= 28000;
      END IF;
      i := rpro_rp_frmwrk_pkg.g_param.FIRST;
      WHILE (i IS NOT NULL)
      LOOP
         l_column_value      := rpro_rp_frmwrk_pkg.g_param (i).column_value;
         IF   rpro_rp_frmwrk_pkg.g_param (i).column_name IN ('REVPRO_PERIOD', 'PERIOD_NAME')
         THEN
            l_period      := l_column_value;
         ELSIF rpro_rp_frmwrk_pkg.g_param (i).column_name = 'CURRENCY_TYPE'
         THEN
            l_currency_type      := l_column_value;
         ELSIF rpro_rp_frmwrk_pkg.g_param (i).column_name = 'WF_TYPE'
         THEN
             l_column_value2      := l_column_value;
             l_where1             := l_where1 || ' AND ( act.wf_type = ''' || l_column_value2 || ''' OR act.wf_summ_type =  ''' || l_column_value2 || ''')';
         ELSIF  rpro_rp_frmwrk_pkg.g_param (i).column_name = 'FORECAST_FLAG'
         THEN
             l_forecast_flag := l_column_value;
             l_where1        := l_where1;
         ELSIF  rpro_rp_frmwrk_pkg.g_param (i).column_name = 'BOOK_ID'
         AND    l_column_value IS NOT NULL
         THEN
            l_book_id := l_column_value;
             IF rpro_rp_frmwrk_pkg.g_param (i).alias IS NOT NULL
             THEN
                l_where1      :=    l_where1
                                    || ' AND '
                                    || rpro_rp_frmwrk_pkg.g_param (i).alias
                                    || '.'
                                    || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                    || ' '
                                    || NVL (rpro_rp_frmwrk_pkg.g_param (i).operator, '=')
                                    || ' :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).bind_variable;
             ELSE
                l_where1      :=    l_where1
                                    || ' AND '
                                    || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                    || ' '
                                    || NVL (rpro_rp_frmwrk_pkg.g_param (i).operator, '=')
                                    || ' :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).bind_variable;
             END IF;
         ELSE
             IF  l_column_value                                     IS NOT NULL
             AND NVL (rpro_rp_frmwrk_pkg.g_param (i).operator, '=') <> 'BETWEEN'
             AND rpro_rp_frmwrk_pkg.g_param (i).column_type         <> 'SYSTEM'
             THEN
                IF  rpro_rp_frmwrk_pkg.g_param (i).alias IS NOT NULL
                THEN
                   l_where1      :=    l_where1
                                       || ' AND '
                                       || rpro_rp_frmwrk_pkg.g_param (i).alias
                                       || '.'
                                       || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                       || ' '
                                       || NVL (rpro_rp_frmwrk_pkg.g_param (i).operator, '=')
                                       || ' :'
                                       || rpro_rp_frmwrk_pkg.g_param (i).bind_variable;
                ELSE
                   l_where1      :=    l_where1
                                       || ' AND '
                                       || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                       || ' '
                                       || NVL (rpro_rp_frmwrk_pkg.g_param (i).operator, '=')
                                       || ' :'
                                       || rpro_rp_frmwrk_pkg.g_param (i).bind_variable;
                END IF;
             ELSIF l_column_value                             IS NOT NULL
             AND   rpro_rp_frmwrk_pkg.g_param (i).operator    = 'BETWEEN'
             AND   rpro_rp_frmwrk_pkg.g_param (i).column_type <> 'SYSTEM'
             THEN
                IF rpro_rp_frmwrk_pkg.g_param (i).alias IS NOT NULL
                THEN
                    l_where1      :=    l_where1
                                        || ' AND '
                                        || rpro_rp_frmwrk_pkg.g_param (i).alias
                                        || '.'
                                        || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                        || ' BETWEEN :'
                                        || rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                        || ' AND :'
                                        || rpro_rp_frmwrk_pkg.g_param (i).to_bind_variable;
                ELSE
                    l_where1      :=    l_where1
                                        || ' AND '
                                        || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                        || ' BETWEEN :'
                                        || rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                        || ' AND :'
                                        || rpro_rp_frmwrk_pkg.g_param (i).to_bind_variable;
                END IF;
             END IF;
         END IF;
         i                   := rpro_rp_frmwrk_pkg.g_param.NEXT (I);
      END LOOP;

      IF l_book_id IS NULL
      THEN
        l_book_id := rpro_utility_pkg.g_book_id;
      END IF;

      BEGIN
          SELECT cal.id
                ,period_name
          INTO   l_open_period_id
                ,l_period_name
          FROM rpro_period   per
                ,rpro_calendar cal
          WHERE status  = 'OPEN'
          AND   book_id = l_book_id
          AND   cal.id  = per.id;
      EXCEPTION
          WHEN no_data_found
            OR too_many_rows THEN
            rpro_utility_pkg.record_err_act (l_proc, 'ERROR: Period not open ' );
      END;

      l_fcsttab_name := 'RPRO_RI_FCST_' || regexp_replace(l_period,'*[- ~!@#$%^&*]', '_');
      l_tab_name     := 'RPRO_RI_WF_' || regexp_replace(l_period,'*[- ~!@#$%^&*]', '_');

       EXECUTE IMMEDIATE
       'select count(1)                        '||
       '  from rpro_prcss_run_log_g            '||
       ' where process_name = ''WF_PERIOD_END'''||
       '   and run_status   = ''COMPLETED'''    ||
       '   and client_id    = '||l_client_id    ||
       '   and sec_atr_val in '||l_sec_atr_val  ||
       '   and upper(run_prd) = '''||UPPER(l_period) || '''' ||
       '   and book_id        = '  ||l_book_id INTO l_wf_close_cnt;

       IF l_wf_close_cnt < 1
       THEN
           l_tab_name     := 'RPRO_RI_WF_SUMM_G';
           l_fcsttab_name := 'RPRO_RI_FCST_SUMM_V';
           rpro_utility_pkg.record_log_act (l_proc,'Forecast Table name ~ Forecast Table count ~ ' || l_tab_name || '~' || l_tab_count,10);
           rpro_utility_pkg.record_log_act (l_proc,'Table name ~ Table count ~ ' || l_tab_name || '~' || l_tab_count,10);
       ELSE
           SELECT COUNT(*)
           INTO  l_fcsttab_count
           FROM  dba_tables
           WHERE table_name = UPPER(l_fcsttab_name)
           AND   owner      = SYS_CONTEXT ( 'userenv', 'current_schema');

           IF l_fcsttab_count = 0
           THEN
              l_fcsttab_name := 'RPRO_RI_FCST_SUMM_V';
           END IF;

           rpro_utility_pkg.record_log_act (l_proc,'forecast table name ~ forecast table count ~ ' || l_tab_name || '~' || l_tab_count,10);

           SELECT COUNT(*)
           INTO  l_tab_count
           FROM  dba_tables
           WHERE table_name = UPPER(l_tab_name)
           AND   owner      = SYS_CONTEXT ( 'userenv', 'current_schema');

           rpro_utility_pkg.record_log_act (l_proc,'table name ~ table count ~ ' || l_tab_name || '~' || l_tab_count,10);

           IF l_tab_count = 0
           THEN
              l_tab_name := 'RPRO_RI_WF_SUMM_G';
           END IF;
       END IF;


       IF l_period IS NOT NULL
       THEN
         SELECT start_date
               ,end_date
               ,qtr_end_dt
               ,year_end_dt
               ,id
           INTO l_start_date
               ,l_end_date
               ,l_quarter_end_date
               ,l_year_end_date
               ,l_prd_id
           FROM rpro_calendar
          WHERE period_name = l_period;
       END IF;


       IF  l_aggregate_rep_flag ='Y'
       THEN
          l_tab_name := 'RPRO_RI_WF_PSUMM_G';
       END IF;

       l_min_prd_id := l_prd_id;

       IF l_forecast_flag = 'AF'
       THEN
          IF l_tab_name = 'RPRO_RI_WF_SUMM_G'
          THEN
              EXECUTE IMMEDIATE
              'select '||l_parallel_h||'max(prd_id) '||
              'from RPRO_RI_WF_SUMM_G wf '||
              'where prd_id >='||l_prd_id||
              '  and client_id = '||l_client_id||
              '  and book_id   = '||l_book_id||
              '  and sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id;
          ELSIF L_TAB_NAME = 'RPRO_RI_WF_PSUMM_G'
          THEN
              EXECUTE IMMEDIATE
              'select '||l_parallel_h||'max(prd_id) '||
              '  from RPRO_RI_WF_PSUMM_G wf ,rpro_calendar_g cal '||
              ' where cal.period_name = '''||l_period||''''||
              '   and wf.as_of_period = cal.id '||
              '   and wf.prd_id >= '||l_prd_id||
              '   and wf.client_id = '||l_client_id||
              '  and book_id   = '||l_book_id||
              '   and wf.sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id;
          ELSE
              EXECUTE IMMEDIATE
              'select '||l_parallel_h||'max(prd_id) '||
              '  from '||l_tab_name ||
              ' where prd_id >=' || l_prd_id ||
              ' and client_id = '||l_client_id||
              '  and book_id   = '||l_book_id||
              ' and sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id ;
          END IF;
          IF l_fcsttab_name = 'RPRO_RI_FCST_SUMM_V'
          THEN
              EXECUTE IMMEDIATE
              'select '||l_parallel_h||'max(prd_id) '||
              'from RPRO_RI_FCST_SUMM_V wf '||
              'where prd_id >='||l_prd_id||
              '  and book_id   = '||l_book_id||
              '  and client_id = '||l_client_id||
              '  and sec_atr_val in '||l_sec_atr_val INTO l_max_fcst_prd_id;
          ELSE
              EXECUTE IMMEDIATE
              'select '||l_parallel_h||'max(prd_id) '||
              '  from '||l_fcsttab_name ||
              ' where prd_id >=' || l_prd_id ||
              ' and client_id = '||l_client_id||
              '  and book_id   = '||l_book_id||
              ' and sec_atr_val in '||l_sec_atr_val INTO l_max_fcst_prd_id ;
          END IF;

          l_max_prd_id := GREATEST(NVL(l_max_prd_id,l_min_prd_id),NVL(l_max_fcst_prd_id,l_min_prd_id));
          IF rpro_utility_pkg.g_log_level >= 10 THEN
              rpro_utility_pkg.record_log_act (l_proc
                                               ,'l_tab_name ~ l_min_prd_id ~ l_max_prd_id ~' || l_tab_name || '~' || l_min_prd_id || '~' || l_max_prd_id
                                               ,10
                                              );
              rpro_utility_pkg.record_log_act (l_proc
                                               ,'l_fcstab_name ~ l_min_prd_id ~ l_max_fcst_prd_id ~' || l_fcsttab_name || '~' || l_min_prd_id || '~' || l_max_fcst_prd_id
                                               ,10
                                              );

          END IF;
       ELSIF l_forecast_flag = 'F'
       THEN
           IF l_fcsttab_name = 'RPRO_RI_FCST_SUMM_V'
           THEN
               EXECUTE IMMEDIATE
               'select '||l_parallel_h||'max(prd_id) '||
               'from RPRO_RI_FCST_SUMM_V wf '||
               'where prd_id >='||l_prd_id||
               '  and client_id = '||l_client_id||
               '   and book_id   = '||l_book_id||
               '  and sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id;
           ELSIF l_tab_name = 'RPRO_RI_WF_PSUMM_G'
           THEN
               EXECUTE IMMEDIATE
               'select '||l_parallel_h||'max(prd_id) '||
               '  from RPRO_RI_WF_PSUMM_G wf ,rpro_calendar_g cal '||
               ' where cal.period_name = '''||l_period||''''||
               '   and wf.as_of_period = cal.id '||
               '   and wf.prd_id >= '||l_prd_id||
               '   and wf.client_id = '||l_client_id||
               '   and book_id   = '||l_book_id||
               '   and wf.sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id;
           ELSE
               EXECUTE IMMEDIATE
               'select '||l_parallel_h||'max(prd_id) '||
               '  from '||l_fcsttab_name ||
               ' where prd_id >=' || l_prd_id ||
               ' and client_id = '||l_client_id||
               ' and book_id   = '||l_book_id||
               ' and sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id ;
           END IF;
           IF rpro_utility_pkg.g_log_level >= 10
           THEN
              rpro_utility_pkg.record_log_act (l_proc
                                               ,'l_fcstab_name ~ l_min_prd_id ~ l_max_prd_id ~' || l_tab_name || '~' || l_min_prd_id || '~' || l_max_prd_id
                                               ,10
                                              );
           END IF;
       ELSE
           IF l_tab_name = 'RPRO_RI_WF_SUMM_G' THEN
               EXECUTE IMMEDIATE
               'select '||l_parallel_h||'max(prd_id) '||
               'from RPRO_RI_WF_SUMM_G wf '||
               'where prd_id >='||l_prd_id||
               '  and client_id = '||l_client_id||
               '  and book_id   = '||l_book_id||
               '  and sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id;
           ELSIF l_tab_name = 'RPRO_RI_WF_PSUMM_G' THEN
               EXECUTE IMMEDIATE
               'select '||l_parallel_h||'max(prd_id) '||
               '  from RPRO_RI_WF_PSUMM_G wf ,rpro_calendar_g cal '||
               ' where cal.period_name = '''||l_period||''''||
               '   and wf.as_of_period = cal.id '||
               '   and wf.prd_id >= '||l_prd_id||
               '   and wf.client_id = '||l_client_id||
               '   and book_id   = '||l_book_id||
               '   and wf.sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id;
           ELSE
               EXECUTE IMMEDIATE
               'select '||l_parallel_h||'max(prd_id) '||
               '  from '||l_tab_name ||
               ' where prd_id >=' || l_prd_id ||
               ' and client_id = '||l_client_id||
               ' and book_id   = '||l_book_id||
               ' and sec_atr_val in '||l_sec_atr_val INTO l_max_prd_id ;
           END IF;
           IF rpro_utility_pkg.g_log_level >= 10
           THEN
               rpro_utility_pkg.record_log_act (l_proc
                                                ,'l_tab_name ~ l_min_prd_id ~ l_max_prd_id ~' || l_tab_name || '~' || l_min_prd_id || '~' || l_max_prd_id
                                                ,10
                                               );
           END IF;
       END IF;

       BEGIN
          SELECT rpro_acct_type_pkg.get_cost_flag(indicators)
          INTO   l_cost_flag
          FROM   rpro_acct_type
          WHERE (wf_type = l_column_value2 or wf_summ_type = l_column_value2)
          AND    ROWNUM < 2;
       EXCEPTION
       WHEN no_data_found THEN
          l_cost_flag := 'N';
       END;

       SELECT MIN(id)
             ,MAX(id)
       INTO   l_min_date
             ,l_max_date
       FROM   rpro_calendar_g
       WHERE  (qtr_start_dt
              ,qtr_end_dt)IN     (SELECT qtr_start_dt
                                        ,qtr_end_dt
                                  FROM  rpro_calendar_g
                                  WHERE id = l_prd_id);

       IF l_min_date = l_prd_id
       THEN
          l_min_prd:=l_min_date ;
          l_max_prd:=l_min_date ;
       ELSIF l_max_date = l_prd_id
       then
          l_min_prd:=l_min_date;
          l_max_prd:=l_max_date;
       ELSIF l_max_date>l_prd_id
       then
          l_min_prd:=l_min_date;
          l_max_prd:=l_prd_id;
       END IF;

       SELECT MIN(id)        --#20191112
             ,MAX(id)
       INTO   l_lb_min_prd_id
             ,l_lb_max_prd_id
       FROM  (SELECT a.*
              FROM   rpro_calendar_g a
              WHERE  id <  l_prd_id
              ORDER BY id DESC)
       WHERE ROWNUM <= l_pr_profile ;    --#20191112

       l_con_prd_id := NULL;             --#20191112

       BEGIN
          SELECT LISTAGG(ID, ',') WITHIN GROUP (ORDER BY id) list
          INTO   l_con_prd_id
          FROM  (SELECT id
                 FROM   rpro_calendar_g
                 WHERE  id  BETWEEN l_lb_min_prd_id AND l_lb_max_prd_id);   --#20191112
       EXCEPTION
       WHEN OTHERS THEN
          l_con_prd_id:=0;
       END;


       l_acctg_type_id:=CASE                               --#20191112
                        WHEN l_column_value2 ='Revenue'
                        THEN '''R,Y'''
                        WHEN l_column_value2 ='Net Revenue'
                        THEN '''R,Y,X'''
                        WHEN l_column_value2 ='Adjustments'--#20191119
                        THEN '''X'''
                        END;                               --#20191112

       --#20191112
       EXECUTE IMMEDIATE 'TRUNCATE TABLE rpro_wf_qtd_tbl';
       l_custom_stmt := 'INSERT INTO rpro_wf_qtd_tbl
                       (SELECT sum(wfv.t_at) qtd_amt
                              ,wfv.root_line_id
                              ,wfv.rc_id
                        FROM   rpro_ri_Acct_summ_g wfv                 --#03122019
                        WHERE  1=1
                        AND wfv.prd_id BETWEEN '||l_min_prd||' AND '||l_max_prd||
                       ' AND  wfv.acct_type_id  IN( SELECT regexp_substr('||l_acctg_type_id||',''[^,]+'', 1, level)
                                                    FROM   dual
                                                    CONNECT BY REGEXP_SUBSTR('||l_acctg_type_id||', ''[^,]+'', 1, level) IS NOT NULL)
                        GROUP BY wfv.root_line_id
                                ,wfv.rc_id)';              --#20191112
       rpro_utility_pkg.record_log_act (l_proc
                                                ,'l_custom_stmt~'||l_custom_stmt
                                                ,2
                                               );
       EXECUTE IMMEDIATE l_custom_stmt;

       L_DROP:='DROP TABLE rpro_wf_prd_qtd_tbl';
       EXECUTE IMMEDIATE  L_DROP;
       --#20191112
       l_prvious_prd := 'CREATE TABLE rpro_wf_prd_qtd_tbl as
                        (SELECT *
                         FROM   (SELECT root_line_id
                                       ,rc_id
                                       ,t_at
                                       ,prd_id
                                 FROM   rpro_ri_Acct_summ_g wfv
                                 WHERE  1=1
                                 AND wfv.prd_id BETWEEN '||l_lb_min_prd_id||' AND '||l_lb_max_prd_id||'
                                 AND  wfv.acct_type_id  IN( SELECT regexp_substr('||l_acctg_type_id||',''[^,]+'', 1, level)
                                                            FROM   dual
                                                            CONNECT BY REGEXP_SUBSTR('||l_acctg_type_id||', ''[^,]+'', 1, level) IS NOT NULL))
                                 PIVOT  (SUM(t_at)  FOR (prd_id) IN ('||l_con_prd_id||')))';   --#20191112
        L_INDEX:='CREATE INDEX rpro_splunk_rootline on RPRO_WF_PRD_QTD_TBL(root_line_id)';      --#20191112

       rpro_utility_pkg.record_log_act (l_proc
                                                ,'l_prvious_prd~'||l_prvious_prd
                                                ,2
                                               );
       EXECUTE IMMEDIATE l_prvious_prd;    --#20191112
       EXECUTE IMMEDIATE L_INDEX;

      FOR I IN (SELECT  cal.id           prd_id                  --#20191112
                FROM    all_tab_columns tab
                       ,rpro_calendar_g cal
                WHERE   table_name        = 'RPRO_WF_PRD_QTD_TBL'
                AND     TO_CHAR(cal.id)   = tab.column_name )
      LOOP
         l_prior_prd := l_prior_prd ||'MAX(NVL(cus_prd."'||i.prd_id||'",0))'||'+';
      END LOOP;                                                   --#20191112
       l_prior_prd       := RTRIM(LTRIM(l_prior_prd,'+'),'+');    --#20191112
       l_stmt            := 'SELECT '||l_parallel_h;
       l_stmt1           := 'SELECT '||l_parallel_h;
       l_stmt2           := 'SELECT '||l_parallel_h;
       l_count_stmt      := 'SELECT '||l_parallel_h||' COUNT(*) FROM ( SELECT  ';
       l_group_by        := ' GROUP BY ';
       l_group_by1       := ' GROUP BY ';
       l_order_by1        := '';

       FOR r_fld IN c_fields (p_rep_layout_id) LOOP
         IF r_fld.col_type = 'AMOUNT'
         THEN
            IF p_no_tags = 'N'
            THEN
              l_stmt      := l_stmt || ' htf.paragraph(''right'') ||TO_CHAR(a' || j || ', ''' || g_amt_format || ''' )  A' || j || ',';
            ELSE
              l_stmt      := l_stmt || ' TO_CHAR( ' || 'a' || j || ', ''' || g_amt_format || ''' )' || '  A' || j || ',';
            END IF;
         ELSIF r_fld.col_type = 'NUMBER'
         THEN
            IF p_no_tags = 'N' THEN
              l_stmt      := l_stmt || ' htf.paragraph(''right'') || TO_CHAR(a' || j || ', ''' || g_num_format || ''' )  A' || j || ',';
            ELSE
              l_stmt      := l_stmt || ' TO_CHAR(a' || j || ', ''' || g_num_format || ''' )  A' || j || ',';
            END IF;
         ELSIF r_fld.col_type = 'DATE'
         THEN
            IF p_no_tags = 'N' THEN
              l_stmt      := l_stmt || ' htf.paragraph(''right'') ||TO_CHAR(a' || j || ',''' || g_date_format || ''') A' || j || ',';
            ELSE
              l_stmt      := l_stmt || ' TO_CHAR(a' || j || ',''' || g_date_format || ''') A' || j || ',';
            END IF;
         ELSIF r_fld.col_type = 'PERCENT'
         THEN
            IF p_no_tags = 'N'
            THEN
              l_stmt      := l_stmt || ' htf.paragraph(''right'') ||TO_CHAR(a' || j || ',''' || g_pct_format || ''') A' || j || ',';
            ELSE
              l_stmt      := l_stmt || ' TO_CHAR(a' || j || ',''' || g_pct_format || ''') A' || j || ',';
            END IF;
         ELSIF r_fld.col_type = 'QUANTITY'
         THEN
            IF p_no_tags = 'N'
            THEN
              l_stmt      := l_stmt || ' htf.paragraph(''right'') ||TO_CHAR(a' || j || ',''' || g_qty_format || ''') A' || j || ',';
            ELSE
              l_stmt      := l_stmt || ' TO_CHAR(a' || j || ',''' || g_qty_format || ''') A' || j || ',';
            END IF;
         ELSIF r_fld.col_type = 'ID'
         THEN
            IF p_no_tags = 'N'
            THEN
              l_stmt      := l_stmt || ' htf.paragraph(''right'')|| A' || j || '  A' || j || ',';
            ELSE
              l_stmt      := l_stmt || '  A' || j || '  A' || j || ',';
            END IF;
          ELSIF r_fld.col_type = 'RAW'
          THEN
             IF p_no_tags = 'N' THEN
               l_stmt      :=  l_stmt || ' htf.paragraph(''right'') ||TO_CHAR(a' || j || ') A' || j || ',';
             ELSE
               l_stmt      := l_stmt || ' TO_CHAR(a' || j || ') A' || j || ',';
             END IF;
         ELSE
            l_stmt      := l_stmt || '  A' || j || '  A' || j || ',';
         END IF;

         l_group_by        := l_group_by || r_fld.alias || r_fld.col_name || ',';
         l_group_by1       := l_group_by1 || 'a' || j || ',';
         l_stmt1           := l_stmt1 || 'null AS ' || ' a' || j || ',';
         l_stmt2           := l_stmt2 || r_fld.alias || r_fld.col_name || ' a' || j || ',';
         l_count_stmt      := l_count_stmt || r_fld.alias || r_fld.col_name || ',';

         IF NVL(r_fld.order_by,'N') <> 'N' THEN
             l_order_by1       := l_order_by1
                                   || 'a'
                                   ||j
                                   || CASE WHEN  r_fld.order_by = 'B' THEN ' ASC NULLS LAST ,'
                                           WHEN  r_fld.order_by = 'A' THEN ' ASC NULLS FIRST ,'
                                           WHEN  r_fld.order_by = 'E' THEN ' DESC NULLS LAST ,'
                                           WHEN  r_fld.order_by = 'D' THEN ' DESC NULLS FIRST ,'
                                      ELSE ' '  END;
         END IF;

         j                 := j + 1;
         p_header          := p_header || r_fld.label || ': ';




       END LOOP;

       l_count_stmt      := RTRIM(l_count_stmt,',');
       l_group_by        := RTRIM(l_group_by,',');
       l_group_by1       := RTRIM(l_group_by1,',');

       IF l_order_by1 IS NOT NULL
       THEN
          l_order_by1 := ' ORDER BY '||l_order_by1;
          l_order_by1 := RTRIM(l_order_by1,',');
       END IF;

        FOR r_per IN c_per (l_min_prd_id,l_max_prd_id, l_forecast_flag)
        LOOP
           IF LENGTH (l_stmt) + LENGTH (l_stmt2) > l_period_overflow_buffer THEN
             EXIT;
           END IF;

           IF  l_aggregate_rep_flag ='Y' AND R_PER.PRD_ID = -999 THEN
               CONTINUE;
           END IF;

           IF p_no_tags = 'Y' THEN
             l_stmt      := l_stmt || ' TO_CHAR(sum(a' || j || '),' || '''' || g_amt_format || '''' || ') ,';
           ELSE
             l_stmt      := l_stmt || 'htf.paragraph (''right'')|| TO_CHAR(sum(a' || j || '),' || '''' || g_amt_format || '''' || ')a' || j || ',';
           END IF;

           write_log('r_per.prd_id'||r_per.prd_id);
           IF p_no_tags = 'Y'
           THEN
              l_stmt1      := l_stmt1 || ' htf.bold(TO_CHAR(sum(a' || j || '),' || '''' || g_amt_format || '''' || ')) a' || j || ',';
              IF  r_per.prd_type = 'MON'
              AND r_per.prd_id = -999
              AND l_forecast_flag = 'A'
              THEN
                  l_stmt2      := l_stmt2 || ' (SUM(
                                                   (CASE WHEN rc.id = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END)-
                                                   (CASE WHEN rc.id = ' || '''' || l_min_prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END)
                                                 ))-('||l_prior_prd||')  a' || J || ',';
              ELSIF r_per.prd_type = 'MON'
              AND r_per.prd_id = -999
              AND l_forecast_flag = 'AF'
              THEN
                  l_stmt2      := l_stmt2 || ' (SUM(
                                                   (CASE WHEN wf.src = ''A'' and rc.id = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END)-
                                                   (CASE WHEN wf.src = ''A'' and rc.id = ' || '''' || l_min_prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END)
                                                  ))-('||l_prior_prd||')  a' || J || ',';
              ELSIF R_PER.PRD_TYPE = 'MON'
              THEN

                  l_stmt2      := l_stmt2 || ' SUM((CASE WHEN rc.id = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END))  a' || J || ',';
             ELSIF R_PER.PRD_TYPE = 'CUST'  --#20191112
             THEN
                l_stmt2      := l_stmt2 || 'MAX(NVL(cus_prd."'||r_per.prd_id||'",0))  a' || J || ',';
              ELSE
                  l_stmt2      := l_stmt2 || ' SUM((CASE WHEN rc.id >= '||r_per.mx_prd_id||' and rc.period_year = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END))  a' || J || ',';
             END IF;


           ELSE
              l_stmt1      := l_stmt1 || 'htf.paragraph (''right'')||  htf.bold(TO_CHAR(sum(a' || J || '),' || '''' || g_amt_format || '''' || '))a' || J || ' ,';
              IF r_per.prd_type = 'MON'
              AND r_per.prd_id = -999
              AND l_forecast_flag = 'A'
              THEN
                 l_stmt2      := l_stmt2 || ' (SUM(
                                                   (CASE WHEN rc.id = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END) -
                                                   (CASE WHEN rc.id = ' || '''' || l_min_prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END)

                                                 ) )-('||l_prior_prd||') a' || J || ',';
              ELSIF r_per.prd_type = 'MON'
              AND r_per.prd_id = -999
              AND l_forecast_flag = 'AF'
              THEN
                 l_stmt2      := l_stmt2 || ' (SUM(
                                                  (CASE WHEN wf.src = ''A'' and rc.id = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END) -
                                                  (CASE WHEN wf.src = ''A'' and rc.id = ' || '''' || l_min_prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END)
                                                 ))-('||l_prior_prd||')  a' || J || ',';
              ELSIF r_per.prd_type = 'MON'
              THEN
                  l_stmt2      := l_stmt2 || ' SUM((CASE WHEN rc.id = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END))  a' || J || ',';
              ELSIF R_PER.PRD_TYPE = 'CUST'  --#20191112
             THEN
                l_stmt2      := l_stmt2 || 'MAX(NVL(cus_prd."'||r_per.prd_id||'",0))  a' || J || ',';
              ELSE
                  l_stmt2      := l_stmt2 || ' SUM((CASE WHEN rc.id >= '||r_per.mx_prd_id||' and rc.period_year = ' || '''' || r_per.prd_id || '''' || ' THEN (wf.' || l_currency_type || '_AT' || ') ELSE 0 END))  a' || J || ',';
              END IF;



           END IF;
           p_header      := p_header || r_per.prd_lbl || ': ';
           j             := j + 1;
        END LOOP;

       IF LENGTH (l_stmt) + LENGTH (l_stmt2) > l_period_overflow_buffer
       THEN
          l_stmt       := l_stmt || '''...''' || ',';
          l_stmt1      := l_stmt1 || '''...''' || ',';
          l_stmt2      := l_stmt2 || '''...''' || ',';
          IF p_no_tags = 'Y' THEN
            p_header      := p_header || '..(Exceeds Limit.All schedules cannot be displayed) : ';
          ELSE
            p_header      := p_header || '<span onmouseover="toolTip_enable(event,this,''Exceeds limit.All schedules cannot be displayed'');" >...</span>' || ': ';
          END IF;
       END IF;

       IF P_NO_TAGS = 'Y'
       THEN
          l_stmt       := l_stmt || ' TO_CHAR(SUM(NVL(a' || J || ',0)), ' || '''' || g_amt_format || '''' || ') a' || J;
          l_stmt1      := l_stmt1 || ' htf.bold(TO_CHAR(SUM(NVL(a' || J || ',0)), ' || '''' || g_amt_format || '''' || ')) a' ||J;
          l_stmt2      := l_stmt2 || ' SUM((CASE WHEN rc.id != -999 THEN NVL(wf.' || l_currency_type || '_AT' || ',0) ELSE 0 END))+'||l_prior_prd||' a' || J;--#20191112
       ELSE
          l_stmt      := l_stmt || 'htf.paragraph (''right'') || TO_CHAR(SUM(NVL(a' || J || ',0)), ' || '''' || g_amt_format || '''' || ') a' || J;
          l_stmt1      := l_stmt1 || 'htf.paragraph (''right'') || htf.bold(TO_CHAR(SUM(NVL(a' || J || ',0)), ' || '''' || g_amt_format || '''' || ')) a' ||J;
          l_stmt2      := l_stmt2 || ' SUM((CASE WHEN rc.id != -999 THEN NVL(wf.' || l_currency_type || '_AT' || ',0) ELSE 0 END))+'||l_prior_prd||' a' || J; --#20191112
       END IF;
       p_header          := p_header || ' Total ';


       IF 1=1-- L_AGGREGATE_REP_FLAG = 'N'
       THEN
         IF l_cost_flag = 'Y'
         THEN
           SELECT text
           INTO   l_view_stmt
           FROM   all_views
           WHERE  view_name = UPPER ('rpro_ri_cost_wf_rep_v')
           AND    owner     = SYS_CONTEXT ( 'userenv', 'current_schema');
         ELSE
             SELECT text
             INTO   l_view_stmt
             FROM   all_views
             WHERE  view_name  = UPPER ('RPRO_RI_SPLUNK_WF_REP_V')
             AND    owner        = SYS_CONTEXT ( 'userenv', 'current_schema');
         END IF;

         l_view_stmt       :=    REPLACE(l_view_stmt,'rpro_ri_wf_summ_g',l_tab_name);
         l_view_stmt       :=    REPLACE(l_view_stmt,'''#l_sec_atr_val''',l_sec_atr_val);
         l_view_stmt       :=    REPLACE(l_view_stmt,'''#l_client_id''',l_client_id);
         l_view_stmt       :=    REPLACE(l_view_stmt,'''#l_min_prd_id''',l_min_prd_id);
         l_stmt2           :=    REPLACE(l_view_stmt,'SELECT ''#1'' c',l_stmt2);
         l_count_stmt      :=    l_count_stmt || REPLACE(l_view_stmt,'SELECT ''#1'' c','');

         IF l_forecast_flag = 'F'
         THEN
            l_stmt2      := REPLACE(l_stmt2,l_tab_name,l_fcsttab_name);
            l_count_stmt := REPLACE(l_count_stmt,l_tab_name,l_fcsttab_name);
         ELSIF l_forecast_flag = 'AF'
         THEN
            l_combine_tab := '(select a.*,''A'' src from '||l_tab_name ||' a union all select f.*,''F'' from '||l_fcsttab_name||' f)';
            l_stmt2       := REPLACE(l_stmt2,l_tab_name,l_combine_tab);
            l_count_stmt  := REPLACE(l_count_stmt,l_tab_name,l_combine_tab);
         END IF;
       END IF;
           -- L_STMT2           :=    L_STMT2
           --                 || ' from ' || L_TAB_NAME ||' wf, '
           --                   || '      rpro_ri_wf_acct_type_v act, '
           --                   || '      rpro_calendar_g rc, '
           --
           --                   || '      (select acct_seg, segment1,segment2, '
           --                   || '              segment3,segment4,segment5, '
           --                   || '              segment6,segment7,segment8, '
           --                   || '              segment9, segment10,client_id '
           --                   || '       from rpro_acct_val_g) racvl '
           --                   || 'where wf.prd_id = rc.id '
           --                   || '  and wf.acct_type_id = act.id '
           --                   || '  and wf.acctg_segs = racvl.acct_seg '
           --                   || '  and wf.client_id = '||L_CLIENT_ID
           --                   || '  and wf.sec_atr_val in '||L_SEC_ATR_VAL
           --                   || '  and wf.client_id = act.client_id '
           --                   || '  and wf.client_id = racvl.client_id '
           --                   || '  and wf.client_id = rc.client_id ';
           -- L_COUNT_STMT      :=    L_COUNT_STMT
           --                   || ' from ' || L_TAB_NAME ||' wf, '
           --                   || '      rpro_ri_wf_acct_type_v act, '
           --                   || '      rpro_calendar_g rc,'
           --
           --                   || '      (select acct_seg, segment1,segment2, '
           --                   || '              segment3,segment4,segment5, '
           --                   || '              segment6,segment7,segment8, '
           --                   || '              segment9, segment10,client_id '
           --                   || '       from rpro_acct_val_g) racvl '
           --                   || 'where wf.prd_id = rc.id '
           --                   || '  and wf.acct_type_id = act.id '
           --                   || '  and wf.acctg_segs = racvl.acct_seg '
           --                   || '  and wf.client_id = '||L_CLIENT_ID
           --                   || '  and wf.sec_atr_val in '||L_SEC_ATR_VAL
           --                   || '  and wf.client_id = act.client_id '
           --                   || '  and wf.client_id = racvl.client_id '
           --                   || '  and wf.client_id = rc.client_id ';

       --END IF;

       IF  l_tab_name =  'RPRO_RI_WF_PSUMM_G'
       THEN
          l_where := ' AND WF.AS_OF_PERIOD = ' || l_prd_id||' AND WF.PRD_ID >= '||l_min_prd_id;
       END IF;
       l_count_stmt := l_count_stmt || l_where || ' ' || l_where1 || l_group_by || ')';

       IF rpro_utility_pkg.g_log_level >= 10
       THEN
          rpro_utility_pkg.record_log_act (p_type => l_proc
                                          ,p_text => 'Count Stmt :'
                                          ,p_log_level => 10
                                          ,p_clob_text => l_count_stmt
                                          );
       END IF;

       IF p_restrict_rows = 'Y'
       THEN
          dbms_sql.parse (l_cursor
                         ,l_count_stmt
                         ,dbms_sql.native
                         );
          dbms_sql.define_column (l_cursor
                                 ,1
                                 ,l_count
                                 );
          i := rpro_rp_frmwrk_pkg.g_param.FIRST;

          WHILE (i IS NOT NULL)
          LOOP
             IF rpro_utility_pkg.g_log_level >= 10
             THEN
               rpro_utility_pkg.record_log_act (
                 l_proc
                ,   'column_name ~ column_value ~ bind_variable '
                 || rpro_rp_frmwrk_pkg.g_param (i).column_name
                 || '~'
                 || rpro_rp_frmwrk_pkg.g_param (i).column_value
                 || '~'
                 || rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                ,10);
             END IF;

             IF (rpro_rp_frmwrk_pkg.g_param (i).column_value IS NOT NULL)
             THEN
                IF rpro_rp_frmwrk_pkg.g_param (i).column_name NOT IN ('REVPRO_PERIOD', 'PERIOD_NAME', 'CURRENCY_TYPE', 'WF_TYPE','FORECAST_FLAG')
                THEN
                   dbms_sql.bind_variable (l_cursor
                                          ,rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                          ,rpro_rp_frmwrk_pkg.g_param (i).column_value
                                          );

                  IF (rpro_rp_frmwrk_pkg.g_param (i).operator IN ('BETWEEN'))
                  THEN
                     dbms_sql.bind_variable (l_cursor
                                            ,rpro_rp_frmwrk_pkg.g_param (i).to_bind_variable
                                            ,rpro_rp_frmwrk_pkg.g_param (i).to_column_value
                                            );
                  END IF;
                END IF;
             END IF;
             i      := rpro_rp_frmwrk_pkg.g_param.next (i);
          END LOOP;

         l_no_rows      := dbms_sql.execute_and_fetch (l_cursor);
         dbms_sql.column_value (l_cursor
                               ,1
                               ,l_count
                               );
       END IF;

       IF rpro_utility_pkg.g_log_level >= 10
       THEN
          rpro_utility_pkg.record_log_act (l_proc
                                          ,'row count: ' || l_count
                                          ,10
                                          );
       END IF;

       IF l_aggregate_rep_flag = 'N' THEN

           l_stmt2           := l_stmt2 || l_where || ' ' || l_where1 || l_group_by || ' ,WF.ROOT_LINE_ID';
           l_stmt            := l_stmt || ' FROM ( ' || l_stmt2 || ') ' || l_where2 || ' ' || l_group_by1 || CASE WHEN l_enable_totals = 'Y' THEN '' ELSE l_order_by1 END;
       ELSE
           l_stmt2           := l_stmt2 || l_where || ' ' || l_where1 || l_group_by ;
           l_stmt            := l_stmt || ' FROM ( ' || l_stmt2 || ') ' || l_where2 || ' ' || l_group_by1 || CASE WHEN l_enable_totals = 'Y' THEN '' ELSE l_order_by1 END;
       END IF;

       IF (l_enable_totals = 'Y')
       AND (LENGTH (l_stmt1) + LENGTH (l_stmt2) <= 32000)
       THEN
          l_stmt1      := l_stmt1 || ' FROM (' || l_stmt2 || ')';
       END IF;

       IF rpro_utility_pkg.g_log_level >= 10
       THEN
          rpro_utility_pkg.record_log_act (l_proc
                                          ,'l_stmt ' || l_stmt
                                          ,10
                                          );
          rpro_utility_pkg.record_log_act (l_proc
                                          ,'l_stmt1 ' || l_stmt1
                                          ,10
                                          );

          rpro_utility_pkg.record_log_act (l_proc
                                          ,'l_stmt2 ' || l_stmt2
                                          ,10
                                          );
       END IF;

       IF rpro_utility_pkg.g_log_level >= 10
       THEN
         rpro_utility_pkg.record_log_act (l_proc
                                         ,l_stmt
                                         ,10
                                         );
       END IF;

       IF l_count > l_report_limit
       THEN
          L_STMT      := 'SELECT * FROM ( ' || L_STMT || ' ) WHERE ROWNUM <=' || L_REPORT_LIMIT;
       END IF;

       IF L_ENABLE_TOTALS = 'Y'
       THEN
          dbms_sql.parse (l_cursor
                         ,'select 1 from ( ' || l_stmt || ' ) WHERE rownum <2 '
                         ,dbms_sql.native
                         );

          IF rpro_utility_pkg.g_log_level >= 10
          THEN
             rpro_utility_pkg.record_log_act (p_type => l_proc
                                             ,p_text => 'count totals stmt: '
                                             ,p_log_level => 10
                                             ,p_clob_text => l_stmt1
                                             );
          END IF;
         i := rpro_rp_frmwrk_pkg.g_param.FIRST;
         WHILE (I IS NOT NULL)
         LOOP
           IF rpro_utility_pkg.g_log_level >= 10 THEN
             rpro_utility_pkg.record_log_act ( l_proc
                                               ,   'column_name ~ column_value ~ bind_variable '
                                                || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                                || '~'
                                                || rpro_rp_frmwrk_pkg.g_param (i).column_value
                                                || '~'
                                                || rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                               ,10);
           END IF;

           IF (rpro_rp_frmwrk_pkg.g_param (i).column_value IS NOT NULL)
           THEN
              IF rpro_rp_frmwrk_pkg.g_param (i).column_name NOT IN ('REVPRO_PERIOD', 'PERIOD_NAME', 'CURRENCY_TYPE', 'WF_TYPE','FORECAST_FLAG')
              THEN
                IF rpro_utility_pkg.g_log_level >= 10
                THEN
                  rpro_utility_pkg.record_log_act (l_proc
                                                  ,'binding paramters...' || l_no_rows
                                                  ,10
                                                  );
                END IF;

                dbms_sql.bind_variable (l_cursor
                                       ,rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                       ,rpro_rp_frmwrk_pkg.g_param (i).column_value
                                       );

               IF (rpro_rp_frmwrk_pkg.g_param (i).operator = 'BETWEEN')
               THEN
                  dbms_sql.bind_variable (l_cursor
                                         ,rpro_rp_frmwrk_pkg.g_param (i).to_bind_variable
                                         ,rpro_rp_frmwrk_pkg.g_param (i).to_column_value
                                         );
               END IF;
              END IF;
           END IF;
           i      := rpro_rp_frmwrk_pkg.g_param.NEXT (i);
         END LOOP;

         l_no_rows      := dbms_sql.execute_and_fetch (l_cursor);

         IF rpro_utility_pkg.g_log_level >= 10
         THEN
            rpro_utility_pkg.record_log_act (l_proc
                                            ,' no rows for totals' || l_no_rows
                                            ,10
                                            );
         END IF;

         IF (l_no_rows > 0
         AND (LENGTH (l_stmt) + LENGTH (l_stmt1)) < 32000)
         THEN
            l_stmt      := l_stmt || CHR (13) || CHR (10) || ' UNION ALL ' || l_stmt1 || l_order_by1;
         END IF;
       END IF;

       IF rpro_utility_pkg.g_log_level >= 10
       THEN
          rpro_utility_pkg.record_log_act (l_proc
                                          ,length (l_stmt) || '~' || length (l_stmt1)
                                          ,10
                                          );
           rpro_utility_pkg.record_log_act (p_type => l_proc
                                          ,p_text => 'query: '
                                          ,p_log_level => 10
                                          ,p_clob_text => l_stmt
                                          );
          rpro_utility_pkg.record_log_act (l_proc
                                          ,'where : ' || l_where
                                          ,10
                                          );
          rpro_utility_pkg.record_log_act (l_proc
                                          ,'header : ' || p_header
                                          ,10
                                          );
          rpro_utility_pkg.record_log_act (l_proc
                                          ,' end()'
                                          ,10
                                          );
       END IF;
       p_query           := l_stmt;
   EXCEPTION
   WHEN OTHERS THEN
       rpro_utility_pkg.record_log_act (l_proc
                                       ,sqlerrm||chr(13)||dbms_utility.format_error_backtrace
                                       ,10
                                       );
   END splunk_qtd_wf_rep;

   PROCEDURE credit_rebill_wrap ( p_rc_id       NUMBER
                                 ,p_batch_id    NUMBER )
   IS

      l_request_id         NUMBER;
      l_ret_msg            VARCHAR2 (500);
      l_retcode            NUMBER;
      l_errbuf             VARCHAR2 (4000);
      l_prog_id            NUMBER;
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      l_retcode                                     := 0;
      l_errbuf                                      := 'SUCCESS';
      write_log('Start credit_rebill_wrap with batch_id : ' || p_batch_id || ' at ' || TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));

      BEGIN
         SELECT id
         INTO   l_prog_id
         FROM   rpro_prog_head
         WHERE  name = 'RevPro SPLK Credit Rebill'
         AND    ROWNUM = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_prog_id := NULL;
      END;
      IF l_prog_id IS NULL THEN
         write_error(' RevPro SPLK Credit Rebill Job Scheduler Not Found ');
         RETURN;
      END IF;

      rpro_job_scheduler_pkg.schedule_job (p_errbuf                 => l_errbuf
                                          ,p_retcode                => l_retcode
                                          ,p_request_id             => l_request_id
                                          ,p_program_id             => l_prog_id
                                          ,p_parameter_text         => p_batch_id  || '~' ||  rpro_utility_pkg.g_sec_atr_val
                                          ,p_requested_start_date   => SYSDATE
                                          ,p_requested_end_date     => NULL
                                          ,p_request_date           => SYSDATE
                                          );

      write_log('End credit_rebill_wrap with batch_id : ' || p_batch_id || ' ~ Request Id : ' || l_request_id || ' at ' || TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS'));
   EXCEPTION
     WHEN OTHERS
     THEN
        ROLLBACK;
        write_error(' Error in credit_rebill_wrap : '||SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace);
   END credit_rebill_wrap;

   PROCEDURE credit_rebill (  p_errbuf           OUT  VARCHAR2
                             ,p_retcode          OUT  NUMBER
                             ,p_batch_id         IN   NUMBER
                             ,p_sec_atr_val      IN   VARCHAR2 DEFAULT NULL
                           )
   IS
      lock_failed                EXCEPTION;
      l_obj_ver                  NUMBER;
      l_rel_pct                  NUMBER;
      l_ret_msg                  VARCHAR2(2000);
      l_rownum                   VARCHAR2(1000);

   BEGIN
      p_retcode                                     := 0;
      p_errbuf                                      := 'SUCCESS';
      write_log('Start Credit Rebill for Batch ' || p_batch_id || '~' || TO_CHAR ( SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      IF NOT (rpro_utility_pkg.check_and_lock (  p_batch_id
                                                ,'CREDIT_REBILL'
                                                ,'Credit Rebill Process is in Progress : ' || p_batch_id
                                                ,p_errbuf
                                                ,p_sec_atr_val
                                               ))
       THEN
          RAISE lock_failed;
      END IF;

      FOR cur_rebill IN (  SELECT rl.rc_id
                                , rl.book_id
                                , rp.id
                                , rl.atr21
                                , rl.id line_id
                                , rl.atr37
                                , NVL(rl.rel_pct,0) rel_pct                 --EK11282018
                           FROM   rpro_rc_line_g rl
                                , rpro_rc_pob_g rp
                           WHERE  rl.batch_id        = p_batch_id
                   --      AND    rl.sec_atr_val     = p_sec_atr_val
                           AND    NVL(rl.atr58, 'N') = 'N'                  -- Processed Flag
                   --      AND    rl.atr36           = 'Y'                  -- Rebill Flag
                           AND    rl.atr37           IS NOT NULL            -- EK 11132018
                           AND    rl.doc_num         <> rl.atr37            -- EK 11132018
                           AND    rl.atr21           IS NOT NULL            -- Project Number
                           AND    rl.type            =  'SO'
                           AND    rl.rc_pob_id       =  rp.id)
      LOOP
         write_log('Processing for RCID : ' || cur_rebill.rc_id || ' ~ RC POB : ' || cur_rebill.id || ' ~ Project : ' || cur_rebill.atr21);
         BEGIN
            SELECT obj_version
            INTO   l_obj_ver
            FROM   rpro_rc_head_g
            WHERE  id      = cur_rebill.rc_id
            AND    book_id = cur_rebill.book_id
            AND    ROWNUM  = 1;
         EXCEPTION
         WHEN OTHERS THEN
            l_obj_ver := 0;
         END;
         l_rel_pct := 0;

         --Release the revenue for credit and rebill lines based on th orig so line EK01022019
         BEGIN
             SELECT  ROUND(GREATEST(NVL(MAX(SUM(CASE WHEN rpro_rc_pob_act_pkg.get_action_flag(rcp.indicators) ='R' AND NVL(rcl.ext_sll_prc,0) <> 0
                                                     THEN NVL((rcp.prcssd_amt/rcl.ext_sll_prc) * 100,0)
                                                     WHEN rpro_rc_pob_act_pkg.get_action_flag(rcp.indicators) ='D' AND NVL(rcl.ext_sll_prc,0) <> 0
                                                     THEN -1 * NVL((rcp.prcssd_amt/rcl.ext_sll_prc)* 100,0)
                                                     END
                                                )
                                           ),0
                                       ),NVL(MAX(rcl.rel_pct),0)
                                    )
                           ) rel_pct
            INTO     l_rel_pct
            FROM     rpro_rc_line_g     rcl
                    ,rpro_rc_pob_act_g  rcp
            WHERE    rcp.rc_pob_id      = rcl.rc_pob_id
            AND      rcl.atr21          = cur_rebill.atr21
            AND      rcl.doc_num        = cur_rebill.atr37
            GROUP BY rcp.rc_pob_id
                    ,rcl.rel_pct;
         EXCEPTION
         WHEN OTHERS THEN
            l_rel_pct := 0;
            write_error('Error In Credit Rebill : ' || SQLERRM);
         END;

         IF l_rel_pct <= 0 OR l_rel_pct > 100
         THEN
            write_error('Exception Release % Out of bound : ' || l_rel_pct || ' ~ for RCID : ' || cur_rebill.rc_id || ' ~ RC POB : ' || cur_rebill.id || ' ~ Project : ' || cur_rebill.atr21);  --Handled the exception EK12182018
         ELSE
            write_log('l_rel_pct'||l_rel_pct);
            IF(cur_rebill.rel_pct = 0)                                            --EK11292018
            THEN
               rpro_revenue_process_pkg.release_pob (  p_rc_id             => cur_rebill.rc_id
                                                      ,p_book_id           => cur_rebill.book_id
                                                      ,p_rc_pob_id         => cur_rebill.id
                                                      ,p_comments          => 'CREDIT REBILL PROJECT AUTO RELEASE~' || TRUNC (SYSDATE)
                                                      ,p_rc_ovn            => l_obj_ver
                                                      ,p_rel_amt           => NULL
                                                      ,p_rel_pct           => l_rel_pct
                                                      ,p_action            => 'REC'
                                                      ,p_rule_start_date   => NULL
                                                      ,p_rule_end_date     => NULL
                                                      ,p_ret_msg           => l_ret_msg
                                                );
            END IF;
            write_log('l_ret_msg : ' || l_ret_msg);
            IF l_ret_msg IS NULL
            THEN
               BEGIN
                  UPDATE rpro_rc_line_g rl
                  SET    rl.atr58 = 'Y'
                  WHERE  rl.id    = cur_rebill.line_id
                  AND    EXISTS ( SELECT 1
                                  FROM   rpro_rc_pob_act_g rpa
                                  WHERE  rpa.rc_id     = rl.rc_id
                                  AND    rpa.rc_pob_id = rl.rc_pob_id);
               EXCEPTION
               WHEN OTHERS THEN
                  write_error('Error In Credit Rebill flag : ' || SQLERRM);
               END;
            END IF;
         END IF;
      END LOOP;

      COMMIT;
      rpro_utility_pkg.remove_lock (p_sec_atr_val);
      write_log('End Credit Rebill for Batch ' || p_batch_id || '~~' || TO_CHAR ( SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   EXCEPTION
    WHEN lock_failed THEN
      p_errbuf       := 'Unable to lock application for Credit Rebill ';
      p_retcode      := 2;
      write_error ( 'Error: ' || p_errbuf);
   WHEN OTHERS THEN
      ROLLBACK;
      rpro_utility_pkg.remove_lock (p_sec_atr_val);
      p_retcode      := 2;
      p_errbuf       := 'Error In Credit Rebill : ' || SQLERRM || '-' || DBMS_UTILITY.format_error_backtrace;
      write_error(p_errbuf);
   END credit_rebill;

   PROCEDURE l3_alctd_amt (p_rc_id    NUMBER
                          ,p_batch_id NUMBER)
   AS
      r_idx                   NUMBER ;
      r_idx_rc                NUMBER ;
      CURSOR  l3_cur
      IS      SELECT prod_fmly
                    ,prod_ctgry
                    ,MAX(end_date)     end_date
                    ,SUM(alctd_xt_prc) alctd_xt_prc
                    ,MIN(orig_rev_rec_start_date)  orig_rev_rec_start_date  -- #02282019
              FROM   rpro_splunk_l3_tbl
              GROUP  BY  prod_fmly
                        ,prod_ctgry;              --#04242019

      TYPE typ_l3 IS TABLE OF l3_cur%ROWTYPE INDEX BY PLS_INTEGER;
      t_l3                       typ_l3;
      t_delete                   typ_l3;           -- #02282019

      TYPE r_chd IS RECORD  (id                       NUMBER
                            ,rc_id                    NUMBER
                            ,type                     VARCHAR2(20)
                            ,alctbl_xt_prc            VARCHAR2(20)
                            ,alctd_xt_prc             NUMBER
                            ,start_date               DATE
                            ,end_date                 DATE
                            ,prod_ctgry               VARCHAR2(300)
                            ,prod_fmly                VARCHAR2(300)
                            ,orig_rev_rec_start_date  DATE
                            ,ord_qty                  NUMBER
                            ,grp_sequence             NUMBER
                            ,current_active           NUMBER
                            ,incremental              NUMBER
                            ,max_of_current_active    NUMBER
                            ,new_term                 NUMBER
                            ,relative_amt             NUMBER
                            ,relative_pct             NUMBER
                            ,ramping_allocation       NUMBER
                            ,l_grp                    NUMBER
                            ,l_max_end_date           DATE);   --#03012019

      TYPE typ_chd_l3 IS TABLE OF r_chd INDEX BY PLS_INTEGER;
      t_l3_chd                   typ_chd_l3;
      g_limit                    NUMBER :=10000;
      l_seq                      NUMBER;
      l_ord_qty                  NUMBER;
      l_increment                NUMBER;
      l_max_curr_active          NUMBER;
      l_term                     NUMBER;
    --  l_max_end_date             DATE;            --#03012019
      l_alctd_amt                NUMBER;
      l_ramping_allocation       NUMBER;
      l_grp_seq                  NUMBER;
      l_relative                 NUMBER;
      l_relative_pct             NUMBER;
      l_curr_round               NUMBER;
      l_count                    NUMBER;
      l_orig_rev_rec_start_date  DATE;              --#03062019
      l_prior_end_date           DATE;
      l_updt_incr_value          NUMBER;            --#04242019
   BEGIN
      rpro_utility_pkg.set_revpro_context;
      write_log('*********************************************************************************');
      write_log(' START l3_alctd_amt : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      r_idx:=rpro_rc_collect_pkg.g_rc_line_data.FIRST;

      WHILE r_idx IS NOT NULL
      LOOP
         rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr28  := rpro_rc_collect_pkg.g_rc_line_data (r_idx).alctd_xt_prc;  --#03222019
         write_log('rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry)'||rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry||'rpro_rc_collect_pkg.g_rc_line_data (r_idx).rc_id'||rpro_rc_collect_pkg.g_rc_line_data (r_idx).rc_id);
         --INSERT in the Global temp table
         IF UPPER(rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry)   = 'LICENSE - TERM'
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx).alctd_xt_prc  <> 0
            --AND   NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx).fv_prc,0) <> 0        -- #02062019
            ---AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_fmly     IS NOT NULL
            AND   NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx).num1,-1)  <> 0        -- #02282019
            AND   NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr54,'N')<> 'Y'      -- #04242019
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr43         IS NOT NULL
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx).date5         IS NOT NULL
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx).end_date      IS NOT NULL
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx).start_date    IS NOT NULL
         THEN
            BEGIN
               INSERT INTO rpro_splunk_l3_tbl
               VALUES    ( rpro_rc_collect_pkg.g_rc_line_data (r_idx).id
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).rc_id
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).type
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).alctbl_xt_prc
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).alctd_xt_prc
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).start_date
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).end_date
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_ctgry
                        --- , rpro_rc_collect_pkg.g_rc_line_data (r_idx).prod_fmly
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).atr43
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).date5
                         , rpro_rc_collect_pkg.g_rc_line_data (r_idx).ord_qty
                         , NULL
                         , NULL
                         , NULL
                         , NULL
                         , NULL
                         , NULL
                         , NULL
                         , NULL
                         , NULL
                         , NULL);        --#03012019
               write_log('ORIG_REV_REC_START_DATE'||rpro_rc_collect_pkg.g_rc_line_data (r_idx).date5 ||'~'||'alctd_xt_prc'||rpro_rc_collect_pkg.g_rc_line_data (r_idx).alctd_xt_prc
                         ||'~'||'rpro_rc_collect_pkg.g_rc_line_data (r_idx).id'||rpro_rc_collect_pkg.g_rc_line_data (r_idx).id);
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error : Main Exception in Select insert into  rpro_splunk_l3_tbl  : '
                                    ||'Error : '      ||SQLERRM
                                    ||CHR(10)
                                    ||'Error Queue : '||dbms_utility.format_error_backtrace);
            END;
         END IF;
         r_idx:=rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx);
      END LOOP;

      OPEN l3_cur;      -- #02282019
      LOOP
         FETCH l3_cur BULK COLLECT INTO t_delete LIMIT g_limit;
         EXIT WHEN t_delete.COUNT = 0;
         FOR l3_delete IN 1..t_delete.COUNT
         LOOP
            --Deleting the records from the Global temp table where min of start date and max of end date existing in the records  #02282019
            BEGIN
               DELETE
               FROM   rpro_splunk_l3_tbl
               WHERE  prod_ctgry                       = t_delete(l3_delete).prod_ctgry
               AND    prod_fmly                        = t_delete(l3_delete).prod_fmly
               AND    TRUNC(orig_rev_rec_start_date)   = TRUNC(t_delete(l3_delete).orig_rev_rec_start_date)
               AND    TRUNC(end_date)                  = TRUNC(t_delete(l3_delete).end_date);
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error : Main Exception in delete from global temp table  : '
                                    ||'Error : '      ||SQLERRM
                                    ||CHR(10)
                                    ||'Error Queue : '||dbms_utility.format_error_backtrace);
            END;
            write_log('Inside delete '||'~t_delete(l3_delete).prod_fmly~'||t_delete(l3_delete).prod_fmly);
            --Max of end date storing in the temp table
            BEGIN                                            --#03012019
               UPDATE rpro_splunk_l3_tbl
               SET    l_max_end_date = t_delete(l3_delete).end_date
               WHERE  prod_ctgry     = t_delete(l3_delete).prod_ctgry
               AND    prod_fmly      = t_delete(l3_delete).prod_fmly;
            EXCEPTION
            WHEN OTHERS THEN
               write_error('Error : Main Exception in UPDATE MAX OF END DATE from global temp table  : '
                                    ||'Error : '      ||SQLERRM
                                    ||CHR(10)
                                    ||'Error Queue : '||dbms_utility.format_error_backtrace);
            END;
         END LOOP;
      END LOOP;
      CLOSE l3_cur;

      OPEN l3_cur;
      LOOP
         FETCH l3_cur BULK COLLECT INTO t_l3 LIMIT g_limit;
         EXIT WHEN t_l3.COUNT = 0;
         FOR l3_seq IN 1..t_l3.COUNT
         LOOP
            --Selecting the count of distinct orig rev rec start date and end date #02052019
            l_prior_end_date := TO_DATE('01-JAN-1800','DD-MON-YYYY');
            l_count := 0;
            BEGIN
               SELECT COUNT(*)
               INTO   l_count
               FROM  (SELECT DISTINCT orig_rev_rec_start_date
                                     ,end_date
                      FROM   rpro_splunk_l3_tbl
                      WHERE  prod_ctgry = t_l3(l3_seq).prod_ctgry
                      AND    prod_fmly  = t_l3(l3_seq).prod_fmly);
               write_log('l_count'||l_count);
            EXCEPTION
            WHEN OTHERS THEN
               l_count := 0;
               write_error('Error : count of distinct orig rev rec start date and end date  : '
                                    ||'Error : '      ||SQLERRM
                                    ||CHR(10)
                                    ||'Error Queue : '||dbms_utility.format_error_backtrace);
            END;
            --If count is greater than 1 that time only it will participate in the level3 allocation  #02052019
            IF(l_count > 1)
            THEN
               write_log('t_l3(l3_seq).prod_ctgry'||t_l3(l3_seq).prod_ctgry||'~'||t_l3(l3_seq).prod_fmly);
               --Collecting the corresponding lines to the prod_fmly and prod_ctgry from the Global temp table
               BEGIN
                  SELECT *
                  BULK   COLLECT INTO  t_l3_chd
                  FROM   rpro_splunk_l3_tbl
                  WHERE  prod_ctgry = t_l3(l3_seq).prod_ctgry
                  AND    prod_fmly  = t_l3(l3_seq).prod_fmly
                  ORDER BY orig_rev_rec_start_date
                          ,end_date ASC;                  --#04192019
               EXCEPTION
               WHEN OTHERS THEN
                  write_error('Error : Main Exception in Select UPDATE grp_sequence  : '
                                       ||'Error : '      ||SQLERRM
                                       ||CHR(10)
                                       ||'Error Queue : '||dbms_utility.format_error_backtrace);
               END;
               l_alctd_amt        := 0;
            --   l_max_end_date     := NULL;            --#03012019
               l_alctd_amt        := t_l3(l3_seq).alctd_xt_prc;
            --   l_max_end_date     := t_l3(l3_seq).end_date;  --#03012019
               l_seq              := 0;
               l_max_curr_active  := 0;
               FOR l3_seq_chd IN 1..t_l3_chd.COUNT
               LOOP
                  write_log('t_l3_chd(l3_seq_chd).id'||t_l3_chd(l3_seq_chd).id||'l_seq'||'~'||l_seq);
                   --Updating the seq in temp table
                  BEGIN
                     UPDATE rpro_splunk_l3_tbl
                     SET    grp_sequence   =  l_seq + 1
                     WHERE  id             =  t_l3_chd(l3_seq_chd).id
                     RETURNING grp_sequence INTO l_grp_seq;
                     l_seq := l_grp_seq;
                     write_log('t_l3_chd(l3_seq_chd).id'||t_l3_chd(l3_seq_chd).id);
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error : Main Exception in Select UPDATE grp_sequence  : '
                                          ||'Error : '      ||SQLERRM
                                          ||CHR(10)
                                          ||'Error Queue : '||dbms_utility.format_error_backtrace);
                  END;

                  t_l3_chd(l3_seq_chd).grp_sequence := l_grp_seq;
               END LOOP;

               --Updating the current_active in temp table
               FOR l_cur_active IN 1..t_l3_chd.COUNT
               LOOP
                  write_log('t_l3_chd(l_cur_active).grp_sequence'||t_l3_chd(l_cur_active).grp_sequence);
                  l_ord_qty                 := 0;
                  IF(l_prior_end_date BETWEEN (t_l3_chd(l_cur_active).orig_rev_rec_start_date - 3) AND (t_l3_chd(l_cur_active).orig_rev_rec_start_date + 3))          --#04242019
                  THEN
                     l_ord_qty := NVL(t_l3_chd(l_cur_active).ord_qty,0);   --#04242019
                  ELSE    --#04242019
                     FOR l_cur_active_chd IN 1..t_l3_chd.COUNT
                     LOOP
                        write_log('t_l3_chd(l_cur_active_chd).grp_sequence'||t_l3_chd(l_cur_active_chd).grp_sequence);
                        IF(t_l3_chd(l_cur_active_chd).grp_sequence >= t_l3_chd(l_cur_active).grp_sequence)
                        THEN
                           EXIT;
                        END IF;

                        IF( t_l3_chd(l_cur_active).orig_rev_rec_start_date BETWEEN t_l3_chd(l_cur_active_chd).orig_rev_rec_start_date AND t_l3_chd(l_cur_active_chd).end_date)
                        OR ( t_l3_chd(l_cur_active).end_date BETWEEN t_l3_chd(l_cur_active_chd).orig_rev_rec_start_date AND t_l3_chd(l_cur_active_chd).end_date)
                        THEN
                           l_ord_qty := l_ord_qty + t_l3_chd(l_cur_active_chd).ord_qty;
                        END IF;   --#04242019
                     END LOOP;
                     l_ord_qty := l_ord_qty + t_l3_chd(l_cur_active).ord_qty;
                  END IF;
                  BEGIN
                     UPDATE rpro_splunk_l3_tbl
                     SET    current_active            = l_ord_qty
                     WHERE  id                        = t_l3_chd(l_cur_active).id;
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error : Main Exception in Select UPDATE current_active  : '
                                          ||'Error : '      ||SQLERRM
                                          ||CHR(10)
                                          ||'Error Queue : '||dbms_utility.format_error_backtrace);
                  END;
                  l_prior_end_date                      := t_l3_chd(l_cur_active).end_date;
                  t_l3_chd(l_cur_active).current_active := l_ord_qty;
                  write_log('t_l3_chd(l_cur_active).current_active'||t_l3_chd(l_cur_active).current_active||'t_l3_chd(l_cur_active).id'||t_l3_chd(l_cur_active).id||'l_ord_qty'||l_ord_qty);
               END LOOP;

               --Updating the max_of_current_active in temp table
               FOR l3_max_val IN 1..t_l3_chd.COUNT
               LOOP
                  write_log('t_l3_chd(l3_max_val).grp_sequence'||t_l3_chd(l3_max_val).grp_sequence||'~'||'t_l3_chd(l3_max_val).prod_ctgry'||t_l3_chd(l3_max_val).prod_ctgry||'~'||'t_l3_chd(l3_max_val).prod_fmly'||t_l3_chd(l3_max_val).prod_fmly);
                  l_max_curr_active := 0;
                  BEGIN
                     SELECT MAX(current_active)
                     INTO   l_max_curr_active
                     FROM   rpro_splunk_l3_tbl
                     WHERE  grp_sequence < t_l3_chd(l3_max_val).grp_sequence
                     AND    prod_ctgry   = t_l3_chd(l3_max_val).prod_ctgry
                     AND    prod_fmly    = t_l3_chd(l3_max_val).prod_fmly;
                     write_log('l_max_curr_active'||l_max_curr_active);
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_log('l_max_curr_active'||-1);
                     write_error('Error : Main Exception in Select l_max_curr_active  : '
                                          ||'Error : '      ||SQLERRM
                                          ||CHR(10)
                                          ||'Error Queue : '||dbms_utility.format_error_backtrace);
                  END;

                  BEGIN
                     UPDATE rpro_splunk_l3_tbl
                     SET    max_of_current_active = NVL(l_max_curr_active,0)
                     WHERE  id                    = t_l3_chd(l3_max_val).id;
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error : Main Exception in UPDATE max_of_current_active  : '
                                          ||'Error : '      ||SQLERRM
                                          ||CHR(10)
                                          ||'Error Queue : '||dbms_utility.format_error_backtrace);
                  END;

                  t_l3_chd(l3_max_val).max_of_current_active    := NVL(l_max_curr_active,0);
                  write_log('t_l3_chd(l3_max_val).max_of_current_active'||t_l3_chd(l3_max_val).max_of_current_active||'t_l3_chd(l3_max_val).id'||'~'||t_l3_chd(l3_max_val).id||'~'||'l_max_curr_active'||'~'||l_max_curr_active);
               END LOOP;
               l_prior_end_date := TO_DATE('01-JAN-1800','DD-MON-YYYY');  --#04242019
               --Updating the incremental,new_term and relative_amt in the temp table
               FOR l3_increment IN 1..t_l3_chd.COUNT
               LOOP
                  write_log('t_l3_chd(l3_increment).current_active'||t_l3_chd(l3_increment).current_active);
                  l_relative   := 0;
                  write_log('l_prior_end_date-'||l_prior_end_date
                            ||CHR(10)
                            ||'orig_rev_rec_start_date - 3-'
                            ||(t_l3_chd(l3_increment).orig_rev_rec_start_date - 3)
                            ||CHR(10)
                            ||'orig_rev_rec_start_date + 3~'
                            ||(t_l3_chd(l3_increment).orig_rev_rec_start_date + 3)
                            ||CHR(10)
                            ||'Order Qty ~'||t_l3_chd(l3_increment).ord_qty
                            ||CHR(10)
                            ||'Max of Current Qty ~'||t_l3_chd(l3_increment).max_of_current_active
                            ||CHR(10)
                            ||'Current Active qty ~'||t_l3_chd(l3_increment).current_active
                            ||CHR(10)
                            ||'Incre qty~'||l_increment);
                  IF(l_prior_end_date BETWEEN (t_l3_chd(l3_increment).orig_rev_rec_start_date - 3) AND (t_l3_chd(l3_increment).orig_rev_rec_start_date + 3))          --#04192019
                  THEN
                     l_increment  := NVL(t_l3_chd(l3_increment).ord_qty - t_l3_chd(l3_increment).max_of_current_active,0);             --#04192019
                  ELSE                                                                                                                 --#04192019
                     l_increment  := NVL(t_l3_chd(l3_increment).current_active - t_l3_chd(l3_increment).max_of_current_active,0);
                  END IF;
                  l_term       := ROUND(NVL(((t_l3_chd(l3_increment).l_max_end_date - t_l3_chd(l3_increment).orig_rev_rec_start_date + 1)/365)*12,0),15);  --#03012019

                  --incremental value updating based on the sign of the amount
                  IF(NVL(l_increment,0) <= 0 )
                  THEN
                     BEGIN
                        UPDATE rpro_splunk_l3_tbl
                        SET    incremental                     = 0
                              ,new_term                        = l_term
                              ,relative_amt                    = 0
                        WHERE  id                              = t_l3_chd(l3_increment).id;
                        t_l3_chd(l3_increment).relative_amt   := 0;
                        t_l3_chd(l3_increment).incremental    := 0;
                        l_prior_end_date                      := t_l3_chd(l3_increment).end_date;    --#04192019

                     EXCEPTION
                     WHEN OTHERS THEN
                        write_error('Error : Main Exception in UPDATE relative_amt as 0  : '
                                             ||'Error : '      ||SQLERRM
                                             ||CHR(10)
                                             ||'Error Queue : '||dbms_utility.format_error_backtrace);
                     END;
                  ELSE
                     l_relative := l_increment * l_term;

                     BEGIN
                        UPDATE rpro_splunk_l3_tbl
                        SET    incremental                     = l_increment
                              ,new_term                        = l_term
                              ,relative_amt                    = l_relative
                        WHERE  id                              = t_l3_chd(l3_increment).id;
                        t_l3_chd(l3_increment).relative_amt   := l_relative;
                        t_l3_chd(l3_increment).incremental    := l_increment;
                        l_prior_end_date                      := t_l3_chd(l3_increment).end_date;     --#04192019
                     EXCEPTION
                     WHEN OTHERS THEN
                        write_error('Error : Main Exception in UPDATE relative_amt  : '
                                             ||'Error : '      ||SQLERRM
                                             ||CHR(10)
                                             ||'Error Queue : '||dbms_utility.format_error_backtrace);
                     END;
                  END IF;

                  t_l3_chd(l3_increment).new_term       := l_term;
                  write_log('t_l3_chd(l3_increment).incremental'||t_l3_chd(l3_increment).incremental||'t_l3_chd(l3_increment).new_term'||'~'||t_l3_chd(l3_increment).new_term||'t_l3_chd(l3_increment).id'||'~'||t_l3_chd(l3_increment).id);
               END LOOP;

               --Updateing the ramping_allocation amount
               FOR l3_rel_pct IN 1..t_l3_chd.COUNT
               LOOP
                  BEGIN
                     l_relative_pct := 0;
                     write_log('t_l3_chd(l3_rel_pct).relative_amt'||t_l3_chd(l3_rel_pct).relative_amt||'~'||'l_relative_pct'||l_relative_pct);

                     SELECT 100 * t_l3_chd(l3_rel_pct).relative_amt/DECODE(SUM(relative_amt),0,1,SUM(relative_amt) )
                     INTO   l_relative_pct
                     FROM   rpro_splunk_l3_tbl
                     WHERE  prod_ctgry = t_l3_chd(l3_rel_pct).prod_ctgry
                     AND    prod_fmly  = t_l3_chd(l3_rel_pct).prod_fmly
                     GROUP  BY prod_ctgry
                              ,prod_fmly;
                     write_log('t_l3_chd(l3_rel_pct).prod_ctgry'||t_l3_chd(l3_rel_pct).prod_ctgry||'l_relative_pct'||l_relative_pct||'t_l3_chd(l3_rel_pct).prod_fmly'||t_l3_chd(l3_rel_pct).prod_fmly);
                  EXCEPTION
                  WHEN OTHERS THEN
                     l_relative_pct := 0;
                     write_error('Error : Main Exception in Select l_relative_pct  : '
                                          ||'Error : '      ||SQLERRM
                                          ||CHR(10)
                                          ||'Error Queue : '||dbms_utility.format_error_backtrace);
                  END;

                  write_log('l_relative_pct'||l_relative_pct||'~'||'l_alctd_amt'||l_alctd_amt);
                  BEGIN
                     UPDATE rpro_splunk_l3_tbl
                     SET    ramping_allocation    = (l_relative_pct * l_alctd_amt )/100
                           ,relative_pct          = l_relative_pct   --#04242019
                     WHERE  id                    = t_l3_chd(l3_rel_pct).id;
                  EXCEPTION
                  WHEN OTHERS THEN
                     write_error('Error : Main Exception in UPDATE l_ramping_allocation  : '
                                          ||'Error : '      ||SQLERRM
                                          ||CHR(10)
                                          ||'Error Queue : '||dbms_utility.format_error_backtrace);
                  END;
               END LOOP;
            END IF;
         END LOOP;
      END LOOP;
      CLOSE l3_cur;

      --Assign the ramping_allocation value to the rc collection
      r_idx_rc:=rpro_rc_collect_pkg.g_rc_line_data.FIRST;
      WHILE r_idx_rc IS NOT NULL
      LOOP
         l_ramping_allocation      := NULL;
         l_orig_rev_rec_start_date := NULL;           --#03062019
         IF UPPER(rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).prod_ctgry)   = 'LICENSE - TERM'   -- #03012019
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).alctd_xt_prc  <> 0
            AND   NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).num1,-1)  <> 0
            AND   NVL(rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).atr54,'N')<> 'Y'      -- #04242019
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).atr43         IS NOT NULL
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).date5         IS NOT NULL
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).end_date      IS NOT NULL
            AND   rpro_rc_collect_pkg.g_rc_line_data (r_idx_rc).start_date    IS NOT NULL
         THEN
            BEGIN
               SELECT ramping_allocation
                     ,orig_rev_rec_start_date
                     ,incremental                    --#04242019
               INTO   l_ramping_allocation
                     ,l_orig_rev_rec_start_date       --#03062019
                     ,l_updt_incr_value               --#04242019
               FROM   rpro_splunk_l3_tbl
               WHERE  id = rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).id;
               write_log('rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).id'||rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).id||'~'||'l_ramping_allocation'||l_ramping_allocation);
            EXCEPTION
            WHEN OTHERS THEN
               l_ramping_allocation := NULL;
            END;
         END IF;
         l_curr_round     := NVL(rpro_utility_pkg.curr_rounding(rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).curr) , 2);
         write_log(' l_curr_round: '||l_curr_round);
         IF(l_ramping_allocation IS NOT NULL)
         THEN
            rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).alctd_xt_prc := ROUND(l_ramping_allocation,l_curr_round);
            rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).start_date   := l_orig_rev_rec_start_date;   ----#03062019
            rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).atr53        := l_updt_incr_value;           ----#04242019
            write_log(' alctd_xt_prc: '||rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).alctd_xt_prc||'~'||' id: '||rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).id||'~'||' Doc_line_id: '||rpro_rc_collect_pkg.g_rc_line_data(r_idx_rc).doc_line_id);
         END IF;

      r_idx_rc:=rpro_rc_collect_pkg.g_rc_line_data.NEXT(r_idx_rc);
      END LOOP;
      write_log(' END l3_alctd_amt : '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      write_log('**************************************************************************');
   EXCEPTION
   WHEN OTHERS THEN
      write_error('Error : Main Exception in l3_alctd_amt : '
                           ||'Error : '      ||SQLERRM
                           ||CHR(10)
                           ||'Error Queue : '||dbms_utility.format_error_backtrace);
   END l3_alctd_amt;
------------------------------------------------------------------------------------------
--LV20200401
   PROCEDURE splunk_mje_report( p_report_id       IN     NUMBER
                              , p_rep_layout_id   IN     NUMBER
                              , p_no_tags         IN     VARCHAR2 DEFAULT 'N'
                              , p_restrict_rows   IN     VARCHAR2 DEFAULT 'N'
                              , p_query              OUT VARCHAR2
                              , p_header             OUT VARCHAR2
                              , p_num_rows           OUT NUMBER
                              )
   AS
      l_report_limit    VARCHAR2(300);
      l_where1          VARCHAR2(32627);
      l_column_value    VARCHAR2(300);
      L_FLTR            VARCHAR2(300);
      l_query           VARCHAR2(32627);
      l_prd_id          VARCHAR2(32627);
      L_PRD_QUERY       VARCHAR2(32627);
      l_start_prd_id    NUMBER;
      l_end_prd_id      NUMBER;
      i                 NUMBER;
      l_err_flag        VARCHAR2(240):='N';
      l_ui_type         VARCHAR2(240);
      l_output_fmt      VARCHAR2(240);
      l_alignment       VARCHAR2(240);
      l_header          VARCHAR2(32627);
      l_nvl_query       VARCHAR2(32627);
      l_selected_columns VARCHAR2(32627);
      l_cnt              NUMBER:=0;
      TYPE rec_header_formation IS RECORD ( seq      NUMBER
                                           ,fld_name VARCHAR2(30000)
                                           ,label    VARCHAR2(30000)
                                           ,col_type VARCHAR2(30000)
                                           ,alias    VARCHAR2(30000));

      TYPE typ_header_formation IS TABLE OF rec_header_formation
      INDEX BY PLS_INTEGER;

      l_header_formation   typ_header_formation;

   BEGIN
      l_ui_type    := rpro_utility_pkg.get_ui_type;
      l_output_fmt := CASE WHEN l_ui_type = 'G' THEN 'JSON' ELSE 'UNKNOWN' END;
      create_mje_report_script;
      i       := rpro_rp_frmwrk_pkg.g_param.FIRST;

      l_where1:= ' AND';

      WHILE (i IS NOT NULL)
      LOOP
         l_column_value      := rpro_rp_frmwrk_pkg.g_param (i).column_value;
         IF l_column_value IS NOT NULL
         THEN
            IF  rpro_rp_frmwrk_pkg.g_param (i).alias IS NOT NULL
            AND rpro_rp_frmwrk_pkg.g_param (i).operator  = 'BETWEEN'
            THEN
                l_where1      :=    l_where1
                                    || ' AND '
                                    || rpro_rp_frmwrk_pkg.g_param (i).alias
                                    || '.'
                                    || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                    || ' BETWEEN :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                    || ' AND :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).to_bind_variable;
            ELSIF  rpro_rp_frmwrk_pkg.g_param (i).alias IS NOT NULL
            AND rpro_rp_frmwrk_pkg.g_param (i).operator  = 'BETWEEN'
         THEN
                l_where1      :=    l_where1
                                    || ' AND '
                                    || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                    || ' BETWEEN :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).bind_variable
                                    || ' AND :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).to_bind_variable;
            END IF;

            IF  rpro_rp_frmwrk_pkg.g_param (i).alias     IS NOT NULL
            AND rpro_rp_frmwrk_pkg.g_param (i).operator  = '='
            THEN
                l_where1      :=    l_where1
                                    || ' AND '
                                    || rpro_rp_frmwrk_pkg.g_param (i).alias
                                    || '.'
                                    || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                    || ' = :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).bind_variable;
            ELSIF rpro_rp_frmwrk_pkg.g_param (i).alias     IS NOT NULL
            AND rpro_rp_frmwrk_pkg.g_param (i).operator  = '='
         THEN
                l_where1      :=    l_where1
                                    || ' AND '
                                    || rpro_rp_frmwrk_pkg.g_param (i).column_name
                                    || ' = :'
                                    || rpro_rp_frmwrk_pkg.g_param (i).bind_variable;
            END IF;
          END IF;

         i                   := rpro_rp_frmwrk_pkg.g_param.NEXT (I);
      END LOOP;
      write_log('l_where1:'||l_where1);

      FOR I IN (SELECT lf.seq
                      ,lbl.report_label label
                      ,rf.fld_name
                      ,lbl.col_type
                      ,rf.alias
                FROM rpro_layout_field_g lf
                     ,rpro_rep_field_g rf
                     ,rpro_label_g lbl
                WHERE rf.id      = lf.field_id
                AND   lbl.id       = rf.label_id
                AND   lf.layout_id = p_rep_layout_id
                ORDER BY lf.seq ASC )
      LOOP
         l_cnt := l_cnt+1;
         l_header_formation(l_cnt).seq       :=i.seq;
         l_header_formation(l_cnt).label     :=i.label;
         l_header_formation(l_cnt).fld_name  :=i.fld_name;
         l_header_formation(l_cnt).col_type  :=i.col_type;
         l_header_formation(l_cnt).alias     :=CASE WHEN i.alias IS NOT NULL THEN i.alias||'.'||i.fld_name ELSE i.fld_name END;
      END LOOP;
         write_log('l_header_formation.count:'||l_header_formation.count);
         write_log('l_header_formation.FIRST:'||l_header_formation.FIRST);
      IF l_output_fmt = 'JSON'
      THEN
         FOR i IN  l_header_formation.FIRST..l_header_formation.LAST
         LOOP
          l_selected_columns :=l_selected_columns||l_header_formation(i).alias||',';
          l_alignment := ' "alignment":"left",';
          l_header := l_header ||',{'
                               ||' "dataField":"'||l_header_formation(i).fld_name||'",'
                               ||RPRO_RP_FRMWRK_PKG.GET_JSON_HDR_TYPE(l_header_formation(i).col_type)
                               ||l_alignment
                               ||' "caption":"'||l_header_formation(i).label||'"'
                               ||'}';
          END LOOP;
      ELSE
         --i := l_header_formation.FIRST;
         FOR i IN  l_header_formation.FIRST..l_header_formation.LAST
         LOOP
            l_header           :=l_header||l_header_formation(i).label||':';
            l_selected_columns :=l_selected_columns||RTRIM(LTRIM(l_header_formation(i).alias,'.'),'.')||',';
         END LOOP;
      END IF;
     -- END LOOP;
      l_header          :=RTRIM(LTRIM(l_header,':'),':');
      l_header          :=RTRIM(LTRIM(l_header,','),',');
      l_selected_columns:=RTRIM(LTRIM(l_selected_columns,','),',');

      l_where1 := RTRIM(LTRIM(l_where1,'AND'),'AND');
      l_query:='SELECT '||l_selected_columns||' FROM rpro_splunk_custom_je_line_g JESCH,rpro_splunk_custom_je_head_g RJEH
      WHERE 1 = 1 AND JESCH.header_id = RJEH.id '||l_where1||' ';
      l_query := RTRIM(LTRIM(l_query,'AND'),'AND');

      IF l_output_fmt = 'JSON'
      THEN
         l_header:= '{"header":['||l_header||'],"rc_id":[{ "dataField":"RC_ID"}],"drill_allowed":"N","child_layouts":[]}';
      END IF;
      rpro_utility_pkg.record_err_act(p_type       =>   'Contractual Waterfall creation'
                                     ,p_text       =>   'l_query:'
                                     ,p_line_id    =>   NULL
                                     ,p_rc_id      =>   NULL
                                     ,p_pob_id     =>   NULL
                                     ,p_clob_text  =>   l_query);
      write_log('l_query:'||l_query);
      write_log('p_header:'||l_header);
      p_query:=l_query;
      p_header:=l_header;
   EXCEPTION
   WHEN others THEN
      p_query := 'SELECT ''String length exceeds the size,Use Download Report option'' ALERT from dual';
      p_header:= NULL;
      p_header := '{"header":[{'
                                  ||' "dataField":"'||'ALERT'||'",'
                                  ||RPRO_RP_FRMWRK_PKG.GET_JSON_HDR_TYPE('VARCHAR2')
                                  ||l_alignment
                                  ||' "caption":"'||'Warning'||'"'
                                  ||'}]}';
      write_error('p_header:'||p_header);
      write_error('Main Exception splunk_mje_report:'||dbms_utility.format_error_backtrace||'Error : '||SQLERRM);
   END splunk_mje_report;

END rpro_splk_fgaap_custom_pkg;
/
