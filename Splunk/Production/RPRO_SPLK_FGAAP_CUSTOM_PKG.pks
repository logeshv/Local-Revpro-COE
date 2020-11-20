CREATE OR REPLACE PACKAGE  RPRO_SPLK_FGAAP_CUSTOM_PKG
AS
  /**************************************************************************************************
  * Name : RPRO_SPLK_FGAAP_CUSTOM_PKG.pks *
  * Author : LEEYO *
  * Date : 01-Nov-2016 *
  * Description : *
  * Version : *
  * $Header: RPRO_SPLK_FGAAP_CUSTOM_PKG.pks 1 2015-09-31 20:35:45 admin $ *
  * Modifications History: *
  * *
  * Modified By           Date        Version     Description *
  * --------------      ----------- ------------  -----------------------------------------*
  Implementation
    Sukesh            17-Oct-2018   1.0          MJE Validation
    Zuora             04-DEC-2018   2.0          Added splunk_qtd_wf_rep procedure
    Elakkiya          09-Nov-2018   3.0          l3_alctd_amt procedure
  **************************************************************************************************/
   g_amt_format    VARCHAR2 (100);
   g_date_format   VARCHAR2 (100);
   g_num_format    VARCHAR2 (100);
   g_pct_format    VARCHAR2 (100);
   g_qty_format    VARCHAR2 (100);
    PROCEDURE second_lvl_allocation (
        p_rc_id      IN VARCHAR2 DEFAULT NULL,
        p_batch_id   IN NUMBER
    );

    PROCEDURE before_validate (
        p_rc_id      VARCHAR2,
        p_batch_id   NUMBER
    );

    PROCEDURE rpro_gl_batch_split_by_rc (
        p_rc_id      IN VARCHAR2 DEFAULT NULL,
        p_batch_id   IN NUMBER
    );

    PROCEDURE merge_rc (
        p_errbuf     OUT VARCHAR2,
        p_retcode    OUT NUMBER,
        p_batch_id   IN NUMBER,
        p_rc_id      IN NUMBER
    );

    PROCEDURE rpro_splk_unbill_rep (
        p_errbuf    OUT VARCHAR2,
        p_retcode   OUT NUMBER
    );

    FUNCTION rpro_splk_schd_amt (
        p_line_id   IN NUMBER,
        p_type      IN VARCHAR2,
        p_prd_id    IN NUMBER
    ) RETURN NUMBER;

    FUNCTION get_unbilled_amt (p_root_line_id     IN   rpro_rc_line_g.id%TYPE
                             ,p_type             IN   VARCHAR2
                             ,p_prd_id           IN   rpro_calendar_g.id%TYPE)
    RETURN NUMBER ;

    PROCEDURE rpro_fv_bkp_prc ( p_rc_id       NUMBER
                               ,p_batch_id    NUMBER );

    PROCEDURE rpro_lvl2_pct_drv ( p_rc_id       NUMBER
                                 ,p_batch_id    NUMBER );

   PROCEDURE rpro_after_fv_calc ( p_rc_id       NUMBER
                                ,p_batch_id    NUMBER );

   PROCEDURE mje_valdation   ( p_rc_id            NUMBER DEFAULT NULL
                              ,p_batch_id         NUMBER);

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
                               ,p_errbuf       IN OUT    NUMBER
                               ,p_errmsg       IN OUT    VARCHAR2);

   PROCEDURE log_je_error         (  p_header_id   IN NUMBER
                                    ,p_line_id     IN NUMBER
                                    ,p_type        IN VARCHAR2
                                    ,p_description IN rpro_je_error_g.type%TYPE
                                    ,p_client_id   IN NUMBER
                                    ,p_sec_atr_val IN rpro_je_error_g.sec_atr_val%TYPE
                                    ,p_book_id     IN rpro_je_error_g.book_id%TYPE  );

   PROCEDURE credit_rebill_wrap ( p_rc_id       NUMBER
                                 ,p_batch_id    NUMBER );

   PROCEDURE credit_rebill (  p_errbuf           OUT  VARCHAR2
                             ,p_retcode          OUT  NUMBER
                             ,p_batch_id         IN   NUMBER
                             ,p_sec_atr_val      IN   VARCHAR2 DEFAULT NULL
                           );

   TYPE get_prd_dt_rec IS RECORD(p_prd_start_date  DATE                    --#20191010
                                ,p_prd_end_date    DATE
                                ,p_qtr_start_date  DATE
                                ,p_qtr_end_date    DATE
                                ,p_period_name     VARCHAR2(10));

   PROCEDURE l3_alctd_amt (p_rc_id    NUMBER
                          ,p_batch_id NUMBER);

   FUNCTION chk_within_assp_flg ( p_ord_qty       IN    NUMBER
                                 ,p_prod_fmly     IN    VARCHAR2
                                 ,p_prod_ctgry    IN    VARCHAR2
                                 ,p_alctd_xt_prc  IN    NUMBER
                                 ,p_term          IN    NUMBER
                                 ,p_atr47         IN    NUMBER )
   RETURN VARCHAR2 ;

   PROCEDURE alctd_prc_within_assp ( p_rc_id     IN    VARCHAR2 DEFAULT NULL
                                    ,p_batch_id  IN    NUMBER );

   PROCEDURE splunk_qtd_wf_rep ( p_report_id       IN     NUMBER
                                ,p_rep_layout_id   IN     NUMBER
                                ,p_no_tags         IN     VARCHAR2 DEFAULT 'N'
                                ,p_restrict_rows   IN     VARCHAR2 DEFAULT 'N'
                                ,p_query           OUT    VARCHAR2
                                ,p_header          OUT    VARCHAR2
                                ,p_num_rows        OUT    NUMBER
                               );
    PROCEDURE create_mje_from_unbill_rep(  p_errbuf OUT VARCHAR2
                                        ,  p_retcode OUT NUMBER ) ;
    PROCEDURE create_mje_report_script;
   PROCEDURE splunk_mje_report ( p_report_id       IN     NUMBER
                                ,p_rep_layout_id   IN     NUMBER
                                ,p_no_tags         IN     VARCHAR2 DEFAULT 'N'
                                ,p_restrict_rows   IN     VARCHAR2 DEFAULT 'N'
                                ,p_query           OUT    VARCHAR2
                                ,p_header          OUT    VARCHAR2
                                ,p_num_rows        OUT    NUMBER
                               );
END RPRO_SPLK_FGAAP_CUSTOM_PKG;
/

