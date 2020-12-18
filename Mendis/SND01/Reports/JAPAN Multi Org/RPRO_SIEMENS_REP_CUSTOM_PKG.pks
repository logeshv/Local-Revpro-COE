create or replace PACKAGE RPRO_SIEMENS_REP_CUSTOM_PKG
AS
   /*===================================================================================================+
   |                              ZUORA, San Jose, California                                           |
   +====================================================================================================+
   |                                                                                                    |
   |    File Name:         RPRO_SIEMENS_REP_CUSTOM_PKG.pks                                              |
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
   +===================================================================================================*/



   TYPE tab_schd_rec IS RECORD( line_id          rpro_rc_line_g.id %TYPE
                               ,rc_id            rpro_rc_line_g.rc_id%TYPE
                               ,start_date       rpro_rc_line_g.start_date%TYPE
                               ,end_date         rpro_rc_line_g.end_date%TYPE
                               ,ext_sll_prc      rpro_rc_line_g.ext_sll_prc%TYPE
                               ,doc_date         rpro_rc_line_g.doc_date%TYPE
                               ,book_id          rpro_rc_line_g.book_id%TYPE
                               ,sec_atr_val      rpro_rc_line_g.sec_atr_val%TYPE
                               ,curr             rpro_rc_line_g.curr%TYPE
                               ,f_ex_rate        rpro_rc_line_g.f_ex_rate%TYPE
                               ,g_ex_rate        rpro_rc_line_g.g_ex_rate%TYPE
                               ,no_start_date    DATE
                               ,no_end_date      DATE
                               ,no_cum_alctd_amt NUMBER
                               );

    TYPE ooh_indx_tab IS TABLE OF PLS_INTEGER
                       INDEX BY PLS_INTEGER;

    g_ooh_schd_data    all_tab_pkg.rc_schd_data_tab;
    g_ooh_indx_tab     ooh_indx_tab;
    g_tab_name         VARCHAR2(240);

    PROCEDURE no_rev_schd_generation( p_errbuf   OUT  VARCHAR2
                                    , p_retcode  OUT  NUMBER
                                    , p_batch_id IN   NUMBER DEFAULT NULL);


    PROCEDURE siemens_waterfall_jpn_rep (p_report_id       IN     NUMBER
                                    ,p_rep_layout_id   IN     NUMBER
                                    ,p_no_tags         IN     VARCHAR2 DEFAULT 'N'
                                    ,p_restrict_rows   IN     VARCHAR2 DEFAULT 'N'
                                    ,p_query          OUT     VARCHAR2
                                    ,p_header         OUT     VARCHAR2
                                    ,p_num_rows       OUT     NUMBER
                                    );
    PROCEDURE tax_acct_reclass (p_errbuf         OUT VARCHAR2
                               ,p_retcode        OUT NUMBER);
    PROCEDURE tax_je_schd_flg (p_errbuf         OUT VARCHAR2
                              ,p_retcode        OUT NUMBER
                              ,p_tax_org_id     IN  VARCHAR2);
END RPRO_SIEMENS_REP_CUSTOM_PKG;
/