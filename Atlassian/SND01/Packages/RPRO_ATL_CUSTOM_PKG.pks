CREATE OR REPLACE PACKAGE rpro_atl_custom_pkg
IS

    /*==========================================================================================+
   |                    ZUORA, San Jose, California                                            |
   +===========================================================================================+
   |                                                                                           |
   |    File Name:         RPRO_ATL_CUSTOM_PKG.pks                                             |
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
   +===========================================================================================*/
   g_batch_id NUMBER := rpro_utility_pkg.g_batch_id;
   TYPE rcrd_so_num_idx IS RECORD (so_num                   VARCHAR2(256)
                                  ,ct_num                   VARCHAR2(256)
                                  ,within_fv_flag_y         NUMBER
                                  ,within_fv_flag_n         NUMBER);
   TYPE typ_so_num_idx IS TABLE OF rcrd_so_num_idx
   INDEX BY VARCHAR(1000);
   tab_so_num_idx        typ_so_num_idx;
   TYPE rc_rec
   IS
   RECORD( rc_id   NUMBER
         , atr43   VARCHAR2(100)
         , doc_num VARCHAR2(100)
         , atr17   VARCHAR2(500)
         , atr41   VARCHAR2(500));

   TYPE rc_rec_multiset
   IS
   RECORD( rc_id   NUMBER
         , doc_num VARCHAR2(100)
         , atr17   VARCHAR2(500)
         , atr41   VARCHAR2(500));

   TYPE rc_rec_type
   IS
   TABLE OF rc_rec INDEX BY pls_integer;

   TYPE tab_rec_type_multiset
   IS
   TABLE OF rc_rec_multiset INDEX BY pls_integer;

   rc_tab rc_rec_type ;

   TYPE rc_type
   IS
   TABLE OF rc_rec_multiset;

   rc_tab_type rc_type  := rc_type() ;
   rc_tab_type_dist_rc  tab_rec_type_multiset;
   l_proc VARCHAR2(100) :='RPRO_ATL_CUSTOM_PKG';

   PROCEDURE rpro_atl_create_manual_rc(p_errbuf out VARCHAR2
                                      ,p_retcode out NUMBER );

   PROCEDURE rpro_atl_recursive_itrtn(p_temp_inv_number VARCHAR2
                                     ,p_mod_sen_num VARCHAR2
                                     ,p_prnt_st_dt DATE
                                     ,p_ref_rc_id  NUMBER DEFAULT NULL                   --20201119
                                     );

   PROCEDURE rpro_atl_delink( p_rc_id IN NUMBER
                             ,p_atr17 IN VARCHAR2
                             ,p_atr41 IN VARCHAR2);

      counter           NUMBER;
      l_cnt             NUMBER:=0;
      l_temp_inv_number VARCHAR2(1000);
      l_mod_sen_number  VARCHAR2(1000);
      l_prnt_st_dt      DATE;

      g_min_doc_date    DATE;         --20201104
      g_min_rc_id       NUMBER;       --20201104
      
   PROCEDURE rpro_after_bndl_explode (p_rc_id      IN VARCHAR2 DEFAULT NULL
                                     ,p_batch_id   IN NUMBER
                                     );

   PROCEDURE before_validate (p_rc_id    NUMBER DEFAULT NULL
                             ,p_batch_id NUMBER); --
   
   PROCEDURE after_fv_analysis(p_rc_id     IN   NUMBER
                              ,p_batch_id  IN   NUMBER);
                              
   PROCEDURE after_validate(p_rc_id    IN NUMBER 
                           ,p_batch_id IN NUMBER);
                           
   PROCEDURE upd_ct_mod_flag ( p_rc_id    IN NUMBER DEFAULT NULL
                             , p_batch_id IN NUMBER DEFAULT NULL);
                           
   PROCEDURE cust_link_delink(p_src_rc_id  IN NUMBER
                             ,p_dest_rc_id IN NUMBER
                             ,p_batch_id   IN NUMBER
                             ,p_line_id    IN NUMBER
                             ,p_rc_pob_id  IN NUMBER);

   PROCEDURE cust_link_delink_cv(p_src_rc_id  IN NUMBER
                                ,p_dest_rc_id IN NUMBER
                                ,p_batch_id   IN NUMBER
                                ,p_line_id    IN NUMBER
                                ,p_rc_pob_id  IN NUMBER);
                             
   PROCEDURE switch_link_delink(  p_retcode    OUT NUMBER
                                , p_errbuf     OUT VARCHAR
                                , p_batch_id   IN  NUMBER DEFAULT NULL
                                , p_rc_id      IN  NUMBER DEFAULT NULL);
   
   PROCEDURE switch_link_delink_cv(  p_retcode    OUT NUMBER  
                                   , p_errbuf     OUT VARCHAR
                                   , p_batch_id   IN  NUMBER DEFAULT NULL
                                   , p_rc_id      IN  NUMBER DEFAULT NULL);
   
   PROCEDURE cmod_wrapper( p_retcode    OUT NUMBER
                         , p_errbuf     OUT VARCHAR
                         , p_batch_id   IN  NUMBER DEFAULT NULL
                         , p_rc_id      IN  NUMBER DEFAULT NULL);
                         
   PROCEDURE fv_roll_up_wrapper ( p_rc_id    IN NUMBER DEFAULT NULL
                                , p_batch_id IN NUMBER DEFAULT NULL);
                                
   PROCEDURE rpro_cmod_link_delink( p_rc_id      IN VARCHAR2 DEFAULT NULL
                                  , p_batch_id   IN NUMBER );
                                  
END rpro_atl_custom_pkg;
/