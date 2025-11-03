create or replace PACKAGE BODY                  "LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG" 
IS 
/*     $Header: LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.pkb 1.0.0 2024/05/17 09:00:00 vsre noship $ */
-- ***************************************************************************
-- @(#) ----------------------------------------------------------------------
-- @(#) Specifique: DTH IMPORT des données dans le Silver - DataHub Customers NOT_ORACLE
-- @(#) Fichier   : .LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.pkb
-- @(#) Version   : 1.0.0 du 17/05/2024
-- @(#) ---------------------------------------------e-------------------------
-- Objet          : Package Body du DTH003 IMPORT des donn�es Silver
-- Commentaires   :
-- Exemple        :
-- ***************************************************************************
--                            HISTORIQUE DES VERSIONS
-- ---------------------------------------------------------------------------
-- Date     Version  Nom            Description de l'intervention
-- -------- -------- -------------- ------------------------------------------
-- 17/07/24  1.0.0   COUVY			Version initiale
-- 29/08/24  1.0.1   POTC           Ajout de champs 
-- 12/09/24  1.0.2   POTC           R�organisation de la gestion des logs
-- 01/10/24  1.0.3   JABIT          Ajout de champs
-- 14/10/24  2.0.0   POTC           Renommage des table et package de ASIA en NOT_ORACLE pour y va y rajouter le chargement de BRAZIL 
-- 16/10/24  2.0.1   POTC           Ajout de la cr�ation de la table CUSTOMER_BRAZIL_TEMP en faisant une copie d'un client oracle coresspondant � Schindler Brazil site_use_id = 40360
-- 05/11/24  3.0.0   POTC           Changement de la gestion des logs 
-- 10/01/25  3.0.1   POTC           Chargement du CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6 � partir de CUSTOMER_GROUP_NOT_ORACLE pour les client NOT ORACLE
-- 25/02/25  3.0.2   POTC           Ajout du champ REVENUE_NATURALACCOUNT_GL_SGT4 et renommage de CUSTOMER_ACCOUNT_NATURALACCOUNT_GL_SGT4 en RECEIVABLE_NATURALACCOUNT_GL_SGT4
-- 11/07/25  3.0.3   POTC           Homog�nisation du champ Statut avec les valeurs Oracle 
-- 23/07/25  4.0.0   GOICHON        Ajout de la proc�dure Customers_Australia_PROC
-- 20/08/25  4.0.1   BIHAN          Changement de la source du INTERCOMPANY_TYPE de la table source vers FILE_VAR_INTERCOMPANY_TYPE, et renseignement du PERIMETRE_LS_CONSO
-- 28/08/25  4.0.1   OJABIT         Correction de Changement de la source du INTERCOMPANY_TYPE de la table source vers FILE_VAR_INTERCOMPANY_TYPE, et renseignement du PERIMETRE_LS_CONSO

-- ***************************************************************************

/****************************************************************************************
    * PROCEDURE   :  Exceptions_PROC
    * DESCRIPTION :  Procedure g�n�rique pour les exceptions
    *                
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/

  PROCEDURE Exceptions_PROC
   IS
   BEGIN  
        g_erreur_pkg := 1 ; 

		 LH2_EXPLOIT_PKG.SET_GLOBAL_VAR_PROC(
		g_programme,
  		g_etape,
  		g_level,
		g_table,
		g_status,
		g_error_code,
		g_error_msg,
		g_date_deb,
		g_date_fin,
		g_rowcount 
		 );

		LH2_EXPLOIT_PKG.EXCEPTIONS_PROC;

	END Exceptions_PROC;

   /****************************************************************************************
    * PROCEDURE   :  Write_Log_PROC
    * DESCRIPTION :  Procedure g�n�rique pour la g�n�ration des Logs
    *                
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------*/

   PROCEDURE Write_Log_PROC 
   IS
   BEGIN  --D�but traitement

    -- g_status    :='COMPLETED';
    g_date_fin  :=sysdate;
    g_rowcount := SQL%ROWCOUNT;

     LH2_EXPLOIT_PKG.SET_GLOBAL_VAR_PROC(
		g_programme,
  		g_etape,
  		g_level,
		g_table,
		g_status,
		g_error_code,
		g_error_msg,
		g_date_deb,
		g_date_fin,
		g_rowcount 
		 );

		LH2_EXPLOIT_PKG.WRITE_LOG_PROC;

	--	g_date_deb  :=sysdate;

   END Write_Log_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_China_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER from CHINA data
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_China_PROC
   IS   v_procedure varchar2(100) := 'Customers_China_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.Customers_Not_Oracle_Silver_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    
    /* CHINA Customers */
    -- g_table     :='CUSTOMER_CHINA_TEMP';
    --  g_etape := '101';
     g_table     := 'CUSTOMER_CHINA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_CHINA_TEMP'  ;
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

    -- g_etape := '102';
     g_table     := 'CUSTOMER_CHINA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        INSERT INTO CUSTOMER_CHINA_TEMP
        SELECT DISTINCT
            CDZVBCBF.CUSTOMERACCOUNTNUMBERBILLTO CUSTOMER_ACCOUNT_NUMBER,
            CDZVBCBF.CUSTOMERCOUNTRY COUNTRY_CODE,
            fcz.COUNTRY_NAME_EN,
            fcz.ZONE_EPG,
            fcz.ZONE_MDE,
            fcz.ZONE_1_CIMD,
            fcz.ZONE_2_CIMD,
            fcz.ZONE_3_CIMD,
            CDZVBCBF.CUSTOMERTOWNBILLTO TOWN,
            CDZVBCBF.CUSTOMERNAME PARTY_NAME,
            CDZVBCBF.SALESAGENT SALES_AGENT_CODE,
            CDZVBCBF.INDUSTRYDETAIL INDUSTRY_DETAIL,
            CDZVBCBF.EIACUSTOMERTYPEBILLTO EIA_CUSTOMER_TYPE,
            CDZVBCBF.TAXREFERENCEBILLTO TAX_REFERENCE,
--            CDZVBCBF.INTERCOMPANYTYPE INTERCOMPANY_TYPE,
            fvit.INTERCOMPANY_TYPE INTERCOMPANY_TYPE,
            case when CDZVBCBF.CUSTOMERSTATUS = 'Active' then 'A' 
            else case when CDZVBCBF.CUSTOMERSTATUS = 'Inactive' then 'I'   
            else CDZVBCBF.CUSTOMERSTATUS
            end end STATUS,
            FCGNO.GROUP_CODE CUSTOMER_GROUP_CODE,
            fgn.GROUP_NAME CUSTOMER_GROUP_NAME,
            fvit.PERIMETRE_LS_CONSO PERIMETRE_LS_CONSO, --DECODE(CDZVBCBF.INTERCOMPANYTYPE,'EXT','Y','NIDEC','Y','N') PERIMETRE_LS_CONSO,
            DECODE(FCGNO.GROUP_CODE,NULL,CDZVBCBF.CUSTOMERNAME,fgn.GROUP_NAME) PARENT_COMPANY,
            1696 ORG_ID,
            CDZVBCBF.CUSTOMERACCOUNTNUMBERBILLTO SITE_USE_ID,
            'LS CN OU FUZ' OU_NAME ,
            'CHINA' SOURCE, 
            '4050' CUSTOMER_ACCOUNT_BU_GL_SGT1,
            '220' CUSTOMER_ACCOUNT_LOCATION_GL_SGT2,
            '0000' CUSTOMER_ACCOUNT_DEPARTMENT_GL_SGT3,
          --  DECODE(CDZVBCBF.INTERCOMPANYTYPE,'EXT','12110100','NIDEC','12901100','12110100') CUSTOMER_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','12110100',null,'12110100','12901100') RECEIVABLE_NATURALACCOUNT_GL_SGT4,
            '000' CUSTOMER_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000') CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,
            101  CHART_OF_ACCOUNTS_ID_REC,
            101  CHART_OF_ACCOUNTS_ID_REV,
            DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','41103100',null,'41103100','41301100') REVENUE_NATURALACCOUNT_GL_SGT4,
            fcz.CURRENCY COUNTRY_CURRENCY
        FROM 
            CHINA_DBO_ZZ_V_BO_CUSTOMERBASE_FZU_BZ cdzvbcbf,
            FILE_CUSTOMER_GROUP_NOT_ORACLE_BZ fcgno,
            FILE_GROUP_NAME_BZ fgn,
            COUNTRY_ZONE fcz,
            FILE_INTERCO_SEGMENT6_bz fvit
        WHERE 1=1
        AND  OU_NAME (+) = 'LS CN OU FUZ' 
        AND  FCGNO.GROUP_CODE = fgn.GROUP_CODE (+)
        AND CDZVBCBF.CUSTOMERACCOUNTNUMBERBILLTO = FCGNO.ACCOUNT_NUMBER (+)
        and CDZVBCBF.CUSTOMERCOUNTRY = fcz.country_code(+)
        AND nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000') = fvit.INTERCOMPANY_GL_CODE (+)
        ORDER by CUSTOMER_ACCOUNT_NUMBER
        ;
    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
	Write_Log_PROC;

    -- g_etape := '100';
     g_table     := 'CUSTOMER_CHINA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_CHINA_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_China_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_India_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER from INDIA data
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_India_PROC
   IS   v_procedure varchar2(100) := 'Customers_India_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.Customers_Not_Oracle_Silver_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    
    /* INDIA Customers */
    -- g_table     :='CUSTOMER_INDIA_TEMP';

    -- g_etape := '104';
     g_table     := 'CUSTOMER_INDIA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_INDIA_TEMP';
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

    -- g_etape := '105';
     g_table     := 'CUSTOMER_INDIA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        insert into CUSTOMER_INDIA_TEMP 
        SELECT DISTINCT
            IDVCB.CUSTOMER_ACCOUNT_NUMBER CUSTOMER_ACCOUNT_NUMBER,
            IDVCB.CUSTOMER_COUNTRY COUNTRY_CODE,
            fcz.COUNTRY_NAME_EN,
            fcz.ZONE_EPG,
            fcz.ZONE_MDE,
            fcz.ZONE_1_CIMD,
            fcz.ZONE_2_CIMD,
            fcz.ZONE_3_CIMD,
            IDVCB.CUSTOMER_TOWN TOWN,
            IDVCB.CUSTOMER_NAME PARTY_NAME,
            IDVCB.SALES_AGENT SALES_AGENT_CODE,
            IDVCB.CUSTOMER_INDUSTRY_VERTICAL_DESCRIPTION INDUSTRY_DETAIL,
            IDVCB.CUSTOMER_CATEGORY EIA_CUSTOMER_TYPE,
            NULL TAX_REFERENCE,
--            IDVCB.INTERCOMPANY_TYPE INTERCOMPANY_TYPE,
            fvit.INTERCOMPANY_TYPE INTERCOMPANY_TYPE,
            NULL STATUS,
            FCGNO.GROUP_CODE CUSTOMER_GROUP_CODE,
            fgn.GROUP_NAME CUSTOMER_GROUP_NAME,
            fvit.PERIMETRE_LS_CONSO PERIMETRE_LS_CONSO, --DECODE(IDVCB.INTERCOMPANY_TYPE,'EXT','Y','NIDEC','Y','N') PERIMETRE_LS_CONSO,
            DECODE(FCGNO.GROUP_CODE,NULL,IDVCB.CUSTOMER_NAME,fgn.GROUP_NAME) PARENT_COMPANY,
            1697 ORG_ID,
            IDVCB.CUSTOMER_ACCOUNT_NUMBER SITE_USE_ID,
            'LS IN OU TRD' OU_NAME ,
            'INDIA' SOURCE,
            '4052' CUSTOMER_ACCOUNT_BU_GL_SGT1,
            '226' CUSTOMER_ACCOUNT_LOCATION_GL_SGT2,
            '0000' CUSTOMER_ACCOUNT_DEPARTMENT_GL_SGT3,
           -- DECODE(IDVCB.INTERCOMPANY_TYPE,'EXT','12110100','NIDEC','12901100','12110100') CUSTOMER_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','12110100',null,'12110100','12901100') RECEIVABLE_NATURALACCOUNT_GL_SGT4,
            '000' CUSTOMER_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000')  CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,
            101  CHART_OF_ACCOUNTS_ID_REC,
            101  CHART_OF_ACCOUNTS_ID_REV,
            DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','41103100',null,'41103100','41301100') REVENUE_NATURALACCOUNT_GL_SGT4,
            fcz.CURRENCY COUNTRY_CURRENCY
        FROM 
            INDIA_DBO_VBOCUSTOMERBASE_BZ idvcb,
            FILE_CUSTOMER_GROUP_NOT_ORACLE_BZ fcgno,
            FILE_GROUP_NAME_BZ fgn,
            COUNTRY_ZONE fcz,
            --FILE_VAR_INTERCOMPANY_TYPE_bz fvit
            FILE_INTERCO_SEGMENT6_bz fvit
        WHERE 1=1
        AND  FCGNO.OU_NAME (+) = 'LS IN OU TRD'
        AND  FCGNO.GROUP_CODE = fgn.GROUP_CODE (+)
        AND IDVCB.CUSTOMER_ACCOUNT_NUMBER = FCGNO.ACCOUNT_NUMBER (+)
        and IDVCB.CUSTOMER_COUNTRY = fcz.country_code (+)
        AND nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000') = fvit.INTERCOMPANY_GL_CODE (+)
        ORDER by CUSTOMER_ACCOUNT_NUMBER
        ;
    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
    Write_Log_PROC;

    -- g_etape := '100';
    g_table     := 'CUSTOMER_INDIA_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_INDIA_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_India_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_Korea_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER from KOREA data
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_Korea_PROC
   IS   v_procedure varchar2(100) := 'Customers_Korea_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.Customers_Not_Oracle_Silver_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;

    /* KOREA Customers */
    -- g_table     :='CUSTOMER_KOREA_TEMP';

    -- g_etape := '107';
     g_table     := 'CUSTOMER_KOREA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_KOREA_TEMP';
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

    -- g_etape := '108';
     g_table     := 'CUSTOMER_KOREA_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        insert into CUSTOMER_KOREA_TEMP 
            WITH KOREA_DBO_OEORDH_BZ_ranked AS (
                SELECT KDOH.*,
                       ROW_NUMBER() OVER (PARTITION BY TRIM(CUSTOMER) ORDER BY ORDUNIQ DESC) AS rn
                FROM KOREA_DBO_OEORDH_BZ KDOH
                )
            SELECT DISTINCT
                TRIM(KDOHR.CUSTOMER) CUSTOMER_ACCOUNT_NUMBER,
                TRIM(KDOHR.BILCOUNTRY) COUNTRY_CODE,
                fcz.COUNTRY_NAME_EN,
                fcz.ZONE_EPG,
                fcz.ZONE_MDE,
                fcz.ZONE_1_CIMD,
                fcz.ZONE_2_CIMD,
                fcz.ZONE_3_CIMD,
                TRIM(KDOHR.BILCITY) TOWN,
                TRIM(KDOHR.BILNAME) PARTY_NAME,
                NULL SALES_AGENT_CODE,
                NULL INDUSTRY_DETAIL,
                NULL EIA_CUSTOMER_TYPE,
                NULL TAX_REFERENCE,
--                null INTERCOMPANY_TYPE,
                fvit.INTERCOMPANY_TYPE INTERCOMPANY_TYPE,
                NULL STATUS,
                FCGNO.GROUP_CODE CUSTOMER_GROUP_CODE,
                fgn.GROUP_NAME CUSTOMER_GROUP_NAME,
                fvit.PERIMETRE_LS_CONSO PERIMETRE_LS_CONSO,
                DECODE(FCGNO.GROUP_CODE,NULL,TRIM(KDOHR.BILNAME),fgn.GROUP_NAME) PARENT_COMPANY,
                99990 ORG_ID,
                TRIM(KDOHR.CUSTOMER) SITE_USE_ID,
                'LS KOREA' OU_NAME ,
                'KOREA' SOURCE,
                'KOREA' CUSTOMER_ACCOUNT_BU_GL_SGT1,
                'KOREA' CUSTOMER_ACCOUNT_LOCATION_GL_SGT2,
                '0000'  CUSTOMER_ACCOUNT_DEPARTMENT_GL_SGT3,
                -- '12110100' CUSTOMER_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','12110100',null,'12110100','12901100') RECEIVABLE_NATURALACCOUNT_GL_SGT4,
                '000'  CUSTOMER_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000')  CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,
                101  CHART_OF_ACCOUNTS_ID_REC,
                101  CHART_OF_ACCOUNTS_ID_REV,
                DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','41103100',null,'41103100','41301100') REVENUE_NATURALACCOUNT_GL_SGT4,
                fcz.CURRENCY COUNTRY_CURRENCY
            FROM 
                KOREA_DBO_OEORDH_BZ_ranked kdohr,
                FILE_CUSTOMER_GROUP_NOT_ORACLE_BZ fcgno,
                FILE_GROUP_NAME_BZ fgn,
                COUNTRY_ZONE fcz,
                --FILE_VAR_INTERCOMPANY_TYPE_bz fvit
                FILE_INTERCO_SEGMENT6_bz fvit
            WHERE 1=1
          --  and TRIM(KDOHR.BILCOUNTRY)=fcz.country_code (+)
            and LH2_DTH_SILVER_FUNCTIONS_PKG.get_country_code_func(KDOHR.BILCOUNTRY) = fcz.country_code (+)
            AND FCGNO.OU_NAME = 'LS KOREA'
            AND FCGNO.GROUP_CODE = fgn.GROUP_CODE (+)
            AND TRIM(KDOHR.CUSTOMER) = FCGNO.ACCOUNT_NUMBER (+)
            AND rn = 1
            and nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000') =  fvit.INTERCOMPANY_GL_CODE (+)
            ORDER by CUSTOMER_ACCOUNT_NUMBER
            ;
    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
    Write_Log_PROC;

    -- g_etape := '100';
    g_table     := 'CUSTOMER_KOREA_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
    LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_KOREA_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_Korea_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_Singapore_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER from SINGAPORE data
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_Singapore_PROC
   IS   v_procedure varchar2(100) := 'Customers_Singapore_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.Customers_Not_Oracle_Silver_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    
     /* SINGAPORE Customers */
    -- g_table     :='CUSTOMER_SINGAPORE_TEMP';

    -- g_etape := '110';
    g_table     := 'CUSTOMER_SINGAPORE_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_SINGAPORE_TEMP';
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

    -- g_etape := '111';
    g_table     := 'CUSTOMER_SINGAPORE_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        INSERT INTO CUSTOMER_SINGAPORE_TEMP
        WITH SINGAPORE_DBO_OEORDH_BZ_ranked AS (
            SELECT SDOH.*,
                   ROW_NUMBER() OVER (PARTITION BY TRIM(CUSTOMER) ORDER BY ORDUNIQ DESC) AS rn
            FROM SINGAPORE_DBO_OEORDH_BZ SDOH
            )
        SELECT DISTINCT
            TRIM(SDOHR.CUSTOMER) CUSTOMER_ACCOUNT_NUMBER,
            TRIM(SDOHR.BILCOUNTRY) COUNTRY_CODE,
            fcz.COUNTRY_NAME_EN,
            fcz.ZONE_EPG,
            fcz.ZONE_MDE,
            fcz.ZONE_1_CIMD,
            fcz.ZONE_2_CIMD,
            fcz.ZONE_3_CIMD,
            TRIM(SDOHR.BILCITY) TOWN,
            TRIM(SDOHR.BILNAME) PARTY_NAME,
            NULL  SALES_AGENT_CODE,
            NULL INDUSTRY_DETAIL,
            NULL EIA_CUSTOMER_TYPE,
            NULL TAX_REFERENCE,
--            null  INTERCOMPANY_TYPE,
            fvit.INTERCOMPANY_TYPE  INTERCOMPANY_TYPE,
            NULL STATUS,
            FCGNO.GROUP_CODE CUSTOMER_GROUP_CODE,
            fgn.GROUP_NAME CUSTOMER_GROUP_NAME,
            fvit.PERIMETRE_LS_CONSO PERIMETRE_LS_CONSO,
            DECODE(FCGNO.GROUP_CODE,NULL,TRIM(SDOHR.BILNAME),fgn.GROUP_NAME) PARENT_COMPANY,
            99991 ORG_ID,
            TRIM(SDOHR.CUSTOMER) SITE_USE_ID,
            'LS SINGAPORE' OU_NAME ,
            'SINGAPORE' SOURCE,
            'SINGAPORE' CUSTOMER_ACCOUNT_BU_GL_SGT1,
            'SINGAPORE' CUSTOMER_ACCOUNT_LOCATION_GL_SGT2,
            '0000' CUSTOMER_ACCOUNT_DEPARTMENT_GL_SGT3,
           -- '12110100' CUSTOMER_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','12110100',null,'12110100','12901100') RECEIVABLE_NATURALACCOUNT_GL_SGT4,
            '000' CUSTOMER_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000')  CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,
            101  CHART_OF_ACCOUNTS_ID_REC,
            101  CHART_OF_ACCOUNTS_ID_REV,
            DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','41103100',null,'41103100','41301100') REVENUE_NATURALACCOUNT_GL_SGT4,
            fcz.CURRENCY COUNTRY_CURRENCY
        FROM 
            SINGAPORE_DBO_OEORDH_BZ_ranked sdohr,
            FILE_CUSTOMER_GROUP_NOT_ORACLE_BZ fcgno,
            FILE_GROUP_NAME_BZ fgn,
            COUNTRY_ZONE fcz,
            --FILE_VAR_INTERCOMPANY_TYPE_bz fvit
            FILE_INTERCO_SEGMENT6_bz fvit
        WHERE 1=1
        AND FCGNO.OU_NAME (+) = 'LS SINGAPORE'
        AND  FCGNO.GROUP_CODE = fgn.GROUP_CODE (+)
        AND TRIM(SDOHR.CUSTOMER) = FCGNO.ACCOUNT_NUMBER (+)
        AND rn = 1
        AND nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000') = fvit.INTERCOMPANY_GL_CODE (+)
       -- and TRIM(SDOHR.BILCOUNTRY)=fcz.country_code (+)
        and LH2_DTH_SILVER_FUNCTIONS_PKG.get_country_code_func(SDOHR.BILCOUNTRY) = fcz.country_code (+)
        ORDER BY CUSTOMER_ACCOUNT_NUMBER
        ;
    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
    Write_Log_PROC;

    -- g_etape := '100';
    g_table     := 'CUSTOMER_SINGAPORE_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_SINGAPORE_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_Singapore_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_Brazil_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER from BRAZIL data
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_Brazil_PROC
   IS   v_procedure varchar2(100) := 'Customers_Brazil_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.Customers_Not_Oracle_Silver_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    
    /* BRAZIL Customers - creation as copy of oracle client Schindler Brazil */
    -- g_table     :='CUSTOMER_BRAZIL_TEMP';

    -- g_etape := '114';
    g_table     := 'CUSTOMER_BRAZIL_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_BRAZIL_TEMP';
    g_status   := 'COMPLETED';
    g_etape    := '011 - TRUNCATE TABLE' ;
    Write_Log_PROC;

    -- g_etape := '115';
    g_table     := 'CUSTOMER_BRAZIL_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        INSERT INTO CUSTOMER_BRAZIL_TEMP
        select  
            '7411' CUSTOMER_ACCOUNT_NUMBER,
            COUNTRY_CODE,
            COUNTRY_NAME_EN,
            ZONE_EPG,
            ZONE_MDE,
            ZONE_1_CIMD,
            ZONE_2_CIMD,
            ZONE_3_CIMD,
            TOWN,
            PARTY_NAME,
            SALES_AGENT_CODE,
            INDUSTRY_DETAIL,
            EIA_CUSTOMER_TYPE,
            TAX_REFERENCE,
            INTERCOMPANY_TYPE,
            STATUS,
            CUSTOMER_GROUP_CODE,
            CUSTOMER_GROUP_NAME,
            PERIMETRE_LS_CONSO,
            PARENT_COMPANY,
            1695 ORG_ID,
            '7411' SITE_USE_ID,
            'LS BR OU BRZ' OU_NAME,
            'BRAZIL' SOURCE,
            '4061' CUSTOMER_ACCOUNT_BU_GL_SGT1,
            '225' CUSTOMER_ACCOUNT_LOCATION_GL_SGT2,
            '0000' CUSTOMER_ACCOUNT_DEPARTMENT_GL_SGT3,
            '12110100' RECEIVABLE_NATURALACCOUNT_GL_SGT4,
            '000' CUSTOMER_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            '0000' CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,
            101  CHART_OF_ACCOUNTS_ID_REC,
            101  CHART_OF_ACCOUNTS_ID_REV,
            '41103100' REVENUE_NATURALACCOUNT_GL_SGT4,
            'BRL' COUNTRY_CURRENCY
        from customer_oracle_bill_to 
        where site_use_id = '40360'
        ; 
    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
    Write_Log_PROC;

    -- g_etape := '100';
    g_table     := 'CUSTOMER_BRAZIL_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'   ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_BRAZIL_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_Brazil_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_Australia_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER from BRAZIL data
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_Australia_PROC
   IS   v_procedure varchar2(100) := 'Customers_Australia_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    

    g_table     := 'CUSTOMER_AUSTRALIA_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_AUSTRALIA_TEMP';
    g_status   := 'COMPLETED';
    g_etape    := '011 - TRUNCATE TABLE' ;
    Write_Log_PROC;

    g_table     := 'CUSTOMER_AUSTRALIA_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        INSERT INTO CUSTOMER_AUSTRALIA_TEMP
        WITH AUSTRALIA_DBO_OEORDH_BZ_ranked AS (
                SELECT ADOH.*,
                       ROW_NUMBER() OVER (PARTITION BY TRIM(CUSTOMER) ORDER BY ORDUNIQ DESC) AS rn
                FROM AUSTRALIA_DBO_OEORDH_BZ ADOH
                )
            SELECT DISTINCT
                TRIM(adohr.CUSTOMER) CUSTOMER_ACCOUNT_NUMBER,
                TRIM(adohr.BILCOUNTRY) COUNTRY_CODE,
                fcz.COUNTRY_NAME_EN,
                fcz.ZONE_EPG,
                fcz.ZONE_MDE,
                fcz.ZONE_1_CIMD,
                fcz.ZONE_2_CIMD,
                fcz.ZONE_3_CIMD,
                TRIM(adohr.BILCITY) TOWN,
                TRIM(adohr.BILNAME) PARTY_NAME,
                CAST(NULL AS VARCHAR2(4000 BYTE)) SALES_AGENT_CODE,
                CAST(NULL AS VARCHAR2(4000 BYTE)) INDUSTRY_DETAIL,
                CAST(NULL AS VARCHAR2(4000 BYTE)) EIA_CUSTOMER_TYPE,
                CAST(NULL AS VARCHAR2(4000 BYTE)) TAX_REFERENCE,
--                CAST(null AS VARCHAR2(4000 BYTE)) INTERCOMPANY_TYPE,
                fvit.INTERCOMPANY_TYPE INTERCOMPANY_TYPE,
                CAST(NULL AS VARCHAR2(4000 BYTE)) STATUS,
                FCGNO.GROUP_CODE CUSTOMER_GROUP_CODE,
                fgn.GROUP_NAME CUSTOMER_GROUP_NAME,
--                CAST(null AS VARCHAR2(1 BYTE)) PERIMETRE_LS_CONSO,
                fvit.PERIMETRE_LS_CONSO  PERIMETRE_LS_CONSO,
                DECODE(FCGNO.GROUP_CODE,NULL,TRIM(adohr.BILNAME),fgn.GROUP_NAME) PARENT_COMPANY,
                99994 ORG_ID,
                TRIM(adohr.CUSTOMER) SITE_USE_ID,
                'LS AUSTRALIA' OU_NAME ,
                'AUSTRALIA' SOURCE,
                'AUSTRALIA' CUSTOMER_ACCOUNT_BU_GL_SGT1,
                'AUSTRALIA' CUSTOMER_ACCOUNT_LOCATION_GL_SGT2,
                '0000'  CUSTOMER_ACCOUNT_DEPARTMENT_GL_SGT3,
                DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','12110100',null,'12110100','12901100') RECEIVABLE_NATURALACCOUNT_GL_SGT4,
                '000'  CUSTOMER_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000')  CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,
                101  CHART_OF_ACCOUNTS_ID_REC,
                101  CHART_OF_ACCOUNTS_ID_REV,
                DECODE(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000','41103100',null,'41103100','41301100') REVENUE_NATURALACCOUNT_GL_SGT4,
            fcz.CURRENCY COUNTRY_CURRENCY
            FROM 
                AUSTRALIA_DBO_OEORDH_BZ_ranked adohr,
                FILE_CUSTOMER_GROUP_NOT_ORACLE_BZ fcgno,
                FILE_GROUP_NAME_BZ fgn,
                COUNTRY_ZONE fcz,
                --FILE_VAR_INTERCOMPANY_TYPE_bz fvit
                FILE_INTERCO_SEGMENT6_bz fvit
            WHERE 1=1
            and LH2_DTH_SILVER_FUNCTIONS_PKG.get_country_code_func(adohr.BILCOUNTRY) = fcz.country_code (+)
            AND FCGNO.OU_NAME (+) = 'LS AUSTRALIA'
            AND FCGNO.GROUP_CODE = fgn.GROUP_CODE (+)
            AND TRIM(adohr.CUSTOMER) = FCGNO.ACCOUNT_NUMBER (+)
            AND rn = 1
            AND  nvl(fcgno.CUSTOMER_ACCOUNT_INTERCOMPANY_GL_SGT6,'0000') = fvit.INTERCOMPANY_GL_CODE (+)
            ORDER by CUSTOMER_ACCOUNT_NUMBER
            ;

    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
    Write_Log_PROC;

    -- g_etape := '100';
    g_table     := 'CUSTOMER_AUSTRALIA_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'   ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_AUSTRALIA_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_Australia_PROC;

  /****************************************************************************************
    * PROCEDURE   :  Customers_Asia_Silver_PROC
    * DESCRIPTION :  Procedure cr�ation tables CUSTOMER for NOT_ORACLE
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
  PROCEDURE Customers_Not_Oracle_Silver_PROC
   IS   v_procedure varchar2(100) := 'Customers_Not_Oracle_Silver_PROC';
        v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.Customers_Not_Oracle_Silver_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    
     Customers_China_PROC;
     Customers_India_PROC;
     Customers_Korea_PROC;
     Customers_Singapore_PROC;
     Customers_Brazil_PROC;
    Customers_Australia_PROC;


    g_table     := 'CUSTOMER_NOT_ORACLE_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_NOT_ORACLE_TEMP';
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

    -- g_etape := '117';
    g_table     := 'CUSTOMER_NOT_ORACLE_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        insert into CUSTOMER_NOT_ORACLE_TEMP
        select * from CUSTOMER_CHINA_TEMP
        union
        select * from CUSTOMER_INDIA_TEMP
        union
        select * from CUSTOMER_KOREA_TEMP
        union
        select * from CUSTOMER_SINGAPORE_TEMP
        union
        select * from CUSTOMER_BRAZIL_TEMP
        union 
        select * from Customer_Australia_TEMP
        ;
    -- g_etape := '90';
    g_status   := 'COMPLETED';
    g_etape    := '010 - INSERT INTO' ;
    Write_Log_PROC;

    -- g_etape := '100';
    g_table     := 'CUSTOMER_NOT_ORACLE_TEMP'; 
    g_date_deb  := sysdate;
    g_status    := 'WIP';
    g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_NOT_ORACLE_TEMP');
    g_status   := 'COMPLETED';
    g_etape    := '099 - STATS' ; 
    Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
     Write_Log_PROC;

     g_table    := v_procedure;
     g_date_deb := v_date_deb_proc; 
     g_status   := 'END SUCCESS';
     g_etape    := '9992 - End PROC';	
     Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
        Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';	
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc; 
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';	
         Write_Log_PROC;

   END Customers_Not_Oracle_Silver_PROC;

  /****************************************************************************************
    * PROCEDURE   :  MAIN
    * DESCRIPTION :  Procedure principale
    *                
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/

	PROCEDURE MAIN (pv_errbuf   OUT VARCHAR2
                  ,pn_retcode  OUT NUMBER
				  )
   IS
     --variables
	 v_procedure           varchar2(100)   := 'MAIN';
	 v_status              varchar2(1) := 'A';  --statut Accept� (sinon R pour Rejet�)
	 v_message             varchar2(1000) := NULL;
     v_date_deb_pkg  TIMESTAMP := sysdate;

   BEGIN  --D�but traitement
	 DBMS_OUTPUT.ENABLE (1000000);

    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG.MAIN';
     g_level := 'S';
	 g_programme := $$plsql_unit || '.' || v_procedure ;
     g_table     := $$plsql_unit;
     g_date_deb  := v_date_deb_pkg;
     g_status   := 'BEGIN';
     g_etape    := '0001 - Begin PKG';
     Write_Log_PROC;

    -- g_etape := '1';
	 DBMS_OUTPUT.PUT_LINE (g_programme);
	 DBMS_OUTPUT.PUT_LINE ('-----------------------------------------------------------------------');
	 DBMS_OUTPUT.PUT_LINE ('---------------------START----------------------------');

	 v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Starting Customers_Not_Oracle_Silver_PROC');
    -- g_etape := '10';
     Customers_Not_Oracle_Silver_PROC;

	-- g_etape := '1000';
     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	DBMS_OUTPUT.PUT_LINE ('-----------------------------END--------------------------------------');--fin compte-rendu

        if   g_erreur_pkg = 1 then
             pn_retcode := 1;
             g_programme := $$plsql_unit || '.' || v_procedure ;
             g_table     := $$plsql_unit;	
             g_date_deb  := v_date_deb_pkg;
             g_status   := 'END FAILED';
             g_etape    := '9991 - End PKG';
             Write_Log_PROC;
        else 
             g_programme := $$plsql_unit || '.' || v_procedure ;
             g_table     := $$plsql_unit;	
             g_date_deb  := v_date_deb_pkg;
             g_status   := 'END SUCCESS';
             g_etape    := '9991 - End PKG';
             Write_Log_PROC;
        end if;

   EXCEPTION
      WHEN OTHERS
      THEN
         pn_retcode := 1;
         pv_errbuf := SQLCODE || '-' || SQLERRM;
         Exceptions_PROC;

        g_programme := $$plsql_unit || '.' || v_procedure ;
        g_table     := $$plsql_unit;	
        g_date_deb  := v_date_deb_pkg;
        g_status   := 'END FAILED';
        g_etape    := '9991 - End PKG';
        Write_Log_PROC;

   END MAIN;

END LH2_DTH_SILVER_CUSTOMER_NOT_ORACLE_PKG;