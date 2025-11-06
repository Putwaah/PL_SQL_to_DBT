create or replace PACKAGE BODY LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG IS

/*     $Header: LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG.sql 1.0.0 2024/06/11 09:00:00 vsre noship $ */
-- ***************************************************************************
-- @(#) ----------------------------------------------------------------------
-- @(#) Specifique: DTH005 IMPORT des donn�es dans le Silver - DataHub Custome Oracle
-- @(#) Fichier   : .LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG.sql
-- @(#) Version   : 1.0.0 du 11/06/2024
-- @(#) ---------------------------------------------e-------------------------
-- Objet          : Package Body du DTH005 IMPORT des donn�es Silver
-- Commentaires   :
-- Exemple        :
-- ***************************************************************************
--                            HISTORIQUE DES VERSIONS
-- ---------------------------------------------------------------------------
-- Date     Version  Nom            Description de l'intervention
-- -------- -------- -------------- ------------------------------------------
-- 11/06/24  1.0.0   JABIT/AUFFERT  Version initiale
-- 12/09/24  1.0.1   POTC           R�organisation de la gestion des logs
-- 05/11/24  2.0.0   POTC           Changement de la gestion des logs 
-- 23/12/24  2.0.1   SMILOSAVLJEVIC  Added column EIA_SALESPERSON
-- 25/02/25  2.0.2   POTC           Remplacement du chargement de champ par le revenue account car mieux renseign� que le receivable account
-- 03/07/25  2.0.3   OJABIT         Ajout du champ orig_system_reference
-- 08/07/25  2.0.4   OJABIT         Ajout des champs PRIMARY_FLAG,STATUS_ACCOUNT + "N" par d�faut sur si vide(checking_hold , credit_hold)
-- 11/09/25  2.0.5   GOICHON        Remplacement de la jointure avec FILE_VAR_INTERCOMPANY_TYPE_BZ par FILE_INTERCO_SEGMENT6_BZ
-- 26/09/25  2.0.6   OJABIT         Remplacement des jointures FND_FLEX_VALUES par la table FND_FLEX_VALUES_US

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

  --  g_status    :='COMPLETED';
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
    * PROCEDURE   :  STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP_PROC
    * DESC:Table temporaire pour cr�er la table CUSTOMER_BILL_TO               
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------*/
  PROCEDURE Steph_HZ_CUST_SITE_USES_ALL_bill_to_temp_PROC
   IS v_procedure varchar2(100) := 'Steph_HZ_CUST_SITE_USES_ALL_bill_to_temp_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;

   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;      
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG.Steph_HZ_CUST_SITE_USES_ALL_bill_to_temp_PROC';
    -- g_level     := 'S';
    -- g_date_deb  := SYSDATE;
    -- g_status    := NULL;
     g_error_code:= NULL;
     g_error_msg := NULL;
     g_date_fin  := NULL;
     g_rowcount  := 0;
    -- g_table := 'STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP';
    
     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;

    -- g_etape := '101';
     g_table     := 'STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
         EXECUTE IMMEDIATE 'DROP TABLE STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP';
     g_status   := 'COMPLETED';
     g_etape    := '002 - DROP TABLE' ;
     Write_Log_PROC;

   --  g_etape := '102';
     g_table     := 'STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
         INSERT INTO TABLE STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP AS
                            SELECT *
                            FROM STEPH_APPS_HZ_CUST_SITE_USES_ALL_BZ
                            WHERE SITE_USE_CODE = "BILL_TO";

    --  g_etape := '90';
     g_status   := 'COMPLETED';
     g_etape    := '001 - CREATE TABLE' ;
     Write_Log_PROC;  

   --  g_etape := '100';
     g_table     := 'STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP');
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

   END Steph_HZ_CUST_SITE_USES_ALL_bill_to_temp_PROC;

    /****************************************************************************************
    * PROCEDURE   :  Customer_Oracle_bill_to_PROC
    *                
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------*/
  
    PROCEDURE Customer_Oracle_bill_to_PROC
   IS v_procedure varchar2(100) := 'Customer_Oracle_bill_to_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;

   BEGIN

     g_programme := $$plsql_unit || '.' || v_procedure ;    
    -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG.Customer_Oracle_bill_to_PROC';
    -- g_level     :='S';
    -- g_date_deb  :=sysdate;
    -- g_status    :=NULL;
     g_error_code:=NULL;
     g_error_msg :=NULL;
     g_date_fin  :=NULL;
     g_rowcount  :=0;
   -- g_table     :='CUSTOMER_ORACLE_BILL_TO';

     g_table     := v_procedure;
     g_date_deb  := v_date_deb_proc;
     g_status    := 'BEGIN';
     g_etape     := '0002 - Begin PROC';
     Write_Log_PROC;
    
    /* CUSTOMER_ORACLE_BILL_TO */
   -- g_etape := '201';
     g_table     := 'CUSTOMER_ORACLE_BILL_TO'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ; 
    	EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER_ORACLE_BILL_TO'  ;
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

   -- g_etape := '202';
     g_table     := 'CUSTOMER_ORACLE_BILL_TO'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        INSERT INTO 
        CUSTOMER_ORACLE_BILL_TO AS
       SELECT DISTINCT
                SAHCAB.ACCOUNT_NUMBER AS CUSTOMER_ACCOUNT_NUMBER ,
                SAHPB.PARTY_NAME AS PARTY_NAME,
                SAHCAB.ATTRIBUTE2 AS INDUSTRY_DETAIL,
                FVITB.INTERCOMPANY_TYPE AS INTERCOMPANY_TYPE,
                SRT.ORA_SALESREP_SALESREP_NUMBER AS SALES_AGENT_CODE,------------
                SACAB_FILTERED.CLASS_CODE AS EIA_CUSTOMER_TYPE,
                FCGB.GROUP_CODE AS CUSTOMER_GROUP_CODE,
                FGNB.GROUP_NAME AS CUSTOMER_GROUP_NAME,
                FVITB.PERIMETRE_LS_CONSO AS PERIMETRE_LS_CONSO,
                DECODE(FCGB.GROUP_CODE, NULL, SAHPB.PARTY_NAME, FGNB.GROUP_NAME) AS PARENT_COMPANY,
                SHCSUABTTB.ORG_ID AS ORG_ID,
                SHCSUABTTB.SITE_USE_ID AS SITE_USE_ID,
                SAHAOUB.NAME AS OU_NAME,
                'ORACLE' AS SOURCE,
                SHCSUABTTB.STATUS AS STATUS,
                SAHPB.TAX_REFERENCE AS TAX_REFERENCE,
                SHCSUABTTB.CUST_ACCT_SITE_ID AS CUST_ACCT_SITE_ID,
                SHCSUABTTB.FOB_POINT AS FOB_POINT,
                SHCSUABTTB.FREIGHT_TERM AS FREIGHT_TERM,
                SHCSUABTTB.ATTRIBUTE2 AS METAL_SUPPLEMENT,
                SHCSUABTTB.SITE_USE_CODE AS SITE_USE_CODE,
                SHCSUABTTB.BILL_TO_SITE_USE_ID AS BILL_TO_SITE_USE_ID,
                SHCSUABTTB.GL_ID_REC AS GL_ID_REC,
                SHCSUABTTB.GL_ID_REV AS GL_ID_REV,
                SHCSUABTTB.LAST_UPDATE_DATE AS LAST_UPDATE_DATE,
             /*   SAGCCKB.SEGMENT4 AS ACCOUNT_GL_CODE,   -- remplacement par le revenue account le 25/02/25 car mieux renseign� que le receivable account
                SAGCCKB.SEGMENT1 AS BU_GL_CODE,
                SAGCCKB.SEGMENT3 AS DPT_GL_CODE,
                SAGCCKB.SEGMENT6 AS INTERCOMPANY_GL_CODE,
                SAGCCKB.SEGMENT5 AS PRODUCT_GROUP_GL_CODE, */ 
                SAGCCKBV.SEGMENT4 AS ACCOUNT_GL_CODE,
                SAGCCKBV.SEGMENT1 AS BU_GL_CODE,
                SAGCCKBV.SEGMENT3 AS DPT_GL_CODE,
                SAGCCKBV.SEGMENT6 AS INTERCOMPANY_GL_CODE,
                SAGCCKBV.SEGMENT5 AS PRODUCT_GROUP_GL_CODE,
                SAGCCKB.CONCATENATED_SEGMENTS AS RECEIVABLE_ACCOUNT,
             --   SAGCCKB.CONCATENATED_SEGMENTS AS REVENUE_ACCOUNT,  
                SAGCCKBV.CONCATENATED_SEGMENTS AS REVENUE_ACCOUNT,
             --   SAGCCKB.SEGMENT2 AS SITE_GL_CODE,  -- remplacement par le revenue account le 25/02/25 car mieux renseign� que le receivable account
                SAGCCKBV.SEGMENT2 AS LOCATION_GL_CODE  ,   -- renommage le 25/02/25 avant : SITE_GL_CODE
                SAHASAB.PARTY_SITE_ID AS PARTY_SITE_ID,
                SAHASAB.CUST_ACCOUNT_ID AS CUST_ACCOUNT_ID,
                SAHPSB.ADDRESSEE AS ADDRESSEE,
                SAHPSB.LOCATION_ID AS LOCATION_ID,
                SAHPSB.PARTY_ID AS PARTY_ID,
                SAHPSB.PARTY_SITE_NUMBER AS PARTY_SITE_NUMBER,
                SAHPB.SIC_CODE AS SIC_CODE,
                ffvb.DESCRIPTION_ffv_tl AS INDUSTRY_DETAIL_DESCRIPTION,
                SAHCAB.CREATION_DATE AS CREATION_DATE,
                SAHCAB.CUSTOMER_TYPE AS CUSTOMER_TYPE,
                SAHCAB.ATTRIBUTE1 AS INDUSTRY_VERTICAL,
                ffv.DESCRIPTION_ffv_tl AS INDUSTRY_VERTICAL_DESCRIPTION,
                SAHCPB.COLLECTOR_ID AS COLLECTOR_ID,
                --SAHCPB.CREDIT_CHECKING AS CREDIT_CHECKING,
                --SAHCPB.CREDIT_HOLD AS CREDIT_HOLD,
                COALESCE(NULLIF(TRIM(SAHCPB.CREDIT_CHECKING), ''), 'N') AS CREDIT_CHECKING, --"N" par d�faut sur si vide
                COALESCE(NULLIF(TRIM(SAHCPB.CREDIT_HOLD), ''), 'N') AS CREDIT_HOLD, --"N" par d�faut sur si vide
                SRT.ORA_SALESREP_SALESREP_IDA AS SALESREP_ID,
                SAHLB.COUNTRY AS COUNTRY_CODE,
                SAHLB.CITY AS TOWN,
                SAHLB.POSTAL_CODE AS POSTAL_CODE,
                SAHLB.PROVINCE AS PROVINCE,
                SAHLB.STATE AS STATE,
                SAHLB.ADDRESS1 AS ADDRESS1,
                SAHLB.ADDRESS2 AS ADDRESS2,
                SAHLB.ADDRESS3 AS ADDRESS3,
                SAHLB.ADDRESS4 AS ADDRESS4,
                SAACB.NAME AS COLLECTOR_NAME,
                SRT.AS400_AGENT_LIBELLE AS SALESREP_NAME,--------------
                SRT.AS400_SUCCURSALE_LIBELLE AS SALESREP_BRANCH_NAME,--------------
                --SRT.WSUSUC AS SALESREP_BRANCH,-------------------- N'EXISTE PAS
                SRT.AS400_SUCCURSALE AS SALESREP_BRANCH,
                SRT.AS400_MARCHE AS SALESREP_MARKET,-----------
                SRT.AS400_REGION_LIBELLE AS SALESREP_AREA_NAME,----------
               -- SRT.WRGREG AS SALESREP_AREA,-------- N'EXISTE PAS
                SRT.AS400_REGION AS SALESREP_AREA,
                SRT.AS400_MARCHE_LIBELLE AS SALESREP_MARKET_NAME,------------
                SAHCCDB.CLASS_CODE_MEANING AS LIBELLE_EIA_CUSTOMER_TYPE,
                SAHLAB.LOCATION_CODE AS INTERNAL_LOCATION,
                SAMPB.ORGANIZATION_ID AS INTERNAL_ORGANIZATION,
                SARTB.NAME AS PAYMENT_TERM,
                SARTB.DESCRIPTION AS PAYMENT_TERM_DESCRIPTION,
                FCZB.COUNTRY_NAME_EN,
                FCZB.ZONE_EPG,
                FCZB.ZONE_MDE,
                FCZB.ZONE_1_CIMD,
                FCZB.ZONE_2_CIMD,
                FCZB.ZONE_3_CIMD,
                sagcckb.CHART_OF_ACCOUNTS_ID CHART_OF_ACCOUNTS_ID_REC,
                sagcckbv.CHART_OF_ACCOUNTS_ID CHART_OF_ACCOUNTS_ID_REV,
                SHCSUABTTB.ship_via,
                SHCSUABTTB.attribute4 DFF_FREIGHT, 
                SHCSUABTTB.attribute8 DFF_FREIGHT_CHARGE,
                SRT. ORA_RESOURCE_RESOURCE_NAME EIA_SALESPERSON_NAME,-----------
                FCGB.CUSTOMER_ACTIVITY,
                SAHASAB.orig_system_reference,
                SHCSUABTTB.PRIMARY_FLAG,
                SAHCAB.STATUS AS STATUS_ACCOUNT,
                fu2.user_name created_by_account,
                SAHCAB.CREATION_DATE CREATION_DATE_ACCOUNT,
                fu1.user_name LAST_UPDATED_BY_account,
                SAHCAB.LAST_UPDATE_DATE LAST_UPDATE_DATE_account,
                fu4.user_name CREATED_BY,
                fu3.user_name LAST_UPDATE_BY,
                fczb.currency country_currency
FROM DEV.LH2_SILVER_DEV.STEPH_HZ_CUST_SITE_USES_ALL_BILL_TO_TEMP SHCSUABTTB
JOIN STEPH_APPS_HR_ALL_ORGANIZATION_UNITS_BZ SAHAOUB
  ON SHCSUABTTB.ORG_ID = SAHAOUB.ORGANIZATION_ID

LEFT JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_BZ SAGCCKB
  ON SHCSUABTTB.GL_ID_REC = SAGCCKB.CODE_COMBINATION_ID
LEFT JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_BZ SAGCCKBV
  ON SHCSUABTTB.GL_ID_REV = SAGCCKBV.CODE_COMBINATION_ID

LEFT JOIN FILE_INTERCO_SEGMENT6_BZ FVITB
  ON SAGCCKBV.SEGMENT6 = FVITB.INTERCOMPANY_GL_CODE

LEFT JOIN STEPH_APPS_HZ_CUST_ACCT_SITES_ALL_BZ SAHASAB
  ON SHCSUABTTB.CUST_ACCT_SITE_ID = SAHASAB.CUST_ACCT_SITE_ID
LEFT JOIN STEPH_APPS_HZ_PARTY_SITES_BZ SAHPSB
  ON SAHASAB.PARTY_SITE_ID = SAHPSB.PARTY_SITE_ID

LEFT JOIN (
  SELECT a.*
  FROM DEV.LH2_BRONZE_DEV.STEPH_APPS_HZ_PARTIES a
  JOIN ( SELECT PARTY_ID
          FROM DEV.LH2_BRONZE_DEV.STEPH_APPS_HZ_PARTIES
         GROUP BY PARTY_ID ) b
    ON a.PARTY_ID = b.PARTY_ID
) SAHPB
  ON SAHPSB.PARTY_ID = SAHPB.PARTY_ID

LEFT JOIN STEPH_APPS_HZ_CUST_ACCOUNTS_BZ SAHCAB
  ON SAHASAB.CUST_ACCOUNT_ID = SAHCAB.CUST_ACCOUNT_ID

LEFT JOIN (
  SELECT DESCRIPTION_ffv_tl, FLEX_VALUE_ffv_values
  FROM DEV.LH2_SILVER_DEV.FND_FLEX_VALUES_US
  WHERE FLEX_VALUE_SET_NAME_ffv_set = 'XXAR_INDUSTRY_VERTICAL_EIA'
    AND PARENT_FLEX_VALUE_LOW_FFV_VALUES IS NULL
) ffv
  ON SAHCAB.ATTRIBUTE1 = ffv.FLEX_VALUE_ffv_values

LEFT JOIN (
  SELECT DESCRIPTION_ffv_tl, FLEX_VALUE_ffv_values, PARENT_FLEX_VALUE_LOW_FFV_VALUES
  FROM DEV.LH2_SILVER_DEV.FND_FLEX_VALUES_US
  WHERE FLEX_VALUE_SET_NAME_ffv_set = 'XXAR_INDUSTRY_DETAILS_EIA'
) ffvb
  ON SAHCAB.ATTRIBUTE2 = ffvb.FLEX_VALUE_ffv_values
 AND SAHCAB.ATTRIBUTE1 = ffvb.PARENT_FLEX_VALUE_LOW_FFV_VALUES

-- ...
JOIN COUNTRY_ZONE FCZB
  ON SAHLB.COUNTRY = FCZB.COUNTRY_CODE

LEFT JOIN STEPH_APPS_FND_USER_bz fu1
  ON SAHCAB.LAST_UPDATED_BY = fu1.USER_ID
LEFT JOIN STEPH_APPS_FND_USER_bz fu2
  ON SAHCAB.CREATED_BY = fu2.USER_ID
LEFT JOIN STEPH_APPS_FND_USER_bz fu3
  ON SHCSUABTTB.LAST_UPDATED_BY = fu3.USER_ID
LEFT JOIN STEPH_APPS_FND_USER_bz fu4
  ON SHCSUABTTB.CREATED_BY = fu4.USER_ID
        ;  
       -- g_etape := '90';
     g_status   := 'COMPLETED';
     g_etape    := '010 - INSERT INTO' ;
     Write_Log_PROC;
        
     --   g_etape := '100';
     g_table     := 'CUSTOMER_ORACLE_BILL_TO'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('CUSTOMER_ORACLE_BILL_TO'); 
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

   END Customer_Oracle_bill_to_PROC;
     
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
	 v_procedure            varchar2(100)   := 'MAIN';
	 v_status               varchar2(1)     := 'A';  --statut Accept� (sinon R pour Rejet�)
	 v_message              varchar2(1000)  := NULL;
     v_date_deb_pkg         TIMESTAMP       := sysdate;

   BEGIN  --D�but traitement
	 DBMS_OUTPUT.ENABLE (1000000);
     
	 -- g_programme := 'LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG.MAIN';
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
	 DBMS_OUTPUT.PUT_LINE ('-------------------------START---------------------------------');

	 v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de Steph_HZ_CUST_SITE_USES_ALL_bill_to_temp_PROC');
     -- g_etape := '10';
     Steph_HZ_CUST_SITE_USES_ALL_bill_to_temp_PROC;
     
     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de Customer_Oracle_PROC');
     -- g_etape := '20';
     Customer_Oracle_bill_to_PROC;

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

END LH2_DTH_SILVER_CUSTOMER_ORACLE_BILL_TO_PKG;