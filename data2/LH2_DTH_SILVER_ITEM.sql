create or replace PACKAGE BODY                  "LH2_DTH_SILVER_ITEMS_PKG" IS
/*     $Header: LH2_DTH_SILVER_ITEMS_PKG.sql 1.0.3 2024/07/02 21:36:00 vsre noship $ */
-- ***************************************************************************
-- @(#) ----------------------------------------------------------------------
-- @(#) Specifique: DTH003 IMPORT des donn�es dans le Silver - DataHub ITEMS
-- @(#) Fichier   : .LH2_DTH_SILVER_ITEMS_PKG.sql
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
-- 23/05/24  1.0.0  COUVY           Version initiale
-- 03/06/24  1.0.1  POTC            Ajout Proc�dure : Steph_Item_Categories_PROC et Steph_Item_Cross_References_PROC
-- 26/06/24  1.0.2  POTC            Renommage de table pour suivre la norme
-- 02/07/24  1.0.3  JKLE            Ajout NVL sur descriptions ITEM
-- 29/07/24  1.0.4  POTC            Ajout NVL sur tous les champs utilis� dans la comparaison du MERGE
-- 07/08/24  1.0.5  POTC            Ajout de champs issus de GL_ACCOUNT_DETAILS_TEMP dans ITEM_DETAILS_ORACLE
-- 19/08/24  1.0.6  POTC            Ajout de champs dans les tables ASIA
-- 06/09/24  1.0.7  POTC            Remplacement du MERGE par un TRUNCATE/INSERT pour la cr�ation des tables ITEM_DETAILS_ORALCE et ITEM_DETAILS_ALL
-- 12/09/24  1.0.8  POTC            R�organisation de la gestion des logs
-- 18/09/24  1.0.9  POTC            Remplacement de jointures par l'appels de fonction pour am�liorer les performances
-- 26/09/24  1.0.10 POTC            Suppression des fonctions qui prennent trop de temps
-- 25/10/24  1.0.11 POTC            Ajout de la cr�ation ITEM_DETAILS_BRAZIL_TEMP
-- 05/11/24  2.0.0  POTC            Changement de la gestion des logs
-- 28/11/24  2.0.1  POTC            Suppression du chargement des champs de regroupements li�s au segment GL
-- 14/01/25  2.0.2  GOICHON         Remplacement de UNION par 2 INSERT afin de voir si on gagne du temps
-- 16/01/25  2.0.3  POTC            Remise en place de UNION car plus rapide que les 2 INSERT
-- 20/02/25  2.0.4  GOICHON         Ajout de LS_ACIM_PO_CATEGORY
-- 07/05/25  2.0.5  POTC            Changement de la jointure pour cr�er REFCFG_ARTICLES_DETAILS_TEMP pour supprimer des doublons
-- 05/06/25  2.0.6  POTC            Renommage de nombreux champs de ITEM_DETAILS_ALL qui ne servaient pas pour les utilis�s pour charger des champs issu du niveau LVO et XMS
-- 06/06/25  2.0.7  POTC            Suppression de champs qui n'�taient pas alimenter
-- 08/08/25  2.0.8  GOICHON         Ajout des champs manquant de REFCFG_INTERFACECOM_INTERFACECOM_ARTICLES_BZ dans REFCFG_ARTICLES_DETAILS_TEMP
-- 16/08/25  2.0.9  GOICHON         Ajout de champs dans ITEM_DETAILS_ORACLE et ITEMS_DETAILS_ALL
-- 22/07/25  2.0.10 GOICHON         Ajout de POSTPROCESSING_LEAD_TIME dans ITEM_DETAILS_ORACLE et ITEMS_DETAILS_ALL
-- 22/07/25  3.0.0  GOICHON         Ajout de la cr�ation ITEM_DETAILS_AUSTRALIA_TEMP
-- 22/07/25  3.1.1  OJABIT          Ajout de EIA_SHIPPING_WAREHOUSE_XMS,EIA_SUB_CONTRACTING_PLANT_CATEGORY_VALUE
-- 29/07/25  3.1.2  OJABIT          Remplacement de la table REFCFG_CODESTAT_LINE_FAMILY_TEMP par des join ind�pendentes
-- 07/08/25  3.1.3  GOICHON         Changement de place de INVENTORY_PLANNING_CODE
-- 25/08/25  3.1.4  OJABIT          Correction de l'emplacement de INVENTORY_PLANNING_CODE
-- 26/08/25  3.2.5  OJABIT          Ajout des hint dans la table item_oracle et item_all
-- 17/09/25  3.2.6  GOICHON         Ajout de FAMILLE_SERVICE_DETAIL et FAMILLE_SERVICE � item_details_all
-- 19/09/25  3.2.7  GOICHON         CHangement de la source de EIA_ALLOCATION dans la partie singapore & korea
-- 29/09/25  3.2.8  GOICHON         Remplacement des jointures avec FILE_VAR_BU_BZ par la jointure avec FILE_TRANSFORMATION_DATA
-- 29/10/25  3.2.9  DEROUBAIX       Changement de la jointure avec FILE_TRANSFORMATION_DATA et ajout de la jointure avec AUSTRALIA_DBO_ICITEMO pour AUSTRALIA.
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
* PROCEDURE   :  Refcfg_Codestat_Line_Family_Temp_PROC
* DESCRIPTION :  Create table with merged InterfaceCom Codes
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/
/* PROCEDURE Refcfg_Codestat_Line_Family_Temp_PROC
   IS v_procedure varchar2(100) := 'Refcfg_Codestat_Line_Family_Temp_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Refcfg_Codestat_Line_Family_Temp_PROC';
   -- g_level     :='S';
   --  g_date_deb  :=sysdate;
   -- g_status    :=NULL;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;
--  g_table :='REFCFG_CODESTAT_LINE_FAMILY_TEMP';

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

--  g_etape := '101';
   g_table     := 'REFCFG_CODESTAT_LINE_FAMILY_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE REFCFG_CODESTAT_LINE_FAMILY_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '102';
   g_table     := 'REFCFG_CODESTAT_LINE_FAMILY_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT INTO REFCFG_CODESTAT_LINE_FAMILY_TEMP
      SELECT DISTINCT
            RICSP.CODE_STAT EIA_STATISTIC_CODE,
            RICSP.LIBELLE EIA_STATISTIC_CODE_DESCRIPTION,
            RILC.NOLIGNE ITEM_LINE,
            RIL.NOM ITEM_LINE_DESCRIPTION,
            RIFP.NOFAMILLE ITEM_FAMILY,
            RIFP.LIBELLE ITEM_FAMILY_DESCRIPTION
      FROM
            REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_CODESTAT_BZ rilc,
            REFCFG_INTERFACECOM_INTERFACECOM_CODES_STAT_PRODUIT_BZ ricsp,
            REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_BZ ril,
            REFCFG_INTERFACECOM_INTERFACECOM_FAMILLES_PRODUIT_BZ rifp
      WHERE 1=1
      AND RICSP.CODE_STAT = RILC.CODESTAT
      AND RILC.NOLIGNE = RIL.NOLIGNE
      AND TO_NUMBER(RIL.NOFAMILLE) = TO_NUMBER(RIFP.NOFAMILLE)
      ORDER BY EIA_STATISTIC_CODE
      ;
   --    g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'REFCFG_CODESTAT_LINE_FAMILY_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('REFCFG_CODESTAT_LINE_FAMILY_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

END Refcfg_Codestat_Line_Family_Temp_PROC;*/

/****************************************************************************************
* PROCEDURE   :  Refcfg_Articles_Details_Temp_PROC
* DESCRIPTION :  Create Articles Details table with all codes ststs based on REFCFG
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Refcfg_Articles_Details_Temp_PROC
IS
   v_procedure       VARCHAR2(100) := 'Refcfg_Articles_Details_Temp_PROC';
   v_date_deb_proc   TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   --  g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Refcfg_Articles_Details_Temp_PROC';
   --  g_level     :='S';
   --  g_date_deb  :=sysdate;
   --  g_status    :=NULL;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;
   -- g_table     := 'REFCFG_ARTICLES_DETAILS_TEMP';

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   -- g_etape := '201';
   g_table     := 'REFCFG_ARTICLES_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE REFCFG_ARTICLES_DETAILS_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '202';
   g_table     := 'REFCFG_ARTICLES_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   insert into REFCFG_ARTICLES_DETAILS_TEMP
      SELECT DISTINCT
            RIA.CODEORACLE CODE_ORACLE,
            RIA.DESIGNOWNER ALICE_DESIGN_OWNER_PLANT,
            RIIP2.TCODEORA DESIGN_OWNER_PLANT,
            RIA.SHIPPINGWAREHOUSE ALICE_SHIPPING_WAREHOUSE,
            RIIP.TCODEORA SHIPPING_WAREHOUSE,
            RIA.DESIGNATION LONG_DESCRIPTION,
            RIA.MASS_ALLOCATION ,
            RIA.CIRCUIT ,
            RIA.NATUREPRODUIT NAUTRE_PRODUIT,
            RIA.PRIXTARIF PRIX_TARIF,
            RIA.POIDS ,
            RIA.CODEDOUANE CODE_DOUANE,
            --RCLF.EIA_STATISTIC_CODE EIA_STATISTIC_CODE,
            ricsp.CODE_STAT EIA_STATISTIC_CODE,
            --RCLF.EIA_STATISTIC_CODE_DESCRIPTION EIA_STATISTIC_CODE_DESCRIPTION,
            RICSP.LIBELLE EIA_STATISTIC_CODE_DESCRIPTION, --
            rilc.NOLIGNE ITEM_LINE,
            --RCLF.ITEM_LINE ITEM_LINE,
            --RCLF.ITEM_LINE_DESCRIPTION ITEM_LINE_DESCRIPTION,
            RIL.NOM ITEM_LINE_DESCRIPTION,
            --RCLF.ITEM_FAMILY ITEM_FAMILY,
            rifp.NOFAMILLE ITEM_FAMILY,
            --RCLF.ITEM_FAMILY_DESCRIPTION ITEM_FAMILY_DESCRIPTION,
            RIFP.LIBELLE ITEM_FAMILY_DESCRIPTION,
            RIA.COO ,
            RIA.CODEALICE CODE_ALICE ,
            ria.QTEMAX,
            ria.LIGNEPRODUIT,
            ria.STATUT,
            ria.COMMENTAIRES,
            ria.SERIALISE,
            ria.CODESTAT,
            ria.DELAI,
            ria.QTE_DELAI,
            ria.SCENARIO_CONFIG,
            ria.SERIE,
            ria.D20JOT,
            ria.QTEXXJOT,
            ria.DELAIXXJOT,
            ria.OBSOLETE,
            ria.DATE_CREATION,
            ria.DATE_DERNIERE_MODIF,
            ROWNUM ROW_NNUMBER_ID,
            SYSDATE ROW_CREATION_DATE,
            SYSDATE ROW_LAST_UPDATE_DATE
      FROM
            REFCFG_INTERFACECOM_INTERFACECOM_ARTICLES_BZ ria
      LEFT OUTER JOIN     REFCFG_INTERFACECOM_INTERFACECOM_PFRFTBUGS_BZ riip
            ON ( RIIP.TBUGROUPE = 'UG'
            AND ltrim(RIA.SHIPPINGWAREHOUSE) =  ltrim(RIIP.TBUUG) )
      LEFT OUTER JOIN    REFCFG_INTERFACECOM_INTERFACECOM_PFRFTBUGS_BZ riip2
            ON ( RIIP2.TBUGROUPE = 'UG'
            AND ltrim(RIA.DESIGNOWNER) =  ltrim(RIIP2.TBUUG) )
      /*LEFT OUTER JOIN    REFCFG_CODESTAT_LINE_FAMILY_TEMP rclf
            ON RIA.CODESTAT = RCLF.EIA_STATISTIC_CODE*/

      LEFT OUTER JOIN REFCFG_INTERFACECOM_INTERFACECOM_CODES_STAT_PRODUIT_BZ ricsp
         ON RIA.CODESTAT = RICSP.CODE_STAT

         LEFT OUTER JOIN REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_CODESTAT_BZ rilc
      on RICSP.CODE_STAT = RILC.CODESTAT

         LEFT OUTER JOIN    REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_BZ ril
         ON RILC.NOLIGNE = RIL.NOLIGNE

      LEFT OUTER JOIN    REFCFG_INTERFACECOM_INTERFACECOM_FAMILLES_PRODUIT_BZ rifp
         ON TO_NUMBER(RIL.NOFAMILLE) = TO_NUMBER(RIFP.NOFAMILLE)

      --  WHERE  ria.date_creation = (SELECT MAX(ria2.date_creation) FROM REFCFG_INTERFACECOM_INTERFACECOM_ARTICLES_BZ ria2 WHERE ria2.codealice = ria.codealice)   -- en commentaire le 07/05/25
      WHERE  ria.date_creation = (SELECT MAX(ria2.date_creation) FROM REFCFG_INTERFACECOM_INTERFACECOM_ARTICLES_BZ ria2 WHERE ria2.CODEORACLE = ria.CODEORACLE)
      -- ORDER BY EIA_ITEM_CODE
      -- ORDER BY ALICE_ITEM_CODE  -- en commentaire le 07/05/25
      ;
   -- g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   --  g_etape := '100';
   g_table     := 'REFCFG_ARTICLES_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('REFCFG_ARTICLES_DETAILS_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Refcfg_Articles_Details_Temp_PROC;

   /****************************************************************************************
   * PROCEDURE   :  Steph_Item_Categories_PROC
   * DESCRIPTION :  Create Item Categories table with specific categories based on STEPH
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
   PROCEDURE Steph_Item_Categories_PROC
   IS v_procedure varchar2(100) := 'Steph_Item_Categories_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Steph_Item_Categories_PROC';
   -- g_level     :='S';
   --  g_date_deb  :=sysdate;
   -- g_status    :=NULL;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;
   --  g_table :='STEPH_ITEM_CATEGORIES_TEMP';

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   --  g_etape := '301';
   g_table     := 'STEPH_ITEM_CATEGORIES_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'TRUNCATE TABLE STEPH_ITEM_CATEGORIES_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   --  g_etape := '302';
   g_table     := 'STEPH_ITEM_CATEGORIES_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         INSERT INTO STEPH_ITEM_CATEGORIES_TEMP
         SELECT *
         FROM (SELECT DISTINCT
                     mic.INVENTORY_ITEM_ID         INVENTORY_ITEM_ID,
                     mic.ORGANIZATION_ID           ORGANIZATION_ID,
                     UPPER(mcst.category_set_name) category_set_name,
                     mc.CONCATENATED_SEGMENTS      category_value
               FROM  STEPH_APPS_MTL_ITEM_CATEGORIES_bz  mic,
                     STEPH_APPS_MTL_CATEGORY_SETS_tl_bz mcst,
                     STEPH_APPS_MTL_CATEGORIES_b_kfv_bz mc
               WHERE mic.category_set_id = mcst.category_set_id
               AND mic.category_id     = mc.category_id
               AND mic.creation_date   = (SELECT MAX(mic2.creation_date)
                                          FROM STEPH_APPS_MTL_ITEM_CATEGORIES_bz mic2
                                          WHERE mic.inventory_item_id = mic2.inventory_item_id
                                             and mic.ORGANIZATION_ID   = mic2.ORGANIZATION_ID
                                             and mic.category_set_id   = mic2.category_set_id)
               AND mcst.LANGUAGE = USERENV ('LANG')
               AND UPPER(mcst.category_set_name) IN ('EIA ALLOCATION',
                                                      'EIA CIRCUIT INFORMATION',
                                                      'EIA COUNTRY OF ORIGIN',
                                                      'EIA DESIGN OWNER PLANT',
                                                      'EIA ITEM STRUCTURE',
                                                      'EIA SHIPPING WAREHOUSE',
                                                      'EIA STATISTIC CODE',
                                                      'EIA PRODUCT STATISTICS CODE',
                                                      'EIA EBUSINESS-TAX',
                                                      'EMR TARIFF CODE',
                                                      'EMR LEAD TIME TYPE LS',
                                                      'EMR SERVICE CATEGORY',
                                                      'INVENTORY',
                                                      'LS ACIM PO CATEGORY',
                                                   'EIA SUB CONTRACTING PLANT')
            ORDER BY INVENTORY_ITEM_ID, ORGANIZATION_ID
            )
         PIVOT (MAX(category_value) AS category_value
               FOR category_set_name IN ('EIA ALLOCATION' AS "EIA_ALLOCATION",
                                       'EIA CIRCUIT INFORMATION' AS "EIA_CIRCUIT_INFORMATION",
                                       'EIA COUNTRY OF ORIGIN' AS "EIA_COUNTRY_OF_ORIGIN",
                                       'EIA DESIGN OWNER PLANT' AS "EIA_DESIGN_OWNER_PLANT",
                                       'EIA ITEM STRUCTURE' AS "EIA_ITEM_STRUCTURE",
                                       'EIA SHIPPING WAREHOUSE' AS "EIA_SHIPPING_WAREHOUSE",
                                       'EIA STATISTIC CODE' AS "EIA_STATISTIC_CODE",
                                       'EIA PRODUCT STATISTICS CODE' AS "EIA_PRODUCT_STATISTICS_CODE",
                                       'EIA EBUSINESS-TAX' AS "EIA_EBUSINESS_TAX",
                                       'EMR TARIFF CODE' AS "EMR_TARIFF_CODE",
                                       'EMR LEAD TIME TYPE LS' AS "EMR_LEAD_TIME_TYPE_LS",
                                       'EMR SERVICE CATEGORY' AS "EMR_SERVICE_CATEGORY",
                                       'INVENTORY' AS "INVENTORY",
                                       'LS ACIM PO CATEGORY' AS "LS_ACIM_PO_CATEGORY",
                                       'EIA SUB CONTRACTING PLANT' AS "EIA_SUB_CONTRACTING_PLANT"))
                                       ;
   -- g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   -- g_etape := '303';
   -- EXECUTE IMMEDIATE 'CREATE INDEX STEPH_ITEM_CATEGORIES_TEMP_PK ON STEPH_ITEM_CATEGORIES_TEMP(INVENTORY_ITEM_ID, ORGANIZATION_ID)';

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'STEPH_ITEM_CATEGORIES_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('STEPH_ITEM_CATEGORIES_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Steph_Item_Categories_PROC;

   /****************************************************************************************
   * PROCEDURE   :  Steph_Item_Cross_References_PROC
   * DESCRIPTION :  Create Item Cross References table with cross ref. "LISA" & "GPAO" based on STEPH
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
   PROCEDURE Steph_Item_Cross_References_PROC
   IS v_procedure varchar2(100) := 'Steph_Item_Cross_References_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Steph_Item_Cross_References_PROC';
   -- g_level     :='S';
   -- g_date_deb  :=sysdate;
   -- g_status    :=NULL;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;
   --  g_table :='STEPH_ITEM_CROSS_REFERENCES_TEMP';

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   -- g_etape := '401';
   g_table     := 'STEPH_ITEM_CROSS_REFERENCES_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE STEPH_ITEM_CROSS_REFERENCES_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   -- g_etape := '402';
   g_table     := 'STEPH_ITEM_CROSS_REFERENCES_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         INSERT INTO STEPH_ITEM_CROSS_REFERENCES_TEMP
         SELECT *
         FROM (SELECT DISTINCT
                     mcr.inventory_item_id            inventory_item_id,
                     mcr.organization_id              organization_id,
                     UPPER(mcr.cross_reference_type)  cross_reference_type,
                     mcr.cross_reference              cross_reference
               FROM STEPH_APPS_MTL_CROSS_REFERENCES_B_BZ mcr
               WHERE UPPER(mcr.cross_reference_type) IN ('LEGACY ITEM LS',
                                                         'LISA ITEM LS')
               AND mcr.creation_date = (SELECT MAX(mcr2.creation_date)
                                          FROM STEPH_APPS_MTL_CROSS_REFERENCES_B_BZ mcr2
                                          WHERE   mcr.inventory_item_id  = mcr2.inventory_item_id
                                          and mcr.organization_id      = mcr2.organization_id
                                          and mcr.cross_reference_type = mcr2.cross_reference_type )
            ORDER BY INVENTORY_ITEM_ID, ORGANIZATION_ID
            )
         PIVOT (MAX(CROSS_REFERENCE)
               FOR CROSS_REFERENCE_TYPE IN ('LISA ITEM LS'    AS "LISA_ITEM_LS" ,
                                             'LEGACY ITEM LS'  AS "LEGACY_ITEM_LS" ))
            ;
   -- g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   --  g_etape := '403';
   --  EXECUTE IMMEDIATE 'CREATE INDEX STEPH_ITEM_CROSS_REFERENCES_TEMP_PK ON STEPH_ITEM_CROSS_REFERENCES_TEMP(INVENTORY_ITEM_ID, ORGANIZATION_ID)';

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   --  g_etape := '100';
   g_table     := 'STEPH_ITEM_CROSS_REFERENCES_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('STEPH_ITEM_CROSS_REFERENCES_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Steph_Item_Cross_References_PROC;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_China_Proc
   * DESCRIPTION :  Procedure cr�ation tables ITEM from CHINA
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
PROCEDURE Item_Details_China_Proc
   IS v_procedure varchar2(100) := 'Item_Details_China_Proc';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Not_Oracle_PROC';
   --  g_level     :='S';
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

   /* CHINA Items */
   -- g_table     :='ITEM_DETAILS_CHINA_TEMP';

--  g_etape := '501';
   g_table     := 'ITEM_DETAILS_CHINA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_CHINA_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '502';
   g_table     := 'ITEM_DETAILS_CHINA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT INTO ITEM_DETAILS_CHINA_TEMP
      WITH CHINA_DBO_ZZ_V_BO_PRODUCTBASE_FZU_BZ_ranked AS(
            SELECT cdzvbpf.*,
               ROW_NUMBER() OVER (PARTITION BY LOCALPRODUCTCODE ORDER BY DESIGNATION1_2 DESC) AS rn
            FROM CHINA_DBO_ZZ_V_BO_PRODUCTBASE_FZU_BZ cdzvbpf
                                                            )
                                                            SELECT DISTINCT
                                                               CDZVBPFR.LOCALPRODUCTCODE INVENTORY_ITEM_ID,
                                                               CDZVBPFR.LOCALPRODUCTCODE ITEM_CODE,
                                                               RAD.CODE_ORACLE EIA_ITEM_CODE,
                                                               CDZVBPFR.PRODUCTTYPE PRODUCT_TYPE,
                                                               --> remplacement de FUZ par la valeur oracle FUM
                                                               DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'FUM',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
                                                               DECODE(RAD.DESIGN_OWNER_PLANT,NULL,'FUM',RAD.DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
                                                               DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'FUM',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
                                                               DECODE(RAD.SHIPPING_WAREHOUSE,NULL,'FUM',RAD.SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
                                                               DECODE(RAD.LONG_DESCRIPTION,NULL,CDZVBPFR.DESIGNATION1_2,RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
                                                               DECODE(RAD.MASS_ALLOCATION,NULL,TO_CHAR(CDZVBPFR.ITEMMASSALLOCATION),RAD.MASS_ALLOCATION) EIA_ALLOCATION,
                                                               RAD.CIRCUIT EIA_CIRCUIT_INFORMATION,
                                                               RAD.NATURE_PRODUIT ITEM_NATURE,
                                                               RAD.PRIX_TARIF LIST_PRICE,
                                                               RAD.POIDS WEIGHT,
                                                               RAD.CODE_DOUANE CUSTOMS_CODE,
                                                               --DECODE(RAD.EIA_STATISTIC_CODE,NULL,RCLF.EIA_STATISTIC_CODE,RAD.EIA_STATISTIC_CODE) EIA_STATISTIC_CODE,
                                                               DECODE(RAD.EIA_STATISTIC_CODE,NULL,rics.CODE_STAT,RAD.EIA_STATISTIC_CODE) EIA_STATISTIC_CODE,
                                                               --DECODE(RAD.EIA_STATISTIC_CODE_DESCRIPTION,NULL,RCLF.EIA_STATISTIC_CODE_DESCRIPTION,RAD.EIA_STATISTIC_CODE_DESCRIPTION) EIA_STATISTIC_CODE_DESCRIPTION,
                                                               DECODE(RAD.EIA_STATISTIC_CODE_DESCRIPTION,NULL,rics.LIBELLE,RAD.EIA_STATISTIC_CODE_DESCRIPTION) EIA_STATISTIC_CODE_DESCRIPTION,
                                                               --DECODE(RAD.ITEM_LINE,NULL,RCLF.ITEM_LINE,RAD.ITEM_LINE) ITEM_LINE,
                                                               DECODE(RAD.ITEM_LINE,NULL,RILC.NOLIGNE,RAD.ITEM_LINE) ITEM_LINE,
                                                               --DECODE(RAD.ITEM_LINE_DESCRIPTION,NULL,RCLF.ITEM_LINE_DESCRIPTION,RAD.ITEM_LINE_DESCRIPTION) ITEM_LINE_DESCRIPTION,
                                                               DECODE(RAD.ITEM_LINE_DESCRIPTION,NULL,RIL.NOM,RAD.ITEM_LINE_DESCRIPTION) ITEM_LINE_DESCRIPTION,
                                                               --DECODE(RAD.ITEM_FAMILY,NULL,RCLF.ITEM_FAMILY,RAD.ITEM_FAMILY) ITEM_FAMILY,
                                                               DECODE(RAD.ITEM_FAMILY,NULL,RIFP.NOFAMILLE,RAD.ITEM_FAMILY) ITEM_FAMILY,
                                                               --DECODE(RAD.ITEM_FAMILY_DESCRIPTION,NULL,RCLF.ITEM_FAMILY_DESCRIPTION,RAD.ITEM_FAMILY_DESCRIPTION) ITEM_FAMILY_DESCRIPTION,
                                                               DECODE(RAD.ITEM_FAMILY_DESCRIPTION,NULL,RIFP.LIBELLE,RAD.ITEM_FAMILY_DESCRIPTION) ITEM_FAMILY_DESCRIPTION,
                                                               --   CDZVBPFR.BUSINESS_UNIT BU,
                                                               --  coalesce(vb.BU_NEW,'NOT AFFECTED') BU,  modif le 29/9/25
                                                               coalesce(vb.VALEUR,'NOT AFFECTED') BU,
                                                               1700 ORGANIZATION_ID,
                                                               'CHINA' "SOURCE"	,
                                                               ROWNUM ROW_NNUMBER_ID,
                                                               SYSDATE ROW_CREATION_DATE,
                                                               SYSDATE ROW_LAST_UPDATE_DATE ,
                                                               null "CATEGORY" ,
                                                               null CNTLACCT,
                                                               coalesce(pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE,'NOT AFFECTED') SUBBU,
                                                               null ITEM_CREATION_DATE,
                                                               null ITEM_LAST_UPDATE_DATE,
                                                               null ITEM_STATUS  ,
                                                               '4050' ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                                                               '220' ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                                                               '0000' ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                                                               '5100ZZZZ' ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                                                               --DECODE(RAD.EIA_ALLOCATION,NULL,TO_CHAR(CDZVBPFR.ITEMMASSALLOCATION),RAD.EIA_ALLOCATION) ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                               coalesce(RAD.MASS_ALLOCATION,TO_CHAR(CDZVBPFR.ITEMMASSALLOCATION),'000') ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                               '0000' ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                                                               '4050' ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                                                               '220' ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                                                               '0000' ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                                                               '4110ZZZZ' ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                                                               -- DECODE(RAD.EIA_ALLOCATION,NULL,TO_CHAR(CDZVBPFR.ITEMMASSALLOCATION),RAD.EIA_ALLOCATION) ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                               coalesce(RAD.MASS_ALLOCATION,TO_CHAR(CDZVBPFR.ITEMMASSALLOCATION),'000') ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                               '0000' ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
                                                               101 CHART_OF_ACCOUNTS_ID
                                                            FROM
                                                               /*CHINA_DBO_ZZ_V_BO_PRODUCTBASE_FZU_BZ_ranked cdzvbpfR,
                                                               REFCFG_ARTICLES_DETAILS_TEMP rad,
                                                               --REFCFG_CODESTAT_LINE_FAMILY_TEMP rclf,
                                                               FILE_PRODUCTGROUP_SEGMENT5_BZ pgs ,
                                                               FILE_VAR_BU_bz vb

                                                            WHERE 1=1
                                                            --   AND CDZVBPFR."ORACLEITEMCODE(ALICECODE)" = RAD.EIA_ITEM_CODE (+)
                                                               AND CDZVBPFR."ORACLEITEMCODE(ALICECODE)" = RAD.CODE_ALICE (+)
                                                               --AND CDZVBPFR.CODESTATPRODUCTS = RCLF.EIA_STATISTIC_CODE (+)
                                                               AND DECODE(RAD.MASS_ALLOCATION,NULL,TO_CHAR(CDZVBPFR.ITEMMASSALLOCATION),RAD.MASS_ALLOCATION) = pgs.PRODUCT_GROUP_GL_CODE (+)
                                                               AND pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE = VB.SUBBU (+)
                                                               AND rn = 1
                                                            ORDER BY EIA_ITEM_CODE */
                                                            CHINA_DBO_ZZ_V_BO_PRODUCTBASE_FZU_BZ_ranked cdzvbpfR
                                                            LEFT JOIN REFCFG_ARTICLES_DETAILS_TEMP rad
                                                            ON cdzvbpfR."ORACLEITEMCODE(ALICECODE)" = rad.CODE_ALICE
                                                            LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_CODES_STAT_PRODUIT_BZ rics
                                                            ON cdzvbpfR.CODESTATPRODUCTS = rics.CODE_STAT
                                                            LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_CODESTAT_BZ rilc
                                                            ON rics.CODE_STAT = rilc.CODESTAT
                                                            LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_BZ ril
                                                            ON rilc.NOLIGNE = ril.NOLIGNE
                                                            LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_FAMILLES_PRODUIT_BZ rifp
                                                            ON TO_NUMBER(ril.NOFAMILLE) = TO_NUMBER(rifp.NOFAMILLE)
                                                            LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
                                                            ON DECODE(rad.MASS_ALLOCATION, NULL, TO_CHAR(cdzvbpfR.ITEMMASSALLOCATION), rad.MASS_ALLOCATION) = pgs.PRODUCT_GROUP_GL_CODE
                                                         /* LEFT JOIN FILE_VAR_BU_bz vb
                                                            ON pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE = vb.SUBBU
                                                            */
                                                            LEFT JOIN FILE_TRANSFORMATION_DATA_BZ vb
                                                               ON  vb.CLE =pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                                                                  and vb.type = 'BU_NEW'
                                                            WHERE rn = 1
                                                            ORDER BY EIA_ITEM_CODE;

   -- g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   --  g_etape := '100';
   g_table     := 'ITEM_DETAILS_CHINA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_CHINA_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_China_Proc;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_India_Proc
   * DESCRIPTION :  Procedure cr�ation tables ITEM from INDIA
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
PROCEDURE Item_Details_India_Proc
   IS v_procedure varchar2(100) := 'Item_Details_India_Proc';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Not_Oracle_PROC';
   --  g_level     :='S';
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

   /* INDIA ITEMS */
   --  g_table     :='ITEM_DETAILS_INDIA_TEMP';

   -- g_etape := '504';
   g_table     := 'ITEM_DETAILS_INDIA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_INDIA_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '505';
   g_table     := 'ITEM_DETAILS_INDIA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT INTO ITEM_DETAILS_INDIA_TEMP
      WITH INDIA_DBO_VBOPRODUCTBASE_BZ_ranked AS(
            SELECT idvp.*,
               ROW_NUMBER() OVER (PARTITION BY LOCAL_PRODUCT_CODE ORDER BY "DESIGNATION_1" || ' ' || "DESIGNATION_2" DESC) AS rn
            FROM INDIA_DBO_VBOPRODUCTBASE_BZ idvp
                                                   )
                                                   SELECT DISTINCT
                                                      IDVPR.LOCAL_PRODUCT_CODE INVENTORY_ITEM_ID,
                                                      IDVPR.LOCAL_PRODUCT_CODE ITEM_CODE,
                                                      RAD.CODE_ORACLE EIA_ITEM_CODE,
                                                      NULL PRODUCT_TYPE,
                                                      --> remplacement de IND par la valeur oracle TRM
                                                      DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'TRM',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
                                                      DECODE(RAD.DESIGN_OWNER_PLANT,NULL,'TRM',RAD.DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
                                                      DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'TRM',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
                                                      DECODE(RAD.SHIPPING_WAREHOUSE,NULL,'TRM',RAD.SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
                                                      DECODE(RAD.LONG_DESCRIPTION,NULL,IDVPR."DESIGNATION_1" || ' ' || IDVPR."DESIGNATION_2",RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
                                                      DECODE(RAD.MASS_ALLOCATION,NULL,TO_CHAR(IDVPR.MASS_ALLOCATION),RAD.MASS_ALLOCATION) EIA_ALLOCATION,
                                                      RAD.CIRCUIT EIA_CIRCUIT_INFORMATION,
                                                      RAD.NATURE_PRODUIT ITEM_NATURE,
                                                      RAD.PRIX_TARIF LIST_PRICE,
                                                      DECODE(RAD.POIDS,NULL,IDVPR.WEIGHT,RAD.POIDS) WEIGHT,
                                                      DECODE(RAD.CODE_DOUANE,NULL,IDVPR.CUSTOM_CODE,RAD.CODE_DOUANE) CUSTOMS_CODE,
                                                      --DECODE(RAD.EIA_STATISTIC_CODE,NULL,RCLF.EIA_STATISTIC_CODE,RAD.EIA_STATISTIC_CODE) EIA_STATISTIC_CODE,
                                                      DECODE(RAD.EIA_STATISTIC_CODE,NULL,rics.CODE_STAT,RAD.EIA_STATISTIC_CODE) EIA_STATISTIC_CODE,
                                                      --DECODE(RAD.EIA_STATISTIC_CODE_DESCRIPTION,NULL,RCLF.EIA_STATISTIC_CODE_DESCRIPTION,RAD.EIA_STATISTIC_CODE_DESCRIPTION) EIA_STATISTIC_CODE_DESCRIPTION,
                                                      DECODE(RAD.EIA_STATISTIC_CODE_DESCRIPTION,NULL,rics.LIBELLE,RAD.EIA_STATISTIC_CODE_DESCRIPTION) EIA_STATISTIC_CODE_DESCRIPTION,
                                                      --DECODE(RAD.ITEM_LINE,NULL,RCLF.ITEM_LINE,RAD.ITEM_LINE) ITEM_LINE,
                                                      DECODE(RAD.ITEM_LINE,NULL,RILC.NOLIGNE,RAD.ITEM_LINE) ITEM_LINE,
                                                      --DECODE(RAD.ITEM_LINE_DESCRIPTION,NULL,RCLF.ITEM_LINE_DESCRIPTION,RAD.ITEM_LINE_DESCRIPTION) ITEM_LINE_DESCRIPTION,
                                                      DECODE(RAD.ITEM_LINE_DESCRIPTION,NULL,RIL.NOM,RAD.ITEM_LINE_DESCRIPTION) ITEM_LINE_DESCRIPTION,
                                                      --DECODE(RAD.ITEM_FAMILY,NULL,RCLF.ITEM_FAMILY,RAD.ITEM_FAMILY) ITEM_FAMILY,
                                                      DECODE(RAD.ITEM_FAMILY,NULL,RIFP.NOFAMILLE,RAD.ITEM_FAMILY) ITEM_FAMILY,
                                                      --DECODE(RAD.ITEM_FAMILY_DESCRIPTION,NULL,RCLF.ITEM_FAMILY_DESCRIPTION,RAD.ITEM_FAMILY_DESCRIPTION) ITEM_FAMILY_DESCRIPTION,
                                                      DECODE(RAD.ITEM_FAMILY_DESCRIPTION,NULL,RIFP.LIBELLE,RAD.ITEM_FAMILY_DESCRIPTION) ITEM_FAMILY_DESCRIPTION,
                                                      /* CASE
                                                            WHEN INSTR(DECODE(RAD.EIA_ALLOCATION,NULL,TO_CHAR(IDVPR.MASS_ALLOCATION),RAD.EIA_ALLOCATION), 'M') > 0 THEN 'C&'||'I'
                                                            ELSE 'EPG'
                                                      END AS BU, */
                                                      --   coalesce(vb.BU_NEW,'NOT AFFECTED') BU,  modif le 29/09/25
                                                      coalesce(vb.VALEUR,'NOT AFFECTED') BU,
                                                      1701 ORGANIZATION_ID,
                                                      'INDIA' SOURCE	,
                                                      ROWNUM ROW_NNUMBER_ID,
                                                      SYSDATE ROW_CREATION_DATE,
                                                      SYSDATE ROW_LAST_UPDATE_DATE ,
                                                      null CATEGORY ,
                                                      null CNTLACCT,
                                                      coalesce(pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE,'NOT AFFECTED') SUBBU,
                                                      null ITEM_CREATION_DATE,
                                                      null ITEM_LAST_UPDATE_DATE,
                                                      null ITEM_STATUS  ,
                                                      '4052' ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                                                      '226' ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                                                      '0000' ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                                                      '5100ZZZZ' ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                                                      -- DECODE(RAD.EIA_ALLOCATION,NULL,TO_CHAR(IDVPR.MASS_ALLOCATION),RAD.EIA_ALLOCATION) ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                      coalesce(RAD.MASS_ALLOCATION,TO_CHAR(IDVPR.MASS_ALLOCATION),'000') ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                      '0000' ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                                                      '4052' ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                                                      '226' ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                                                      '0000' ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                                                      '4110ZZZZ' ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                                                      -- DECODE(RAD.EIA_ALLOCATION,NULL,TO_CHAR(IDVPR.MASS_ALLOCATION),RAD.EIA_ALLOCATION) ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                      coalesce(RAD.MASS_ALLOCATION,TO_CHAR(IDVPR.MASS_ALLOCATION),'000') ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                                                      '0000' ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
                                                      101 CHART_OF_ACCOUNTS_ID
                                                   FROM
                                                      /* INDIA_DBO_VBOPRODUCTBASE_BZ_ranked idvpr,
                                                      REFCFG_ARTICLES_DETAILS_TEMP rad,
                                                      --REFCFG_CODESTAT_LINE_FAMILY_TEMP rclf ,
                                                      FILE_PRODUCTGROUP_SEGMENT5_BZ pgs ,
                                                      FILE_VAR_BU_bz vb,
                                                      REFCFG_INTERFACECOM_INTERFACECOM_CODES_STAT_PRODUIT_BZ rics

                                                   WHERE 1=1
                                                   --   AND IDVPR.ALICE_CODE = RAD.EIA_ITEM_CODE (+)
                                                      AND IDVPR.ALICE_CODE = RAD.CODE_ALICE (+)
                                                      --AND IDVPR.CODE_STAT_PRODUCTS = RCLF.EIA_STATISTIC_CODE (+)
                                                      AND IDVPR.CODE_STAT_PRODUCTS = rics.CODE_STAT (+)
                                                      AND coalesce(RAD.MASS_ALLOCATION,TO_CHAR(IDVPR.MASS_ALLOCATION),'000') = pgs.PRODUCT_GROUP_GL_CODE (+)
                                                      AND pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE = VB.SUBBU (+)
                                                      and rn = 1
                                                   ORDER BY EIA_ITEM_CODE */
                                                   INDIA_DBO_VBOPRODUCTBASE_BZ_ranked idvpr
                                                   LEFT JOIN REFCFG_ARTICLES_DETAILS_TEMP rad
                                                      ON idvpr.ALICE_CODE = rad.CODE_ALICE
                                                   LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_CODES_STAT_PRODUIT_BZ rics
                                                      ON idvpr.CODE_STAT_PRODUCTS = rics.CODE_STAT
                                                   LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_CODESTAT_BZ rilc
                                                      ON rics.CODE_STAT = rilc.CODESTAT
                                                   LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_BZ ril
                                                      ON rilc.NOLIGNE = ril.NOLIGNE
                                                   LEFT JOIN REFCFG_INTERFACECOM_INTERFACECOM_FAMILLES_PRODUIT_BZ rifp
                                                      ON TO_NUMBER(ril.NOFAMILLE) = TO_NUMBER(rifp.NOFAMILLE)
                                                   LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
                                                      ON COALESCE(rad.MASS_ALLOCATION, TO_CHAR(idvpr.MASS_ALLOCATION), '000') = pgs.PRODUCT_GROUP_GL_CODE
                                                   /* LEFT JOIN FILE_VAR_BU_bz vb
                                                      ON pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE = vb.SUBBU remplacer par la jointure avec FILE_TRANSFORMATION_DATA*/
                                                   LEFT JOIN    FILE_TRANSFORMATION_DATA_BZ vb
                                                      ON vb.cle = pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                                                      AND vb.type = 'BU_NEW'
                                                   WHERE rn = 1
                                                   ORDER BY EIA_ITEM_CODE
;
   -- g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

--  g_etape := '100';
   g_table     := 'ITEM_DETAILS_INDIA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
         LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_INDIA_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_India_Proc;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_Korea_Proc
   * DESCRIPTION :  Procedure cr�ation tables ITEM From KOREA
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
PROCEDURE Item_Details_Korea_Proc
   IS v_procedure varchar2(100) := 'Item_Details_Korea_Proc';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Not_Oracle_PROC';
   --  g_level     :='S';
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

   /* KOREA ITEMS */
   -- g_table     :='ITEM_DETAILS_KOREA_TEMP';

--  g_etape := '507';
   g_table     := 'ITEM_DETAILS_KOREA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_KOREA_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '508';
   g_table     := 'ITEM_DETAILS_KOREA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
            INSERT INTO ITEM_DETAILS_KOREA_TEMP
      /*    changement de la selection le 19/08/24
            WITH KOREA_DBO_OEORDD_BZ_ranked AS ( SELECT KDOD.*,ROW_NUMBER() OVER (PARTITION BY TRIM(ITEM) ORDER BY ORDUNIQ DESC) AS rn  FROM KOREA_DBO_OEORDD_BZ kdod )
                                                SELECT DISTINCT
                                                   TRIM(KDODR.ITEM) INVENTORY_ITEM_ID,
                                                   TRIM(KDODR.ITEM) ITEM_CODE,
                                                   RAD.EIA_ITEM_CODE EIA_ITEM_CODE,
                                                   KDODR.CATEGORY PRODUCT_TYPE,
                                                   DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'KOR',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
                                                   DECODE(RAD.EIA_DESIGN_OWNER_PLANT,NULL,'KOR',RAD.EIA_DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
                                                   DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'KOR',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
                                                   DECODE(RAD.EIA_SHIPPING_WAREHOUSE,NULL,'KOR',RAD.EIA_SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
                                                   DECODE(RAD.LONG_DESCRIPTION,NULL,KDODR."DESC",RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
                                                   RAD.EIA_ALLOCATION EIA_ALLOCATION,
                                                   RAD.EIA_CIRCUIT_INFORMATION,
                                                   RAD.ITEM_NATURE,
                                                   RAD.LIST_PRICE,
                                                   RAD.WEIGHT,
                                                   RAD.CUSTOMS_CODE,
                                                   RAD.EIA_STATISTIC_CODE,
                                                   RAD.EIA_STATISTIC_CODE_DESCRIPTION,
                                                   RAD.ITEM_LINE,
                                                   RAD.ITEM_LINE_DESCRIPTION,
                                                   RAD.ITEM_FAMILY,
                                                   RAD.ITEM_FAMILY_DESCRIPTION,
                                                   CASE WHEN INSTR(TRIM(KDODR.CATEGORY), 'ACSS0') > 0 THEN  'M&'||'D'  ELSE 'C&'||'I'  END AS BU,
                                                   99992 ORGANIZATION_ID,
                                                   'KOREA' SOURCE	,
                                                   ROWNUM ROW_NNUMBER_ID,
                                                   SYSDATE ROW_CREATION_DATE,
                                                   SYSDATE ROW_LAST_UPDATE_DATE
                                                FROM
                                                   KOREA_DBO_OEORDD_BZ_ranked kdodr,
                                                   REFCFG_ARTICLES_DETAILS_TEMP rad
                                                WHERE 1=1
                                                   AND TRIM(KDODR.ITEM) = RAD.EIA_ITEM_CODE (+)
                                                   AND TRIM(KDODR.ITEM) IS NOT NULL
                                                   and rn = 1
                                                ORDER BY EIA_ITEM_CODE   ; */
WITH vw AS (
                        SELECT DISTINCT
                              TRIM(KDODR.ITEM) INVENTORY_ITEM_ID,
                              TRIM(KDODR.ITEM) ITEM_CODE,
                              RAD.CODE_ORACLE EIA_ITEM_CODE,
                              null PRODUCT_TYPE,
                              DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'KOR',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
                              DECODE(RAD.DESIGN_OWNER_PLANT,NULL,'KOR',RAD.DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
                              DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'KOR',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
                              DECODE(RAD.SHIPPING_WAREHOUSE,NULL,'KOR',RAD.SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
                              DECODE(RAD.LONG_DESCRIPTION,NULL,trim(KDODR."DESC"),RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
                           --   RAD.EIA_ALLOCATION EIA_ALLOCATION,
                              /* CASE WHEN RAD.MASS_ALLOCATION is not null THEN RAD.MASS_ALLOCATION
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'ACSS0' THEN 'M12'
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'AC' OR TRIM(KDODR.CATEGORY) = 'VMA' THEN 'M11'
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'GM'  THEN 'M17'
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'MG' OR TRIM(KDODR.CATEGORY) = 'MGS' THEN 'M14'
                              ELSE 'M16'
                              END END END END END AS EIA_ALLOCATION,*/
                              CASE WHEN RAD.MASS_ALLOCATION is not null THEN RAD.MASS_ALLOCATION
                              else td.valeur end as EIA_ALLOCATION,
                              RAD.CIRCUIT EIA_CIRCUIT_INFORMATION,
                              RAD.NATURE_PRODUIT ITEM_NATURE,
                              RAD.PRIX_TARIF LIST_PRICE,
                              RAD.POIDS WEIGHT,
                              RAD.CODE_DOUANE CUSTOMS_CODE,
                              RAD.EIA_STATISTIC_CODE,
                              RAD.EIA_STATISTIC_CODE_DESCRIPTION,
                              RAD.ITEM_LINE,
                              RAD.ITEM_LINE_DESCRIPTION,
                              RAD.ITEM_FAMILY,
                              RAD.ITEM_FAMILY_DESCRIPTION,
                              99992 ORGANIZATION_ID,
                              'KOREA' SOURCE	,
                              TRIM(KDODR.CATEGORY) CATEGORY ,
                              TRIM(KDODR.ACCTSET) CNTLACCT,
                           /*    CASE WHEN pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE is not null THEN pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'ACSS0' THEN 'MOTION'
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'AC' OR TRIM(KDODR.CATEGORY) = 'VMA' THEN 'CIMD MOTORS'
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'GM'  THEN 'GBMD'
                              ELSE CASE WHEN TRIM(KDODR.CATEGORY) = 'MG' OR TRIM(KDODR.CATEGORY) = 'MGS' THEN 'CIMD DRIVES'
                              ELSE 'SERVICE PDR'
                              END END END END END AS SUBBU,*/
                              null ITEM_CREATION_DATE,
                              null ITEM_LAST_UPDATE_DATE,
                              null ITEM_STATUS  ,
                              'KOREA' ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                              'KOREA' ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                              '0000' ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                              '5100ZZZZ' ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                              -- RAD.EIA_ALLOCATION ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                              '0000' ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                              'KOREA' ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                              'KOREA' ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                              '0000' ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                              '4110ZZZZ' ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                              -- RAD.EIA_ALLOCATION ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                              '0000' ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6 ,
                              ROW_NUMBER() OVER (PARTITION BY TRIM(kdodr.ITEM) ORDER BY kdodr.ORDUNIQ DESC) AS rn
                        FROM
                              KOREA_DBO_OEORDD_BZ kdodr
                        LEFT JOIN       REFCFG_ARTICLES_DETAILS_TEMP rad
                        ON TRIM(KDODR.ITEM) = RAD.CODE_ALICE
                        LEFT JOIN    FILE_TRANSFORMATION_DATA_BZ td
                        ON td.cle = TRIM(kdodr.category)
                        AND td.type = 'EIA_ALLOCATION_KOREA'

                           --  , FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
                        WHERE 1=1
                        --  AND TRIM(KDODR.ITEM) = RAD.EIA_ITEM_CODE (+)
                        AND TRIM(KDODR.ITEM) IS NOT NULL
                        -- AND RAD.EIA_ALLOCATION = pgs.PRODUCT_GROUP_GL_CODE (+)
                        ORDER BY EIA_ITEM_CODE
                        )
            SELECT  distinct
               vw.INVENTORY_ITEM_ID ,
               vw.ITEM_CODE,
               vw.EIA_ITEM_CODE,
               vw.PRODUCT_TYPE,
               vw.ALICE_DESIGN_OWNER_PLANT,
               vw.EIA_DESIGN_OWNER_PLANT,
               vw.ALICE_SHIPPING_WAREHOUSE,
               vw.EIA_SHIPPING_WAREHOUSE,
               vw.LONG_DESCRIPTION,
               vw.EIA_ALLOCATION,
               vw.EIA_CIRCUIT_INFORMATION,
               vw.ITEM_NATURE,
               vw.LIST_PRICE,
               vw.WEIGHT,
               vw.CUSTOMS_CODE,
               vw.EIA_STATISTIC_CODE,
               vw.EIA_STATISTIC_CODE_DESCRIPTION,
               vw.ITEM_LINE,
               vw.ITEM_LINE_DESCRIPTION,
               vw.ITEM_FAMILY,
               vw.ITEM_FAMILY_DESCRIPTION,
               -- vb.BU_NEW BU,  mo dif le 29/09/25
               vb.VALEUR BU,
               vw.ORGANIZATION_ID,
               vw.SOURCE,
               ROWNUM ROW_NNUMBER_ID,
               SYSDATE ROW_CREATION_DATE,
               SYSDATE ROW_LAST_UPDATE_DATE,
               vw.CATEGORY,
               vw.CNTLACCT,
               pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE SUBBU,
               vw.ITEM_CREATION_DATE,
               vw.ITEM_LAST_UPDATE_DATE,
               vw.ITEM_STATUS,
               vw.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
               vw.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
               vw.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
               vw.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               vw.EIA_ALLOCATION ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               vw.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
               vw.ITEM_SALES_ACCOUNT_BU_GL_SGT1,
               vw.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
               vw.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
               vw.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               vw.EIA_ALLOCATION ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               vw.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
               101 CHART_OF_ACCOUNTS_ID
            FROM vw
               LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
               ON vw.EIA_ALLOCATION = pgs.PRODUCT_GROUP_GL_CODE (+)
               /*LEFT JOIN FILE_VAR_BU_bz vb
               ON pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE = VB.SUBBU (+)
               remplacer par la jointure avec FILE_TRANSFORMATION_DATA*/
               LEFT JOIN FILE_TRANSFORMATION_DATA_BZ vb
                     ON vb.cle = pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                     AND vb.type = 'BU_NEW'
            WHERE vw.rn =  1
         ;  --  g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'ITEM_DETAILS_KOREA_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_KOREA_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_Korea_Proc;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_Singapore_Proc
   * DESCRIPTION :  Procedure cr�ation tables ITEM from SINGAPORE
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
PROCEDURE Item_Details_Singapore_Proc
   IS v_procedure varchar2(100) := 'Item_Details_Singapore_Proc';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Not_Oracle_PROC';
   --  g_level     :='S';
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

   /* SINGAPORE ITEMS */
--  g_table     :='ITEM_DETAILS_SINGAPORE_TEMP';

   -- g_etape := '511';
   g_table     := 'ITEM_DETAILS_SINGAPORE_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_SINGAPORE_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   -- g_etape := '512';
   g_table     := 'ITEM_DETAILS_SINGAPORE_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT INTO ITEM_DETAILS_SINGAPORE_TEMP
   /*   changement de la selection le 19/08/24
         WITH SINGAPORE_DBO_OEORDD_BZ_ranked AS ( SELECT SDOD.*,  ROW_NUMBER() OVER (PARTITION BY TRIM(ITEM) ORDER BY ORDUNIQ DESC) AS rn FROM SINGAPORE_DBO_OEORDD_BZ sdod  )
                                                SELECT DISTINCT
                                                   TRIM(SDODR.ITEM) INVENTORY_ITEM_ID,
                                                   TRIM(SDODR.ITEM) ITEM_CODE,
                                                   RAD.EIA_ITEM_CODE EIA_ITEM_CODE,
                                                   SDODR.CATEGORY PRODUCT_TYPE,
                                                   DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'SGP',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
                                                   DECODE(RAD.EIA_DESIGN_OWNER_PLANT,NULL,'SGP',RAD.EIA_DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
                                                   DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'SGP',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
                                                   DECODE(RAD.EIA_SHIPPING_WAREHOUSE,NULL,'SGP',RAD.EIA_SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
                                                   DECODE(RAD.LONG_DESCRIPTION,NULL,SDODR."DESC",RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
                                                   RAD.EIA_ALLOCATION,
                                                   RAD.EIA_CIRCUIT_INFORMATION,
                                                   RAD.ITEM_NATURE,
                                                   RAD.LIST_PRICE,
                                                   RAD.WEIGHT,
                                                   RAD.CUSTOMS_CODE,
                                                   RAD.EIA_STATISTIC_CODE,
                                                   RAD.EIA_STATISTIC_CODE_DESCRIPTION,
                                                   RAD.ITEM_LINE,
                                                   RAD.ITEM_LINE_DESCRIPTION,
                                                   RAD.ITEM_FAMILY,
                                                   RAD.ITEM_FAMILY_DESCRIPTION,
                                                   CASE  WHEN INSTR(TRIM(SDODR.ACCTSET), 'LSMOT') > 0 OR INSTR(TRIM(SDODR.ACCTSET), 'LSMSV') > 0 THEN 'C&'||'I'
                                                      WHEN INSTR(TRIM(SDODR.ACCTSET), 'LSASV') > 0 OR INSTR(TRIM(SDODR.ACCTSET), 'LSEPG') > 0 THEN 'EPG'  ELSE 'CT'  END AS BU,
                                                   99993 ORGANIZATION_ID,
                                                   'SINGAPORE' SOURCE	,
                                                   ROWNUM ROW_NNUMBER_ID,
                                                   SYSDATE ROW_CREATION_DATE,
                                                   SYSDATE ROW_LAST_UPDATE_DATE
                                                FROM
                                                   SINGAPORE_DBO_OEORDD_BZ_ranked sdodr,
                                                   REFCFG_ARTICLES_DETAILS_TEMP rad
                                                WHERE 1=1
                                                   AND TRIM(SDODR.ITEM) = RAD.EIA_ITEM_CODE (+)
                                                   AND TRIM(SDODR.ITEM) IS NOT NULL
                                                   and rn = 1
                                                ORDER BY EIA_ITEM_CODE    ; */
      WITH vw AS (
                  SELECT DISTINCT
                        TRIM(SDODR.ITEM) INVENTORY_ITEM_ID,
                        TRIM(SDODR.ITEM) ITEM_CODE,
                        RAD.CODE_ORACLE EIA_ITEM_CODE,
                        null PRODUCT_TYPE,
                        DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'SGP',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
                        DECODE(RAD.DESIGN_OWNER_PLANT,NULL,'SGP',RAD.DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
                        DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'SGP',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
                        DECODE(RAD.SHIPPING_WAREHOUSE,NULL,'SGP',RAD.SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
                        DECODE(RAD.LONG_DESCRIPTION,NULL,trim(SDODR."DESC"),RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
                     --  RAD.EIA_ALLOCATION,
                     /*  CASE WHEN RAD.MASS_ALLOCATION is not null THEN RAD.MASS_ALLOCATION
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MON' OR TRIM(SDODR.CATEGORY) = 'NIDAMT'  OR TRIM(SDODR.CATEGORY) = 'NIDOPT' THEN 'M12'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MGP' OR TRIM(SDODR.CATEGORY) = 'LS7MAP' THEN 'M11'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MGP' THEN 'M17'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MEP' THEN 'M14'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MSS' THEN 'M16'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7ASP' OR TRIM(SDODR.CATEGORY) = 'LSASVJ'  OR TRIM(SDODR.CATEGORY) = 'LSASVO' OR TRIM(SDODR.CATEGORY) = 'LSAWAO' THEN 'E02'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7ALT' THEN 'E01'
                        ELSE 'B00'
                        END END END END END END END END AS EIA_ALLOCATION,*/
                        CASE WHEN RAD.MASS_ALLOCATION is not null THEN RAD.MASS_ALLOCATION
                        else td.valeur end as EIA_ALLOCATION,
                        RAD.CIRCUIT EIA_CIRCUIT_INFORMATION,
                        RAD.NATURE_PRODUIT ITEM_NATURE,
                        RAD.PRIX_TARIF LIST_PRICE,
                        RAD.POIDS WEIGHT,
                        RAD.CODE_DOUANE CUSTOMS_CODE,
                        RAD.EIA_STATISTIC_CODE,
                        RAD.EIA_STATISTIC_CODE_DESCRIPTION,
                        RAD.ITEM_LINE,
                        RAD.ITEM_LINE_DESCRIPTION,
                        RAD.ITEM_FAMILY,
                        RAD.ITEM_FAMILY_DESCRIPTION,
                        99993 ORGANIZATION_ID,
                        'SINGAPORE' SOURCE	,
                        TRIM(SDODR.CATEGORY) CATEGORY ,
                        TRIM(SDODR.ACCTSET) CNTLACCT,
                  /*    CASE WHEN pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE is not null THEN pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MON' OR TRIM(SDODR.CATEGORY) = 'NIDAMT'  OR TRIM(SDODR.CATEGORY) = 'NIDOPT' THEN 'MOTION'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MGP' OR TRIM(SDODR.CATEGORY) = 'LS7MAP' THEN 'CIMD MOTORS'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MGP' THEN 'GBMD'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MEP' THEN 'CIMD DRIVES'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7MSS' THEN 'SERVICE PDR'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7ASP' OR TRIM(SDODR.CATEGORY) = 'LSASVJ'  OR TRIM(SDODR.CATEGORY) = 'LSASVO' OR TRIM(SDODR.CATEGORY) = 'LSAWAO' THEN 'EPG Service'
                        ELSE CASE WHEN TRIM(SDODR.CATEGORY) = 'LS7ALT' THEN 'EPG LV'
                        ELSE 'CT'
                        END END END END END END END END AS SUBBU,*/
                        null ITEM_CREATION_DATE,
                        null ITEM_LAST_UPDATE_DATE,
                        null ITEM_STATUS   ,
                        'SINGAPORE' ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                        'SINGAPORE' ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                        '0000' ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                        '5100ZZZZ' ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                     --   RAD.EIA_ALLOCATION ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                        '0000' ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                        'SINGAPORE' ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                        'SINGAPORE' ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                        '0000' ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                        '4110ZZZZ' ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                     --  RAD.EIA_ALLOCATION ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                        '0000' ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6 ,
                        ROW_NUMBER() OVER (PARTITION BY TRIM(sdodr.ITEM) ORDER BY sdodr.ORDUNIQ DESC) AS rn
                  FROM
                        SINGAPORE_DBO_OEORDD_BZ sdodr
               LEFT JOIN    REFCFG_ARTICLES_DETAILS_TEMP rad
                        ON TRIM(SDODR.ITEM) = RAD.CODE_ALICE
                  LEFT JOIN    FILE_TRANSFORMATION_DATA_BZ td
                        ON td.cle = sdodr.category
                        AND td.type = 'EIA_ALLOCATION_SINGAPORE'
                     --  FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
                  WHERE 1=1
                     --   AND TRIM(SDODR.ITEM) = RAD.EIA_ITEM_CODE (+)
                        AND TRIM(SDODR.ITEM) IS NOT NULL
                     --   AND RAD.EIA_ALLOCATION = pgs.PRODUCT_GROUP_GL_CODE (+)

                  ORDER BY EIA_ITEM_CODE
                  )
      SELECT  distinct
            vw.INVENTORY_ITEM_ID ,
            vw.ITEM_CODE,
            vw.EIA_ITEM_CODE,
            vw.PRODUCT_TYPE,
            vw.ALICE_DESIGN_OWNER_PLANT,
            vw.EIA_DESIGN_OWNER_PLANT,
            vw.ALICE_SHIPPING_WAREHOUSE,
            vw.EIA_SHIPPING_WAREHOUSE,
            vw.LONG_DESCRIPTION,
            vw.EIA_ALLOCATION,
            vw.EIA_CIRCUIT_INFORMATION,
            vw.ITEM_NATURE,
            vw.LIST_PRICE,
            vw.WEIGHT,
            vw.CUSTOMS_CODE,
            vw.EIA_STATISTIC_CODE,
            vw.EIA_STATISTIC_CODE_DESCRIPTION,
            vw.ITEM_LINE,
            vw.ITEM_LINE_DESCRIPTION,
            vw.ITEM_FAMILY,
            vw.ITEM_FAMILY_DESCRIPTION,
            -- vb.BU_NEW BU,  modif le 29/09/25
            vb.VALEUR BU,
            vw.ORGANIZATION_ID,
            vw.SOURCE,
            ROWNUM ROW_NNUMBER_ID,
            SYSDATE ROW_CREATION_DATE,
            SYSDATE ROW_LAST_UPDATE_DATE,
            vw.CATEGORY,
            vw.CNTLACCT,
            pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE SUBBU,
            vw.ITEM_CREATION_DATE,
            vw.ITEM_LAST_UPDATE_DATE,
            vw.ITEM_STATUS,
            vw.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
            vw.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
            vw.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
            vw.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            vw.EIA_ALLOCATION ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            vw.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
            vw.ITEM_SALES_ACCOUNT_BU_GL_SGT1,
            vw.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
            vw.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
            vw.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            vw.EIA_ALLOCATION ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            vw.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
            101 CHART_OF_ACCOUNTS_ID
         FROM vw
               LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
               ON vw.EIA_ALLOCATION = pgs.PRODUCT_GROUP_GL_CODE (+)
               /*LEFT JOIN FILE_VAR_BU_bz vb
               ON pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE = VB.SUBBU (+)
               remplacer par la jointure avec FILE_TRANSFORMATION_DATA*/
               LEFT JOIN FILE_TRANSFORMATION_DATA_BZ vb
                     ON vb.cle = pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                     AND vb.type = 'BU_NEW'
               WHERE vw.rn =  1 ;
   --  g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'ITEM_DETAILS_SINGAPORE_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_SINGAPORE_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_Singapore_Proc;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_Brazil_Proc
   * DESCRIPTION :  Procedure cr�ation tables ITEM from BRAZIL
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
PROCEDURE Item_Details_Brazil_Proc
   IS v_procedure varchar2(100) := 'Item_Details_Brazil_Proc';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Not_Oracle_PROC';
   --  g_level     :='S';
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

   /* BRAZIL ITEMS */
--  g_table     :='ITEM_DETAILS_BRAZIL_TEMP';

--  g_etape := '513';
   g_table     := 'ITEM_DETAILS_BRAZIL_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_BRAZIL_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '514';
   g_table     := 'ITEM_DETAILS_BRAZIL_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT INTO ITEM_DETAILS_BRAZIL_TEMP
         WITH vw AS (
                  SELECT DISTINCT
                           TRIM(ob.SKU) INVENTORY_ITEM_ID,
                           TRIM(ob.SKU) ITEM_CODE,
                           null EIA_ITEM_CODE,
                           ob.FAM PRODUCT_TYPE,
                           'BRZ' ALICE_DESIGN_OWNER_PLANT,
                           'BRZ' EIA_DESIGN_OWNER_PLANT,
                           'BRZ' ALICE_SHIPPING_WAREHOUSE,
                           'BRZ' EIA_SHIPPING_WAREHOUSE,
                           ob."Descri��o material" LONG_DESCRIPTION,
                           'M18' EIA_ALLOCATION,
                           null EIA_CIRCUIT_INFORMATION,
                           null ITEM_NATURE,
                           null LIST_PRICE,
                           null WEIGHT,
                           null CUSTOMS_CODE,
                           null EIA_STATISTIC_CODE,
                           null EIA_STATISTIC_CODE_DESCRIPTION,
                           null ITEM_LINE,
                           null ITEM_LINE_DESCRIPTION,
                           null ITEM_FAMILY,
                           null ITEM_FAMILY_DESCRIPTION,
                           1699 ORGANIZATION_ID,
                           'BRAZIL' SOURCE	,
                           null  CATEGORY ,
                           null  CNTLACCT,
                           coalesce(pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE,'NOT AFFECTED')  SUBBU,
                           null ITEM_CREATION_DATE,
                           null ITEM_LAST_UPDATE_DATE,
                           'A' ITEM_STATUS  ,
                           '4061' ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                           '225' ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                           '0000' ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                           '5100ZZZZ' ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                           'M18' ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                           '0000' ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                           '4061' ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                           '225' ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                           '0000' ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                           '4110ZZZZ' ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                           'M18' ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                           '0000' ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
                           101 CHART_OF_ACCOUNTS_ID
                  FROM
                           file_order_brazil_BZ ob,
                           FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
                  WHERE 1=1
                     AND TRIM(ob.SKU) IS NOT NULL
                     AND pgs.PRODUCT_GROUP_GL_CODE = 'M18'
                  ORDER BY INVENTORY_ITEM_ID
                  )
      SELECT  distinct
            vw.INVENTORY_ITEM_ID ,
            vw.ITEM_CODE,
            vw.EIA_ITEM_CODE,
            vw.PRODUCT_TYPE,
            vw.ALICE_DESIGN_OWNER_PLANT,
            vw.EIA_DESIGN_OWNER_PLANT,
            vw.ALICE_SHIPPING_WAREHOUSE,
            vw.EIA_SHIPPING_WAREHOUSE,
            vw.LONG_DESCRIPTION,
            vw.EIA_ALLOCATION,
            vw.EIA_CIRCUIT_INFORMATION,
            vw.ITEM_NATURE,
            vw.LIST_PRICE,
            vw.WEIGHT,
            vw.CUSTOMS_CODE,
            vw.EIA_STATISTIC_CODE,
            vw.EIA_STATISTIC_CODE_DESCRIPTION,
            vw.ITEM_LINE,
            vw.ITEM_LINE_DESCRIPTION,
            vw.ITEM_FAMILY,
            vw.ITEM_FAMILY_DESCRIPTION,
            -- vb.BU_NEW BU, modif le 29/09/25
            vb.VALEUR BU,
            vw.ORGANIZATION_ID,
            vw.SOURCE,
            ROWNUM ROW_NNUMBER_ID,
            SYSDATE ROW_CREATION_DATE,
            SYSDATE ROW_LAST_UPDATE_DATE,
            vw.CATEGORY,
            vw.CNTLACCT,
            vw.SUBBU,
            vw.ITEM_CREATION_DATE,
            vw.ITEM_LAST_UPDATE_DATE,
            vw.ITEM_STATUS,
            vw.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
            vw.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
            vw.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
            vw.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            vw.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            vw.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
            vw.ITEM_SALES_ACCOUNT_BU_GL_SGT1,
            vw.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
            vw.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
            vw.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            vw.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            vw.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
            vw.CHART_OF_ACCOUNTS_ID
      FROM vw
         /*LEFT JOIN  FILE_VAR_BU_bz vb
         ON vw.SUBBU = VB.SUBBU*/
         LEFT JOIN FILE_TRANSFORMATION_DATA_BZ vb
            ON vb.cle = vw.SUBBU
            AND vb.type = 'BU_NEW'
      ;
--     g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   --  g_etape := '100';
   g_table     := 'ITEM_DETAILS_BRAZIL_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_BRAZIL_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_Brazil_Proc;

/****************************************************************************************
* PROCEDURE   :  Item_Details_Australia_Proc
* DESCRIPTION :  Procedure cr�ation tables ITEM from AUSTRALIA
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Item_Details_Australia_Proc
IS
   v_procedure       VARCHAR2(100) := 'Item_Details_Australia_Proc';
   v_date_deb_proc   TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Australia_Proc';
   --  g_level     :='S';
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

   /* BRAZIL ITEMS */
   -- g_table     :='ITEM_DETAILS_BRAZIL_TEMP';

   -- g_etape := '513';
   g_table     := 'Item_Details_Australia_temp';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE Item_Details_Australia_temp';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   -- g_etape := '514';
   g_table     := 'Item_Details_Australia_temp';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO Item_Details_Australia_temp
      WITH vw AS (
         SELECT DISTINCT
            TRIM(adodr.ITEM) INVENTORY_ITEM_ID,
            TRIM(adodr.ITEM) ITEM_CODE,
            RAD.CODE_ORACLE EIA_ITEM_CODE,
            DECODE(RAD.ALICE_DESIGN_OWNER_PLANT,NULL,'AUS',RAD.ALICE_DESIGN_OWNER_PLANT) ALICE_DESIGN_OWNER_PLANT,
            DECODE(RAD.DESIGN_OWNER_PLANT,NULL,'AUS',RAD.DESIGN_OWNER_PLANT) EIA_DESIGN_OWNER_PLANT,
            DECODE(RAD.ALICE_SHIPPING_WAREHOUSE,NULL,'AUS',RAD.ALICE_SHIPPING_WAREHOUSE) ALICE_SHIPPING_WAREHOUSE,
            DECODE(RAD.SHIPPING_WAREHOUSE,NULL,'AUS',RAD.SHIPPING_WAREHOUSE) EIA_SHIPPING_WAREHOUSE,
            DECODE(RAD.LONG_DESCRIPTION,NULL,trim(adodr."DESC"),RAD.LONG_DESCRIPTION) LONG_DESCRIPTION,
            case
               when RAD.MASS_ALLOCATION is not null then RAD.MASS_ALLOCATION
               else td.valeur
            end as EIA_ALLOCATION,
            RAD.CIRCUIT EIA_CIRCUIT_INFORMATION,
            RAD.NATURE_PRODUIT ITEM_NATURE,
            RAD.PRIX_TARIF LIST_PRICE,
            RAD.POIDS WEIGHT,
            RAD.CODE_DOUANE CUSTOMS_CODE,
            RAD.EIA_STATISTIC_CODE,
            RAD.EIA_STATISTIC_CODE_DESCRIPTION,
            RAD.ITEM_LINE,
            RAD.ITEM_LINE_DESCRIPTION,
            RAD.ITEM_FAMILY,
            RAD.ITEM_FAMILY_DESCRIPTION,
            99995 ORGANIZATION_ID,
            'AUSTRALIA' SOURCE,
            TRIM(adodr.CATEGORY) CATEGORY,
            TRIM(adodr.ACCTSET) CNTLACCT,
            'AUSTRALIA' ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
            'AUSTRALIA' ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
            '0000' ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
            '5100ZZZZ' ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            '0000' ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
            'AUSTRALIA' ITEM_SALES_ACCOUNT_BU_GL_SGT1,
            'AUSTRALIA' ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
            '0000' ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
            '4110ZZZZ' ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            '0000' ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
            ROW_NUMBER() OVER (PARTITION BY TRIM(adodr.ITEM) ORDER BY adodr.ORDUNIQ DESC) AS rn,
            adi.VALUE   ITEM_CATEGORY
         FROM AUSTRALIA_DBO_OEORDD_BZ adodr
         LEFT JOIN REFCFG_ARTICLES_DETAILS_TEMP rad
            ON TRIM(adodr.ITEM) = RAD.CODE_ALICE
         LEFT JOIN AUSTRALIA_DBO_ICITEMO_BZ adi
            ON adi.OPTFIELD = 'ITEMCATEGORY'
            AND adi.ITEMNO = TRIM(adodr.ITEM)
         LEFT JOIN FILE_TRANSFORMATION_DATA_BZ td
            ON td.cle = adi.VALUE
            AND td.type = 'EIA_ALLOCATION_SINGAPORE'
         WHERE TRIM(adodr.ITEM) IS NOT NULL
         ORDER BY EIA_ITEM_CODE
      )
      SELECT DISTINCT
         vw.INVENTORY_ITEM_ID ,
         vw.ITEM_CODE,
         vw.EIA_ITEM_CODE,
         CAST(NULL AS VARCHAR2(50)) AS PRODUCT_TYPE,
         vw.ALICE_DESIGN_OWNER_PLANT,
         vw.EIA_DESIGN_OWNER_PLANT,
         vw.ALICE_SHIPPING_WAREHOUSE,
         vw.EIA_SHIPPING_WAREHOUSE,
         vw.LONG_DESCRIPTION,
         vw.EIA_ALLOCATION,
         vw.EIA_CIRCUIT_INFORMATION,
         vw.ITEM_NATURE,
         vw.LIST_PRICE,
         vw.WEIGHT,
         vw.CUSTOMS_CODE,
         vw.EIA_STATISTIC_CODE,
         vw.EIA_STATISTIC_CODE_DESCRIPTION,
         vw.ITEM_LINE,
         vw.ITEM_LINE_DESCRIPTION,
         vw.ITEM_FAMILY,
         vw.ITEM_FAMILY_DESCRIPTION,
         vb.VALEUR BU,
         vw.ORGANIZATION_ID,
         vw.SOURCE,
         ROWNUM ROW_NNUMBER_ID,
         SYSDATE ROW_CREATION_DATE,
         SYSDATE ROW_LAST_UPDATE_DATE,
         vw.CATEGORY,
         vw.CNTLACCT,
         pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE SUBBU,
         CAST(NULL AS DATE) AS ITEM_CREATION_DATE,
         CAST(NULL AS DATE) AS ITEM_LAST_UPDATE_DATE,
         CAST(NULL AS VARCHAR2(20)) AS ITEM_STATUS,
         vw.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
         vw.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
         vw.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
         vw.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
         vw.EIA_ALLOCATION ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
         vw.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
         vw.ITEM_SALES_ACCOUNT_BU_GL_SGT1,
         vw.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
         vw.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
         vw.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
         vw.EIA_ALLOCATION ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
         vw.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
         101 CHART_OF_ACCOUNTS_ID,
         vw.ITEM_CATEGORY
      FROM vw
      LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ pgs
         ON vw.EIA_ALLOCATION = pgs.PRODUCT_GROUP_GL_CODE
      LEFT JOIN FILE_TRANSFORMATION_DATA_BZ vb
         ON vb.cle = pgs.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
         AND vb.type = 'BU_NEW'
      WHERE vw.rn = 1;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   --  g_etape := '100';
   g_table     := 'ITEM_DETAILS_Australia_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('Item_Details_Australia_temp');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_Australia_Proc;


   /****************************************************************************************
   * PROCEDURE   :  Item_Details_Not_Oracle_PROC
   * DESCRIPTION :  Procedure cr�ation tables ITEM for NOT_ORACLE
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/
PROCEDURE Item_Details_Not_Oracle_PROC
   IS v_procedure varchar2(100) := 'Item_Details_Not_Oracle_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Not_Oracle_PROC';
   --  g_level     :='S';
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

   Item_Details_China_Proc;
   Item_Details_India_Proc;
   Item_Details_Korea_Proc;
   Item_Details_Singapore_Proc;
   Item_Details_Brazil_Proc;
   Item_Details_Australia_Proc;

   /* Merge CHINA INDIA, KOREA SINGAPORE Items */
--  g_table     :='ITEM_DETAILS_NOT_ORACLE_TEMP';

--  g_etape := '514';
   g_table     := 'ITEM_DETAILS_NOT_ORACLE_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_NOT_ORACLE_TEMP';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

--  g_etape := '515';
   g_table     := 'ITEM_DETAILS_NOT_ORACLE_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
            insert into ITEM_DETAILS_NOT_ORACLE_TEMP
               select * from ITEM_DETAILS_CHINA_TEMP
            union
               select * from ITEM_DETAILS_INDIA_TEMP
            union
               select idkt.*
               from
                  ITEM_DETAILS_KOREA_TEMP idkt,
                  ITEM_DETAILS_CHINA_TEMP idct
               where 1=1
               and idkt.ITEM_CODE = idct.ITEM_CODE (+)
               and idct.ITEM_CODE is NULL
            union
               -- ajout POTC cas particulier d'article CHINA utilis� par la KOREA
               select
                     idkt.INVENTORY_ITEM_ID,
                     idkt.ITEM_CODE,
                     idkt.EIA_ITEM_CODE,
                     idct.PRODUCT_TYPE,
                     idkt.ALICE_DESIGN_OWNER_PLANT,
                     idkt.EIA_DESIGN_OWNER_PLANT,
                     idkt.ALICE_SHIPPING_WAREHOUSE,
                     idkt.EIA_SHIPPING_WAREHOUSE,
                     idct.LONG_DESCRIPTION,
                     idct.EIA_ALLOCATION,
                     idct.EIA_CIRCUIT_INFORMATION,
                     idct.ITEM_NATURE,
                     idct.LIST_PRICE,
                     idct.WEIGHT,
                     idct.CUSTOMS_CODE,
                     idct.EIA_STATISTIC_CODE,
                     idct.EIA_STATISTIC_CODE_DESCRIPTION,
                     idct.ITEM_LINE,
                     idct.ITEM_LINE_DESCRIPTION,
                     idct.ITEM_FAMILY,
                     idct.ITEM_FAMILY_DESCRIPTION,
                     idct.BU,
                     idkt.ORGANIZATION_ID,
                     idct.SOURCE,
                     idct.ROW_NUMBER_ID,
                     idct.ROW_CREATION_DATE,
                     idct.ROW_LAST_UPDATE_DATE,
                     idct.CATEGORY ,
                     idct.CNTLACCT,
                     idct.SUBBU,
                     idct.ITEM_CREATION_DATE,
                     idct.ITEM_LAST_UPDATE_DATE,
                     idct.ITEM_STATUS  ,
                     idkt.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                     idkt.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                     idkt.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                     idkt.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                     idct.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                     idkt.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                     idkt.ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                     idkt.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                     idkt.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                     idkt.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                     idct.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                     idkt.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
                     idkt.CHART_OF_ACCOUNTS_ID
               from
                  ITEM_DETAILS_KOREA_TEMP idkt,
                  ITEM_DETAILS_CHINA_TEMP idct
               where 1=1
               and idkt.ITEM_CODE = idct.ITEM_CODE (+)
               and idct.ITEM_CODE is NOT NULL
            union
               select * from ITEM_DETAILS_SINGAPORE_TEMP
            union
               select * from ITEM_DETAILS_BRAZIL_TEMP
            union
               select
                  INVENTORY_ITEM_ID,
                  ITEM_CODE,
                  EIA_ITEM_CODE,
                  PRODUCT_TYPE,
                  ALICE_DESIGN_OWNER_PLANT,
                  EIA_DESIGN_OWNER_PLANT,
                  ALICE_SHIPPING_WAREHOUSE,
                  EIA_SHIPPING_WAREHOUSE,
                  LONG_DESCRIPTION,
                  EIA_ALLOCATION,
                  EIA_CIRCUIT_INFORMATION,
                  ITEM_NATURE,
                  LIST_PRICE,
                  WEIGHT,
                  CUSTOMS_CODE,
                  EIA_STATISTIC_CODE,
                  EIA_STATISTIC_CODE_DESCRIPTION,
                  ITEM_LINE,
                  ITEM_LINE_DESCRIPTION,
                  ITEM_FAMILY,
                  ITEM_FAMILY_DESCRIPTION,
                  BU,
                  ORGANIZATION_ID,
                  SOURCE,
                  ROWNUM ROW_NNUMBER_ID,
                  ROW_CREATION_DATE,
                  ROW_LAST_UPDATE_DATE,
                  CATEGORY,
                  CNTLACCT,
                  SUBBU,
                  ITEM_CREATION_DATE,
                  ITEM_LAST_UPDATE_DATE,
                  ITEM_STATUS,
                  ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
                  ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
                  ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
                  ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                  EIA_ALLOCATION ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                  ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
                  ITEM_SALES_ACCOUNT_BU_GL_SGT1,
                  ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
                  ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
                  ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                  EIA_ALLOCATION ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                  ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
                  CHART_OF_ACCOUNTS_ID
               from ITEM_DETAILS_Australia_TEMP
            ;
   --g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

--  g_etape := '100';
   g_table     := 'ITEM_DETAILS_NOT_ORACLE_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_NOT_ORACLE_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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
         g_etape     := $$plsql_line + 1  || ' - num error line'   ;
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc;
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';
         Write_Log_PROC;

   END Item_Details_Not_Oracle_PROC;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_Oracle_PROC
   * DESCRIPTION :  Create Item Details Oracle table based on STEPH
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   *  ---------------------------------------------------------------------------
   *  Date     Version  Nom            Description de l'intervention
   *  -------- -------- -------------- ------------------------------------------
   *  02/07/24  1.0.3   JKLE           Ajout NVL sur descriptions ITEM
   ****************************************************************************************/
   PROCEDURE Item_Details_Oracle_PROC
   IS v_procedure varchar2(100) := 'Item_Details_Oracle_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_Oracle_PROC';
   -- g_level     :='S';
   -- g_date_deb  :=sysdate;
   --  g_status    :=NULL;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;
   --  g_table     := 'ITEM_DETAILS_ORACLE';

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   -- g_etape := '601';
   g_table     := 'ITEM_DETAILS_ORACLE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_ORACLE';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   -- g_etape := '602';
   g_table     := 'ITEM_DETAILS_ORACLE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT /*+ PARALLEL(ITEM_DETAILS_ORACLE, 6) */ INTO ITEM_DETAILS_ORACLE
      WITH person_name_cte AS (
   SELECT /*+ INDEX(pap STEPH_APPS_PER_ALL_PEOPLE_F_bz_PK) */
         pap.person_id,
         pap.global_name
   FROM steph_apps_per_all_people_f_bz pap
   WHERE pap.effective_start_date = (
      SELECT MAX(pap2.effective_start_date)
      FROM steph_apps_per_all_people_f_bz pap2
      WHERE pap2.person_id = pap.person_id
   )
)
SELECT --DISTINCT
               msi.INVENTORY_ITEM_ID                                                                        INVENTORY_ITEM_ID,
               msi.ORGANIZATION_ID                                                                          ORGANIZATION_ID,
               msi.SEGMENT1                                                                                 ITEM_CODE,
               msi.LAST_UPDATE_DATE                                                                         LAST_UPDATE_DATE,
               fum.user_name                                                                                LAST_UPDATED_BY_NAME,
               msi.CREATION_DATE                                                                            CREATION_DATE,
               fuc.user_name                                                                                CREATED_BY_NAME,
               NVL(msitf.DESCRIPTION, msi.description)                                                      DESCRIPTION_FRENCH,
               NVL(msitf.LONG_DESCRIPTION, msi.description)                                                 LONG_DESCRIPTION_FRENCH,
               NVL(msite.DESCRIPTION, msi.description)                                                      DESCRIPTION_ENGLISH,
               NVL(msite.LONG_DESCRIPTION, msi.description)                                                 LONG_DESCRIPTION_ENGLISH,
               icr.LEGACY_ITEM_LS                                                                           LEGACY_ITEM_LS,
               icr.LISA_ITEM_LS                                                                             LISA_ITEM_LS,
               msi.INVENTORY_ITEM_STATUS_CODE                                                               INVENTORY_ITEM_STATUS_CODE,
               msi.EXPENSE_ACCOUNT                                                                          EXPENSE_ACCOUNT_ID,
               msi.PRIMARY_UOM_CODE                                                                         PRIMARY_UOM_CODE,
               msi.PLANNER_CODE                                                                             PLANNER_CODE,
               --LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(msi.buyer_id)                                     BUYER_NAME,
               (SELECT global_name FROM person_name_cte WHERE person_id = msi.buyer_id FETCH FIRST 1 ROWS ONLY) AS BUYER_NAME, -- 04/09/25 test de replacement de la fonction pour les hints
               msi.ITEM_TYPE                                                                                ITEM_TYPE,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE1 else NULL end )             DFF_DRAWING_REVISION,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE2 else NULL end)              DFF_PRICE_CHANGE_DATE,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE3 else NULL end) DFF_DRAWING_NUMBER,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE5 else NULL end) DFF_GLOBAL_REF_NUMBER,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE6 else NULL end) DFF_GLOBAL_REF_DWG_NUMBER,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE7 else NULL end) DFF_GLOBAL_REF_DWG_NUMBER_REV,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE8 else NULL end) DFF_ITEM_CLASSIFICATION,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE9 else NULL end) DFF_ORIGINATOR,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE10 else NULL end) DFF_DMCA_INSPECTION,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE11 else NULL end) DFF_PURCHASE_NOTE_REVISION,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE14 else NULL end) DFF_PACKING_QUANTITY,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE15 else NULL end) DFF_INVOICE_UOM,
               ( case  when msi.ATTRIBUTE_CATEGORY =  'XMS' then msi.ATTRIBUTE12 else NULL end) DFF_TRANSFERRED_TO_FUZHOU,
               msi.ITEM_CATALOG_GROUP_ID ITEM_CATALOG_GROUP_ID,
               micg.SEGMENT1 ITEM_ICC,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE1 else NULL end) DFF_COMMODITY_MANAGER_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE2 else NULL end) DFF_CENTRAL_SPEC_NAME_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE3 else NULL end) DFF_CENTRAL_SPEC_REV_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE4 else NULL end) DFF_DRI_CODES_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE5 else NULL end) DFF_UNSPSC_CODES_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE6 else NULL end) DFF_CENTRAL_HOMO_FLAG_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE7 else NULL end) DFF_INTRASTAT_CLASS_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE8 else NULL end) DFF_QUALIFICTION_REQ_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then Substr(micg.Attribute9 ,1,1) else NULL end) DFF_COMMODITY_CTRL,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then Substr(micg.Attribute9 ,2,4) else NULL end) DFF_LEADTIME_TYPE,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE10 else NULL end) DFF_COMMODITY_PRICE_CONTROL,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE11 else NULL end) DFF_ITEM_DUPLICATE_CHECK_LS,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE12 else NULL end) DFF_ICC_UOM,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE13 else NULL end) DFF_EXPENSE_NATURAL_ACCOUNT,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE14 else NULL end) DFF_SALES_NATURAL_ACCOUNT,
               ( case  when micg.ATTRIBUTE_CATEGORY =  'LEROY SOMER' then micg.ATTRIBUTE15 else NULL end) DFF_DRI_UOM_CONVERSION,
               micgp.SEGMENT1 PARENT_ICC,
               msi.SALES_ACCOUNT SALES_ACCOUNT_ID,
               ic.EIA_ALLOCATION_CATEGORY_VALUE EIA_ALLOCATION,
               ic.EIA_CIRCUIT_INFORMATION_CATEGORY_VALUE EIA_CIRCUIT_INFORMATION,
               ic.EIA_COUNTRY_OF_ORIGIN_CATEGORY_VALUE EIA_COUNTRY_OF_ORIGIN,
               ic.EIA_DESIGN_OWNER_PLANT_CATEGORY_VALUE EIA_DESIGN_OWNER_PLANT,
               ic.EIA_EBUSINESS_TAX_CATEGORY_VALUE EIA_EBUSINESS_TAX,
               ic.EIA_PRODUCT_STATISTICS_CODE_CATEGORY_VALUE EIA_PRODUCT_STATISTICS_CODE,
               -- ic.EIA_SHIPPING_WAREHOUSE_CATEGORY_VALUE EIA_SHIPPING_WAREHOUSE,
               substr(ic.EIA_SHIPPING_WAREHOUSE_CATEGORY_VALUE, -3) EIA_SHIPPING_WAREHOUSE,
               ic.EIA_STATISTIC_CODE_CATEGORY_VALUE EIA_STATISTIC_CODE,
               --clf.EIA_STATISTIC_CODE_DESCRIPTION EIA_STATISTIC_CODE_DESCRIPTION,
               rics.LIBELLE as EIA_STATISTIC_CODE_DESCRIPTION,
               substr(EIA_ITEM_STRUCTURE_CATEGORY_VALUE, 1, 3) ITEM_NATURE,
               substr(EIA_ITEM_STRUCTURE_CATEGORY_VALUE, 5, 2) ITEM_FAMILY,
               --clf.ITEM_FAMILY_DESCRIPTION ITEM_FAMILY_DESCRIPTION,
               riifp.LIBELLE ITEM_FAMILY_DESCRIPTION,
               substr(EIA_ITEM_STRUCTURE_CATEGORY_VALUE , 8, 2) ITEM_LINE,
               --clf.ITEM_LINE_DESCRIPTION ITEM_LINE_DESCRIPTION,
               riil.NOM as ITEM_LINE_DESCRIPTION,
               rad.PRIX_TARIF LIST_PRICE,
               rad.POIDS WEIGHT,
               substr(ic.EMR_TARIFF_CODE_CATEGORY_VALUE, 4, 40) CUSTOMS_CODE,
               rad.COO COO,
               'ORACLE' SOURCE,
               ROWNUM ROW_NUMBER_ID,
               SYSDATE ROW_CREATION_DATE,
               SYSDATE ROW_LAST_UPDATE_DATE,
               gadte.SEGMENT1 ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1, -- gadte.BU_GL_SEGMENT1 ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
               gadte.SEGMENT2 ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2, -- gadte.LOCATION_GL_SEGMENT2 ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
               gadte.SEGMENT3 ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3, -- gadte.DEPARTMENT_GL_SEGMENT3 ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
               gadte.SEGMENT4 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4, -- gadte.NATURAL_ACCOUNT_GL_SEGMENT4 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               gadte.SEGMENT5 ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5, -- gadte.PRODUCT_GROUP_GL_SEGMENT5 ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               NULL ITEM_EXPENSE_ACCOUNT_BU, -- vbe.BU_NEW ITEM_EXPENSE_ACCOUNT_BU,
               gadte.SEGMENT6 ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6, --gadte.INTERCOMPANY_GL_SEGMENT6 ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
               gadts.SEGMENT1 ITEM_SALES_ACCOUNT_BU_GL_SGT1, -- gadts.BU_GL_SEGMENT1 ITEM_SALES_ACCOUNT_BU_GL_SGT1,
               gadts.SEGMENT2 ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2, -- gadts.LOCATION_GL_SEGMENT2 ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
               gadts.SEGMENT3 ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3, -- gadts.DEPARTMENT_GL_SEGMENT3 ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
               gadts.SEGMENT4 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4, -- gadts.NATURAL_ACCOUNT_GL_SEGMENT4 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               gadts.SEGMENT5 ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5, -- gadts.PRODUCT_GROUP_GL_SEGMENT5 ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               NULL ITEM_SALES_ACCOUNT_BU, -- vbs.BU_NEW ITEM_SALES_ACCOUNT_BU,
               gadts.SEGMENT6 ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6, --gadts.INTERCOMPANY_GL_SEGMENT6 ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
               gadts.CHART_OF_ACCOUNTS_ID,
               ic.LS_ACIM_PO_CATEGORY_CATEGORY_VALUE LS_ACIM_PO_CATEGORY,
               msi.planning_make_buy_code,
               msi.WIP_SUPPLY_TYPE,
               msi.MINIMUM_ORDER_QUANTITY,
               msi.FIXED_LOT_MULTIPLIER,
               msi.MAXIMUM_ORDER_QUANTITY,
               msi.ORDER_COST,
               msi.CARRYING_COST,
               msi.FULL_LEAD_TIME,
               msi.PREPROCESSING_LEAD_TIME,
               flv1.MEANING planning_make_buy_meaning,
               flv2.MEANING WIP_SUPPLY_TYPE_MEANING,
               flv3.MEANING INVENTORY_PLANNING_MEANING,
               msi.POSTPROCESSING_LEAD_TIME,
               msi.INVENTORY_PLANNING_CODE,
               ic.EIA_SUB_CONTRACTING_PLANT_CATEGORY_VALUE EIA_SUB_CONTRACTING_PLANT

      FROM STEPH_APPS_MTL_SYSTEM_ITEMS_B_bz msi
      LEFT OUTER JOIN STEPH_APPS_FND_USER_bz fum
            ON msi.last_updated_by = fum.user_id
      LEFT OUTER JOIN STEPH_APPS_FND_USER_bz fuc
            ON msi.created_by = fuc.user_id
      LEFT OUTER JOIN STEPH_APPS_MTL_SYSTEM_ITEMS_TL_bz msitf
            ON ( msi.organization_id    = msitf.organization_id
            AND msi.inventory_item_id   = msitf.inventory_item_id
            AND msitf.language = 'F' )
      LEFT OUTER JOIN STEPH_APPS_MTL_SYSTEM_ITEMS_TL_bz msite
            ON ( msi.organization_id    = msite.organization_id
            AND msi.inventory_item_id   = msite.inventory_item_id
            AND msite.language = 'US' )
      LEFT OUTER JOIN STEPH_ITEM_CROSS_REFERENCES_TEMP icr
            ON ( msi.organization_id  = icr.organization_id
            AND msi.inventory_item_id = icr.inventory_item_id )
      LEFT OUTER JOIN STEPH_APPS_MTL_ITEM_CATALOG_GROUPS_B_bz micg
            ON 	msi.item_catalog_group_id = micg.item_catalog_group_id
      LEFT OUTER JOIN STEPH_APPS_MTL_ITEM_CATALOG_GROUPS_B_bz micgp
            ON 	micg.parent_catalog_group_id = micgp.item_catalog_group_id
      LEFT OUTER JOIN STEPH_ITEM_CATEGORIES_TEMP ic
            ON 	( msi.organization_id    = ic.organization_id
            AND msi.inventory_item_id   = ic.inventory_item_id )
      /*LEFT OUTER JOIN REFCFG_CODESTAT_LINE_FAMILY_TEMP clf
            ON ( ic.EIA_STATISTIC_CODE_CATEGORY_VALUE = clf.eia_statistic_code
            AND substr(ic.EIA_ITEM_STRUCTURE_CATEGORY_VALUE , 5, 2) = clf.item_family
            AND substr(ic.EIA_ITEM_STRUCTURE_CATEGORY_VALUE , 8, 2) = clf.item_line  )*/

         left join REFCFG_INTERFACECOM_INTERFACECOM_CODES_STAT_PRODUIT_BZ rics

            on rics.CODE_STAT =ic.EIA_STATISTIC_CODE_CATEGORY_VALUE

      left join REFCFG_INTERFACECOM_INTERFACECOM_FAMILLES_PRODUIT_BZ riifp

            on riifp.NOFAMILLE=substr(ic.EIA_ITEM_STRUCTURE_CATEGORY_VALUE , 5, 2)

      left join REFCFG_INTERFACECOM_INTERFACECOM_LIGNEPRODUIT_BZ riil

            on riil.NOLIGNE=substr(ic.EIA_ITEM_STRUCTURE_CATEGORY_VALUE , 8, 2)

      LEFT OUTER JOIN REFCFG_ARTICLES_DETAILS_TEMP rad
            ON 	msi.segment1 = rad.CODE_ORACLE
      LEFT OUTER JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_bz gadte
            ON 	msi.EXPENSE_ACCOUNT = gadte.code_combination_id
      LEFT OUTER JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_bz gadts
            ON 	msi.SALES_ACCOUNT = gadts.code_combination_id

      LEFT JOIN steph_apps_fnd_lookup_values_bz flv1 ON
            flv1.lookup_type = 'MTL_PLANNING_MAKE_BUY'
            and flv1.language = 'US'
         and flv1.lookup_code = planning_make_buy_code

      LEFT JOIN steph_apps_fnd_lookup_values_bz flv2 ON
            flv2.lookup_type = 'WIP_SUPPLY'
            and flv2.language = 'US'
            and flv2.lookup_code = wip_supply_type

      LEFT JOIN steph_apps_fnd_lookup_values_bz flv3 ON
            flv3.lookup_type = 'EGO_INVENTORY_PLANNING_CODE'
            and flv3.language = 'US'
            and flv3.lookup_code = INVENTORY_PLANNING_CODE
      ;

   -- g_etape := '90';
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'ITEM_DETAILS_ORACLE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_ORACLE');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_Oracle_PROC;

   /****************************************************************************************
   * PROCEDURE   :  Item_Details_All_PROC
   * DESCRIPTION :  Create Item Details All table based on Item_Drtails_Asia + Item_Details_Oracle en excluant les level XMS et LVO
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   *  ---------------------------------------------------------------------------
   *  Date     Version  Nom            Description de l'intervention
   *  -------- -------- -------------- ------------------------------------------
   *  02/07/24  1.0.3   JKLE           Ajout NVL sur descriptions ITEM
   ****************************************************************************************/
   PROCEDURE Item_Details_All_PROC
   IS v_procedure varchar2(100) := 'Item_Details_All_PROC';
      v_date_deb_proc  TIMESTAMP := sysdate;
   BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   --  g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.Item_Details_All_PROC';
   -- g_level     :='S';
   --  g_date_deb  :=sysdate;
   -- g_status    :=NULL;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;
   --  g_table     := 'ITEM_DETAILS_ALL';

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   --  g_etape := '701';
   g_table     := 'ITEM_DETAILS_ALL';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'TRUNCATE TABLE ITEM_DETAILS_ALL';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   --  g_etape := '702';
   g_table     := 'ITEM_DETAILS_ALL';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT /*+ PARALLEL(ITEM_DETAILS_ALL, 6) */  INTO ITEM_DETAILS_ALL
      SELECT -- distinct
               TO_CHAR(ito.INVENTORY_ITEM_ID) INVENTORY_ITEM_IDA,
               ito.ORGANIZATION_ID ORGANIZATION_ID,
               ito.ITEM_CODE ITEM_CODE,
               itoxms.LAST_UPDATE_DATE LAST_UPDATE_DATE_XMS,
               itoxms.LAST_UPDATED_BY_NAME LAST_UPDATED_BY_NAME_XMS,
               itoxms.CREATION_DATE CREATION_DATE_XMS,
               itoxms.CREATED_BY_NAME CREATED_BY_NAME_XMS,
               itoxms.INVENTORY_ITEM_STATUS_CODE INVENTORY_ITEM_STATUS_CODE_XMS,
               sidt.OOD_ORGANIZATION_CODE IO_CODE,
               TRIM(sidt.HOU_NAME) OU_NAME,
               ito.LAST_UPDATE_DATE LAST_UPDATE_DATE,
               ito.LAST_UPDATED_BY_NAME LAST_UPDATED_BY_NAME,
               ito.CREATION_DATE CREATION_DATE,
               ito.CREATED_BY_NAME CREATED_BY_NAME,
               ito.DESCRIPTION_FRENCH DESCRIPTION_FRENCH,
               ito.LONG_DESCRIPTION_FRENCH LONG_DESCRIPTION_FRENCH,
               ito.DESCRIPTION_ENGLISH DESCRIPTION_ENGLISH,
               ito.LONG_DESCRIPTION_ENGLISH LONG_DESCRIPTION_ENGLISH,
               ito.LEGACY_ITEM_LS LEGACY_ITEM_LS,
               itolvo.LISA_ITEM_LS LISA_ITEM_LS,
               ito.INVENTORY_ITEM_STATUS_CODE INVENTORY_ITEM_STATUS_CODE,
               ito.EXPENSE_ACCOUNT_ID EXPENSE_ACCOUNT_ID,
               ito.PRIMARY_UOM_CODE PRIMARY_UOM_CODE,
               ito.PLANNER_CODE PLANNER_CODE,
               ito.BUYER_NAME BUYER_NAME_IO,
               ito.ITEM_TYPE ITEM_TYPE,
               itoxms.DFF_DRAWING_REVISION DFF_DRAWING_REVISION,
               itoxms.DFF_PRICE_CHANGE_DATE DFF_PRICE_CHANGE_DATE,
               itoxms.DFF_DRAWING_NUMBER DFF_DRAWING_NUMBER,
               itoxms.DFF_GLOBAL_REF_NUMBER DFF_GLOBAL_REF_NUMBER,
               itoxms.DFF_GLOBAL_REF_DWG_NUMBER DFF_GLOBAL_REF_DWG_NUMBER,
               itoxms.DFF_GLOBAL_REF_DWG_NUMBER_REV DFF_GLOBAL_REF_DWG_NUMBER_REV,
               itoxms.DFF_ITEM_CLASSIFICATION DFF_ITEM_CLASSIFICATION,
               itoxms.DFF_ORIGINATOR DFF_ORIGINATOR,
               itoxms.DFF_DMCA_INSPECTION DFF_DMCA_INSPECTION,
               itoxms.DFF_PURCHASE_NOTE_REVISION DFF_PURCHASE_NOTE_REVISION,
               itoxms.DFF_PACKING_QUANTITY DFF_PACKING_QUANTITY,
               itoxms.DFF_INVOICE_UOM DFF_INVOICE_UOM,
               itoxms.DFF_TRANSFERRED_TO_FUZHOU DFF_TRANSFERRED_TO_FUZHOU,
               ito.ITEM_CATALOG_GROUP_ID ITEM_CATALOG_GROUP_ID,
               ito.ITEM_ICC ITEM_ICC,
               ito.DFF_COMMODITY_MANAGER_LS DFF_COMMODITY_MANAGER_LS,
               ito.DFF_CENTRAL_SPEC_NAME_LS DFF_CENTRAL_SPEC_NAME_LS,
               ito.DFF_CENTRAL_SPEC_REV_LS DFF_CENTRAL_SPEC_REV_LS,
               ito.DFF_DRI_CODES_LS DFF_DRI_CODES_LS,
               ito.DFF_UNSPSC_CODES_LS DFF_UNSPSC_CODES_LS,
               ito.DFF_CENTRAL_HOMO_FLAG_LS DFF_CENTRAL_HOMO_FLAG_LS,
               ito.DFF_INTRASTAT_CLASS_LS DFF_INTRASTAT_CLASS_LS,
               ito.DFF_QUALIFICTION_REQ_LS DFF_QUALIFICTION_REQ_LS,
               ito.DFF_COMMODITY_CTRL DFF_COMMODITY_CTRL,
               ito.DFF_LEADTIME_TYPE DFF_LEADTIME_TYPE,
               ito.DFF_COMMODITY_PRICE_CONTROL DFF_COMMODITY_PRICE_CONTROL,
               ito.DFF_ITEM_DUPLICATE_CHECK_LS DFF_ITEM_DUPLICATE_CHECK_LS,
               ito.DFF_ICC_UOM DFF_ICC_UOM,
               ito.DFF_EXPENSE_NATURAL_ACCOUNT DFF_EXPENSE_NATURAL_ACCOUNT,
               ito.DFF_SALES_NATURAL_ACCOUNT DFF_SALES_NATURAL_ACCOUNT,
               ito.DFF_DRI_UOM_CONVERSION DFF_DRI_UOM_CONVERSION,
               ito.PARENT_ICC PARENT_ICC,
               ito.SALES_ACCOUNT_ID SALES_ACCOUNT_ID,
               ito.EIA_ALLOCATION EIA_ALLOCATION,
               ito.EIA_CIRCUIT_INFORMATION EIA_CIRCUIT_INFORMATION,
               ito.EIA_COUNTRY_OF_ORIGIN EIA_COUNTRY_OF_ORIGIN,
               ito.EIA_DESIGN_OWNER_PLANT EIA_DESIGN_OWNER_PLANT,
               ito.EIA_EBUSINESS_TAX EIA_EBUSINESS_TAX,
               ito.EIA_PRODUCT_STATISTICS_CODE EIA_PRODUCT_STATISTICS_CODE,
               -- substr(ito.EIA_SHIPPING_WAREHOUSE, -3) EIA_SHIPPING_WAREHOUSE,
               ito.EIA_SHIPPING_WAREHOUSE EIA_SHIPPING_WAREHOUSE,
               ito.EIA_STATISTIC_CODE EIA_STATISTIC_CODE,
               ito.EIA_STATISTIC_CODE_DESCRIPTION EIA_STATISTIC_CODE_DESCRIPTION,
               ito.ITEM_NATURE ITEM_NATURE,
               ito.ITEM_FAMILY ITEM_FAMILY,
               ito.ITEM_FAMILY_DESCRIPTION ITEM_FAMILY_DESCRIPTION,
               ito.ITEM_LINE ITEM_LINE,
               ito.ITEM_LINE_DESCRIPTION ITEM_LINE_DESCRIPTION,
               ito.LIST_PRICE LIST_PRICE,
               ito.WEIGHT WEIGHT,
               ito.CUSTOMS_CODE CUSTOMS_CODE,
               ito.COO COO,
               ito.ITEM_CODE EIA_ITEM_CODE,
               itolvo.EIA_ALLOCATION EIA_ALLOCATION_LVO, --vs.SUBBU EIA_CATEGORY_ITEM_SUBBU,
               itoxms.EIA_ALLOCATION EIA_ALLOCATION_XMS, --vb.BU_NEW  EIA_CATEGORY_ITEM_BU,
               ito.SOURCE,
               ROWNUM ROW_NUMBER_ID,
               SYSDATE ROW_CREATION_DATE,
               SYSDATE ROW_LAST_UPDATE_DATE,
               ito.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1 ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
               itolvo.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1 ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1_LVO, --ito.ITEM_EXPENSE_ACCOUNT_BU_GL_RGP_A ITEM_EXPENSE_ACCOUNT_BU_GL_RGP_A,
               itoxms.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1 ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1_XMS,  --ito.ITEM_EXPENSE_ACCOUNT_BU_GL_RGP_V ITEM_EXPENSE_ACCOUNT_BU_GL_RGP_V,
               ito.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2 ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
               itolvo.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2 ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2_LVO, --ito.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_RGP_A ITEM_EXPENSE_ACCOUNT_LOCATION_GL_RGP_A,
               itoxms.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2 ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2_XMS, --ito.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_RGP_V ITEM_EXPENSE_ACCOUNT_LOCATION_GL_RGP_V,
               ito.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
               itolvo.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3_LVO, --ito.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_RGP_A ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_RGP_A,
               itoxms.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3_XMS, --ito.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_RGP_V ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_RGP_V,
               ito.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               itolvo.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4_LVO, --ito.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_1 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_1,
               itoxms.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4_XMS, --ito.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_2 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_2,
            --  null ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_3, --ito.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_3 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_3,
               ito.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               itolvo.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5_LVO, --ito.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_RGP_A ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_RGP_A,
               itoxms.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5_XMS, --ito.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_RGP_V  ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_RGP_V,
               null ITEM_EXPENSE_ACCOUNT_BU, --ito.ITEM_EXPENSE_ACCOUNT_BU ITEM_EXPENSE_ACCOUNT_BU,
               ito.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
               itolvo.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6_LVO, --ito.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_RGP_A ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_RGP_A,
               itoxms.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6_XMS, --ito.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_RGP_V  ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_RGP_V,
               ito.ITEM_SALES_ACCOUNT_BU_GL_SGT1 ITEM_SALES_ACCOUNT_BU_GL_SGT1,
               itolvo.ITEM_SALES_ACCOUNT_BU_GL_SGT1 ITEM_SALES_ACCOUNT_BU_GL_SGT1_LVO, --ito.ITEM_SALES_ACCOUNT_BU_GL_RGP_A ITEM_SALES_ACCOUNT_BU_GL_RGP_A,
               itoxms.ITEM_SALES_ACCOUNT_BU_GL_SGT1 ITEM_SALES_ACCOUNT_BU_GL_SGT1_XMS, --ito.ITEM_SALES_ACCOUNT_BU_GL_RGP_V ITEM_SALES_ACCOUNT_BU_GL_RGP_V,
               ito.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2 ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
               itolvo.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2 ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2_LVO,  --ito.ITEM_SALES_ACCOUNT_LOCATION_GL_RGP_A ITEM_SALES_ACCOUNT_LOCATION_GL_RGP_A,
               itoxms.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2 ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2_XMS, --ito.ITEM_SALES_ACCOUNT_LOCATION_GL_RGP_V ITEM_SALES_ACCOUNT_LOCATION_GL_RGP_V,
               ito.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
               itolvo.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3_LVO, --ito.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_RGP_A ITEM_SALES_ACCOUNT_DEPARTMENT_GL_RGP_A,
               itoxms.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3_XMS, --ito.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_RGP_V ITEM_SALES_ACCOUNT_DEPARTMENT_GL_RGP_V,
               ito.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               itolvo.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4_LVO, --ito.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_1 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_1,
               itoxms.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4_XMS, --ito.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_2 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_2,
            --  null ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_3, --ito.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_3 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_3,
               ito.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               itolvo.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5_LVO, -- ito.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_RGP_A ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_RGP_A,
               itoxms.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5_XMS, -- ito.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_RGP_V ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_RGP_V,
               null ITEM_SALES_ACCOUNT_BU, --ito.ITEM_SALES_ACCOUNT_BU ITEM_SALES_ACCOUNT_BU,
               ito.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
               itolvo.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6_LVO, -- ito.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_RGP_A ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_RGP_A,
               itoxms.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6_XMS, -- --ito.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_RGP_V ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_RGP_V,
            --  null FLAG_ITEM_LS_SALES, --ito.FLAG_ITEM_LS_SALES FLAG_ITEM_LS_SALES,
            --  null PERIMETRE_LS_CONSO,-- ito.PERIMETRE_LS_CONSO PERIMETRE_LS_CONSO,
               ito.CHART_OF_ACCOUNTS_ID,
               itoxms.BUYER_NAME BUYER_NAME_XMS,
               itolvo.BUYER_NAME BUYER_NAME_LVO,
               ito.LS_ACIM_PO_CATEGORY,
               ito.planning_make_buy_code,
               ito.WIP_SUPPLY_TYPE,
               ito.MINIMUM_ORDER_QUANTITY,
               ito.FIXED_LOT_MULTIPLIER,
               ito.MAXIMUM_ORDER_QUANTITY,
               ito.ORDER_COST,
               ito.CARRYING_COST,
               ito.FULL_LEAD_TIME,
               ito.PREPROCESSING_LEAD_TIME,
               ito.planning_make_buy_meaning,
               ito.WIP_SUPPLY_TYPE_MEANING,
               ito.INVENTORY_PLANNING_MEANING,
               ito.POSTPROCESSING_LEAD_TIME,
               ito.INVENTORY_PLANNING_CODE,
               itoxms.EIA_SHIPPING_WAREHOUSE as EIA_SHIPPING_WAREHOUSE_XMS,
               ito.EIA_SUB_CONTRACTING_PLANT,
               td.valeur AS FAMILLE_SERVICE,
               td2.valeur AS FAMILLE_SERVICE_DETAIL
      FROM ITEM_DETAILS_ORACLE     ito
      JOIN IO_DETAILS_TEMP   sidt
         ON  ito.ORGANIZATION_ID = sidt.OOD_ORGANIZATION_ID
      LEFT OUTER JOIN  ITEM_DETAILS_ORACLE itoxms
            ON ( ito.INVENTORY_ITEM_ID = itoxms.INVENTORY_ITEM_ID  and itoxms.ORGANIZATION_ID = 85 )
      LEFT OUTER JOIN  ITEM_DETAILS_ORACLE itolvo
            ON ( ito.INVENTORY_ITEM_ID = itolvo.INVENTORY_ITEM_ID  and itolvo.ORGANIZATION_ID = 356 )
      LEFT JOIN FILE_TRANSFORMATION_DATA_BZ td
            ON td.cle = ito.item_line AND td.type = 'FAMILLE_SERVICE'
      LEFT JOIN FILE_TRANSFORMATION_DATA_BZ td2
            ON td2.cle = ito.EIA_STATISTIC_CODE AND td2.type = 'FAMILLE_SERVICE_DETAIL'

/*      ;
   g_status   := 'COMPLETED';
               g_etape    := '010 - INSERT INTO' ;
               Write_Log_PROC;*/

            UNION ALL
/*                 g_table     := 'ITEM_DETAILS_ALL';
               g_date_deb  := sysdate;
               g_status    := 'WIP';
               g_etape     := $$plsql_line + 1  || ' - num error line';
                  INSERT INTO ITEM_DETAILS_ALL*/
      SELECT
               ida.INVENTORY_ITEM_ID INVENTORY_ITEM_IDA,
               ida.ORGANIZATION_ID ORGANIZATION_ID,
               ida.ITEM_CODE ITEM_CODE,
               ida.ITEM_LAST_UPDATE_DATE LAST_UPDATE_DATE_XMS,
               NULL LAST_UPDATED_BY_NAME_XMS,
               ida.ITEM_CREATION_DATE CREATION_DATE_XMS,
               NULL CREATED_BY_NAME_XMS,
               ida.ITEM_STATUS INVENTORY_ITEM_STATUS_CODE_XMS,
               sidt.OOD_ORGANIZATION_CODE IO_CODE,
               TRIM(sidt.HOU_NAME) OU_NAME,
               ida.ITEM_LAST_UPDATE_DATE LAST_UPDATE_DATE,
               NULL LAST_UPDATED_BY_NAME,
               ida.ITEM_CREATION_DATE CREATION_DATE,
               NULL CREATED_BY_NAME,
               ida.LONG_DESCRIPTION DESCRIPTION_FRENCH,
               ida.LONG_DESCRIPTION LONG_DESCRIPTION_FRENCH,
               ida.LONG_DESCRIPTION DESCRIPTION_ENGLISH,
               ida.LONG_DESCRIPTION LONG_DESCRIPTION_ENGLISH,
               ida.ITEM_CODE LEGACY_ITEM_LS,
               NULL LISA_ITEM_LS,
               ida.ITEM_STATUS INVENTORY_ITEM_STATUS_CODE,
               NULL EXPENSE_ACCOUNT_ID,
               NULL PRIMARY_UOM_CODE,
               NULL PLANNER_CODE,
               'BUYER_' || ida.source as BUYER_NAME_IO,
               NULL ITEM_TYPE,
               NULL DFF_DRAWING_REVISION,
               NULL DFF_PRICE_CHANGE_DATE,
               NULL DFF_DRAWING_NUMBER,
               NULL DFF_GLOBAL_REF_NUMBER,
               NULL DFF_GLOBAL_REF_DWG_NUMBER,
               NULL DFF_GLOBAL_REF_DWG_NUMBER_REV,
               NULL DFF_ITEM_CLASSIFICATION,
               NULL DFF_ORIGINATOR,
               NULL DFF_DMCA_INSPECTION,
               NULL DFF_PURCHASE_NOTE_REVISION,
               NULL DFF_PACKING_QUANTITY,
               NULL DFF_INVOICE_UOM,
               NULL DFF_TRANSFERRED_TO_FUZHOU,
               NULL ITEM_CATALOG_GROUP_ID,
               NULL ITEM_ICC,
               NULL DFF_COMMODITY_MANAGER_LS,
               NULL DFF_CENTRAL_SPEC_NAME_LS,
               NULL DFF_CENTRAL_SPEC_REV_LS,
               NULL DFF_DRI_CODES_LS,
               NULL DFF_UNSPSC_CODES_LS,
               NULL DFF_CENTRAL_HOMO_FLAG_LS,
               NULL DFF_INTRASTAT_CLASS_LS,
               NULL DFF_QUALIFICTION_REQ_LS,
               NULL DFF_COMMODITY_CTRL,
               NULL DFF_LEADTIME_TYPE,
               NULL DFF_COMMODITY_PRICE_CONTROL,
               NULL DFF_ITEM_DUPLICATE_CHECK_LS,
               NULL DFF_ICC_UOM,
               NULL DFF_EXPENSE_NATURAL_ACCOUNT,
               NULL DFF_SALES_NATURAL_ACCOUNT,
               NULL DFF_DRI_UOM_CONVERSION,
               NULL PARENT_ICC,
               NULL SALES_ACCOUNT_ID,
               ida.EIA_ALLOCATION EIA_ALLOCATION,
               ida.EIA_CIRCUIT_INFORMATION EIA_CIRCUIT_INFORMATION,
               NULL EIA_COUNTRY_OF_ORIGIN,
               ida.EIA_DESIGN_OWNER_PLANT EIA_DESIGN_OWNER_PLANT,
               NULL EIA_EBUSINESS_TAX,
               NULL EIA_PRODUCT_STATISTICS_CODE,
               ida.EIA_SHIPPING_WAREHOUSE EIA_SHIPPING_WAREHOUSE,
               ida.EIA_STATISTIC_CODE EIA_STATISTIC_CODE,
               ida.EIA_STATISTIC_CODE_DESCRIPTION EIA_STATISTIC_CODE_DESCRIPTION,
               ida.ITEM_NATURE ITEM_NATURE,
               ida.ITEM_FAMILY ITEM_FAMILY,
               ida.ITEM_FAMILY_DESCRIPTION ITEM_FAMILY_DESCRIPTION,
               ida.ITEM_LINE ITEM_LINE,
               ida.ITEM_LINE_DESCRIPTION ITEM_LINE_DESCRIPTION,
               ida.LIST_PRICE LIST_PRICE,
               ida.WEIGHT WEIGHT,
               ida.CUSTOMS_CODE CUSTOMS_CODE,
               NULL COO,
               ida.EIA_ITEM_CODE EIA_ITEM_CODE,
               null EIA_ALLOCATION_LVO,--ida.SUBBU EIA_CATEGORY_ITEM_SUBBU,
               null EIA_ALLOCATION_XMS, --ida.BU EIA_CATEGORY_ITEM_BU,
               ida.SOURCE SOURCE,
               ROWNUM ROW_NUMBER_ID,
               SYSDATE ROW_CREATION_DATE,
               SYSDATE ROW_LAST_UPDATE_DATE,
               ida.ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1 ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1,
               null ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1_LVO, --fr1e.BU_GL_REGROUPEMENT_ACHAT ITEM_EXPENSE_ACCOUNT_BU_GL_RGP_A,
               null ITEM_EXPENSE_ACCOUNT_BU_GL_SGT1_XMS, --fr1e.BU_GL_REGROUPEMENT_VENTE ITEM_EXPENSE_ACCOUNT_BU_GL_RGP_V,
               ida.ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2 ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2,
               null ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2_LVO, --fr2e.LOCATION_GL_REGROUPEMENT_ACHAT ITEM_EXPENSE_ACCOUNT_LOCATION_GL_RGP_A,
               null ITEM_EXPENSE_ACCOUNT_LOCATION_GL_SGT2_XMS, --fr2e.LOCATION_GL_REGROUPEMENT_VENTE ITEM_EXPENSE_ACCOUNT_LOCATION_GL_RGP_V,
               ida.ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3,
               null ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3_LVO, --fr3e.DEPARTMENT_GL_REGROUPEMENT_ACHAT ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_RGP_A,
               null ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_SGT3_XMS, --fr3e.DEPARTMENT_GL_REGROUPEMENT_VENTE ITEM_EXPENSE_ACCOUNT_DEPARTMENT_GL_RGP_V,
               ida.ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               null ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4_LVO, --fr4e.NATURAL_ACCOUNT_GL_REGROUPEMENT_1 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_1,
               null ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_SGT4_XMS, --fr4e.NATURAL_ACCOUNT_GL_REGROUPEMENT_2 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_2,
            --   null ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_3, --fr4e.NATURAL_ACCOUNT_GL_REGROUPEMENT_3 ITEM_EXPENSE_ACCOUNT_NATURALACCOUNT_GL_RGP_3,
               ida.ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               null ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5_LVO, --fr5e.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT  ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_RGP_A,
               null ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_SGT5_XMS, --fr5e.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE ITEM_EXPENSE_ACCOUNT_PRODUCTGROUP_GL_RGP_V,
               null ITEM_EXPENSE_ACCOUNT_BU, --vbe.BU_NEW ITEM_EXPENSE_ACCOUNT_BU,
               ida.ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6,
               null ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6_LVO, --fr6e.INTERCOMPANY_GL_REGROUPEMENT_ACHAT ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_RGP_A,
               null ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_SGT6_XMS, --fr6e.INTERCOMPANY_TYPE ITEM_EXPENSE_ACCOUNT_INTERCOMPANY_GL_RGP_V,
               ida.ITEM_SALES_ACCOUNT_BU_GL_SGT1 ITEM_SALES_ACCOUNT_BU_GL_SGT1,
               null ITEM_SALES_ACCOUNT_BU_GL_SGT1_LVO, --fr1s.BU_GL_REGROUPEMENT_ACHAT ITEM_SALES_ACCOUNT_BU_GL_RGP_A,
               null ITEM_SALES_ACCOUNT_BU_GL_SGT1_XMS, --fr1s.BU_GL_REGROUPEMENT_VENTE ITEM_SALES_ACCOUNT_BU_GL_RGP_V,
               ida.ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2 ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2,
               null ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2_LVO, --fr2s.LOCATION_GL_REGROUPEMENT_ACHAT ITEM_SALES_ACCOUNT_LOCATION_GL_RGP_A,
               null ITEM_SALES_ACCOUNT_LOCATION_GL_SGT2_XMS, --fr2s.LOCATION_GL_REGROUPEMENT_VENTE ITEM_SALES_ACCOUNT_LOCATION_GL_RGP_V,
               ida.ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3 ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3,
               null ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3_LVO, --fr3s.DEPARTMENT_GL_REGROUPEMENT_ACHAT ITEM_SALES_ACCOUNT_DEPARTMENT_GL_RGP_A,
               null ITEM_SALES_ACCOUNT_DEPARTMENT_GL_SGT3_XMS, --fr3s.DEPARTMENT_GL_REGROUPEMENT_VENTE ITEM_SALES_ACCOUNT_DEPARTMENT_GL_RGP_V,
               ida.ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4,
               null ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4_LVO, --fr4s.NATURAL_ACCOUNT_GL_REGROUPEMENT_1 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_1,
               null ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_SGT4_XMS, --fr4s.NATURAL_ACCOUNT_GL_REGROUPEMENT_2 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_2,
            --   null ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_3, --fr4s.NATURAL_ACCOUNT_GL_REGROUPEMENT_3 ITEM_SALES_ACCOUNT_NATURALACCOUNT_GL_RGP_3,
               ida.ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5 ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5,
               null ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5_LVO, --fr5s.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT  ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_RGP_A,
               null ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_SGT5_XMS, -- fr5s.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE ITEM_SALES_ACCOUNT_PRODUCTGROUP_GL_RGP_V,
               ida.BU ITEM_SALES_ACCOUNT_BU, -- vbs.BU_NEW ITEM_SALES_ACCOUNT_BU,
               ida.ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6 ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6,
               null ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6_LVO, -- fr6s.INTERCOMPANY_GL_REGROUPEMENT_ACHAT ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_RGP_A,
               null ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_SGT6_XMS, --fr6s.INTERCOMPANY_TYPE ITEM_SALES_ACCOUNT_INTERCOMPANY_GL_RGP_V,
            --    null FLAG_ITEM_LS_SALES,-- fr4s.FLAG_ITEM_LS_SALES FLAG_ITEM_LS_SALES,
            --    null PERIMETRE_LS_CONSO , -- fr6s.PERIMETRE_LS_CONSO PERIMETRE_LS_CONSO ,
               ida.CHART_OF_ACCOUNTS_ID,
               'BUYER_' || ida.source as  BUYER_NAME_XMS,
               'BUYER_' || ida.source as  BUYER_NAME_LVO,
               NULL LS_ACIM_PO_CATEGORY,
               NULL planning_make_buy_code,
               NULL WIP_SUPPLY_TYPE,
               NULL MINIMUM_ORDER_QUANTITY,
               NULL FIXED_LOT_MULTIPLIER,
               NULL MAXIMUM_ORDER_QUANTITY,
               NULL ORDER_COST,
               NULL CARRYING_COST,
               NULL FULL_LEAD_TIME,
               NULL PREPROCESSING_LEAD_TIME,
               NULL planning_make_buy_meaning,
               NULL WIP_SUPPLY_TYPE_MEANING,
               NULL INVENTORY_PLANNING_MEANING,
               NULL POSTPROCESSING_LEAD_TIME,
               NULL INVENTORY_PLANNING_CODE,
               null as EIA_SHIPPING_WAREHOUSE_XMS,
               NULL EIA_SUB_CONTRACTING_PLANT,
               td.valeur AS FAMILLE_SERVICE,
               td2.valeur AS FAMILLE_SERVICE_DETAIL

      FROM ITEM_DETAILS_NOT_ORACLE_TEMP ida
      LEFT OUTER JOIN IO_DETAILS_TEMP   sidt
            ON  ida.ORGANIZATION_ID = sidt.OOD_ORGANIZATION_ID
      LEFT JOIN FILE_TRANSFORMATION_DATA_BZ td
            ON td.cle = ida.item_line AND td.type = 'FAMILLE_SERVICE'
      LEFT JOIN FILE_TRANSFORMATION_DATA_BZ td2
            ON td2.cle = ida.EIA_STATISTIC_CODE AND td2.type = 'FAMILLE_SERVICE_DETAIL'
   ;
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   --  g_etape := '100';
   g_table     := 'ITEM_DETAILS_ALL';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ITEM_DETAILS_ALL');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
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

   END Item_Details_All_PROC;

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
   v_status              varchar2(1) := 'A';  --statut Accept� (sinon R pour Rejet�)
   v_message             varchar2(1000) := NULL;
   v_errbuf	VARCHAR2(4000);
   v_retcode   NUMBER;
      v_date_deb_pkg  TIMESTAMP := sysdate;

   BEGIN  --D�but traitement
   DBMS_OUTPUT.ENABLE (1000000);

   -- g_programme := 'LH2_DTH_SILVER_ITEMS_PKG.MAIN';
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

   /*v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Refcfg_Codestat_Line_Family_Temp_PROC');
   -- g_etape := '10';
   Refcfg_Codestat_Line_Family_Temp_PROC;*/

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Refcfg_Articles_Details_Temp_PROC');
   -- g_etape := '20';
   Refcfg_Articles_Details_Temp_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Steph_Item_Categories_PROC');
   --  g_etape := '30';
   Steph_Item_Categories_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Steph_Item_Cross_References_PROC');
   --  g_etape := '40';
   Steph_Item_Cross_References_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Item_Details_Not_Oracle_PROC');
--   g_etape := '50';
   Item_Details_Not_Oracle_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Item_Details_Oracle_PROC');

   EXECUTE IMMEDIATE 'ALTER SESSION SET OPTIMIZER_IGNORE_HINTS = FALSE'  ;
   EXECUTE IMMEDIATE 'ALTER SESSION SET OPTIMIZER_IGNORE_PARALLEL_HINTS = FALSE';
   EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;

   --  g_etape := '60';
   Item_Details_Oracle_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Item_Details_All_PROC');
--   g_etape := '70';
   Item_Details_All_PROC;

   -- g_etape := '1000';
   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('-----------------------------END--------------------------------------');--fin compte-rendu

   if g_erreur_pkg = 1 then
         pn_retcode  := 1;
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
   end if ;

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

END LH2_DTH_SILVER_ITEMS_PKG;