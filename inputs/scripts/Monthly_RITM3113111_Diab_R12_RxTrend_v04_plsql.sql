CREATE OR REPLACE PROCEDURE MONTHLY_RITM3113111_DIAB_R12_RXTREND_V04 AS
    V_SQL CLOB;

    PROCEDURE DROP_TABLE_IF_EXISTS(P_TABLE_NAME IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || DBMS_ASSERT.QUALIFIED_SQL_NAME(P_TABLE_NAME) || ' PURGE';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
                RAISE;
            END IF;
    END;
BEGIN
    DROP_TABLE_IF_EXISTS('R12_MM_DATA');
    DROP_TABLE_IF_EXISTS('R12_MN');
    DROP_TABLE_IF_EXISTS('R12_TEMP_MM');
    DROP_TABLE_IF_EXISTS('R12_CLM');
    DROP_TABLE_IF_EXISTS('R12_TEMP_CLM');
    DROP_TABLE_IF_EXISTS('RAWDATA');
    DROP_TABLE_IF_EXISTS('LOB_CLM_TMP');
    DROP_TABLE_IF_EXISTS('R12_REPORT');

    EXECUTE IMMEDIATE q'[
        CREATE TABLE R12_MM_DATA AS
        SELECT
            YEARMO,
            SUM(COMMERCIAL_MBR_CNT) AS TOT_COMM,
            SUM(COM_UW_MBR_CNT) AS TOT_COMM_UW,
            SUM(COM_FI_MBR_CNT) AS TOT_COMM_FI,
            SUM(NOVATION_IMAPD) AS TOT_PHP_MDCR,
            SUM(IMAPD_MBR_CNT) + SUM(GMAPD_MBR_CNT) AS MAPD_NO_PHP,
            SUM(IMAPD_ALL_MBR_CNT) + SUM(GMAPD_MBR_CNT) AS MAPD_ALL,
            SUM(IPDP_MBR_CNT) + SUM(GPDP_MBR_CNT) AS PDP_ALL,
            SUM(TOT_MDCR_RX) AS TOT_MDCR,
            SUM(PHP_CMC_CNT) AS TOT_CMC,
            SUM(TOT_MDCR_RX) + SUM(PHP_CMC_CNT) AS TOT_MDCR_W_CMC,
            SUM(CORE_MBR_CNT) AS TOT_CORE,
            SUM(PREM_MBR_CNT) AS TOT_PREM,
            SUM(TOT_SG_MBR_CNT) AS TOT_SG,
            SUM(TOT_IFP_MBR_CNT) AS TOT_IFP
        FROM MEMBERSHIP_SUMMARY
        GROUP BY YEARMO
        ORDER BY YEARMO
    ]';

    EXECUTE IMMEDIATE 'DELETE FROM A.R12_MM WHERE YRMO IN (SELECT DISTINCT TO_DATE(SUBSTR(YEARMO,1,4)||''-''||SUBSTR(YEARMO,6,2)||''-01'',''YYYY-MM-DD'') FROM R12_MM_DATA)';

    EXECUTE IMMEDIATE q'[
        CREATE TABLE R12_MM AS
        SELECT LOB,
               YRMO,
               SUM(MM) AS MM
        FROM A.R12_MM
        GROUP BY LOB, YRMO
        ORDER BY LOB, YRMO
    ]';

    EXECUTE IMMEDIATE q'[
        CREATE TABLE R12_MN AS
        SELECT DISTINCT YRMO,
               ADD_MONTHS(TRUNC(YRMO, 'MM'), -11) AS R12_BEG
        FROM A.R12_MM
        WHERE YRMO >= DATE '2019-12-01'
        ORDER BY YRMO
    ]';

    /* Roll-up step mirrors SAS macro loops. */
    EXECUTE IMMEDIATE q'[
        CREATE TABLE RAWDATA AS
        SELECT DISTINCT
            CASE WHEN B.NDC IS NULL THEN 'Non-GLP1: DM' ELSE 'Drug Type 1' END AS drug_type1,
            CASE WHEN B.NDC IS NULL THEN 'Non-GLP1: DM' ELSE 'Drug Type2' END AS drug_type2,
            TO_DATE(TO_CHAR(A.YRMO), 'YYYYMM') AS YRMO,
            EXTRACT(YEAR FROM A.DATE_FILLED) AS YR,
            TRUNC(A.DATE_FILLED, 'IW') AS WEEK_OF,
            A.*
        FROM CLAIM_DETAIL_PAID A
        LEFT JOIN A.R12_DIAB_DRUGS B
          ON A.NDC = B.NDC
        WHERE A.DATE_FILLED BETWEEN DATE '2022-01-01' AND LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1))
          AND A.DRUG_NDC IN (SELECT NDC FROM A.R12_DIAB_DRUGS)
          AND A.CLAIM_CICS_STATUS_CODE NOT IN ('HIS','HRV')
    ]';

    EXECUTE IMMEDIATE q'[
        CREATE TABLE R12_CLM AS
        SELECT
            LOB,
            drug_type1,
            drug_type2,
            YRMO,
            SUM(util_mbrs) AS tot_util_mbrs,
            AVG(util_mbrs) AS avg_util_mbrs,
            SUM(rx) AS rx,
            SUM(rx_norm) AS rx_norm,
            SUM(days) / SUM(rx) AS days_rx,
            SUM(allw) AS allw,
            SUM(mbr_cost) AS mbr_cost,
            SUM(paid) AS paid,
            SUM(allw) / SUM(rx_norm) AS allw_rx_norm
        FROM RAWDATA
        GROUP BY LOB, drug_type1, drug_type2, YRMO
    ]';

    EXECUTE IMMEDIATE q'[
        CREATE TABLE R12_REPORT AS
        SELECT
            a.LOB,
            drug_type1,
            drug_type2,
            a.YRMO,
            tot_util_mbrs,
            avg_util_mbrs,
            mm,
            rx,
            rx_norm,
            days_rx,
            allw,
            mbr_cost,
            paid,
            allw_rx_norm,
            tot_util_mbrs / mm AS tot_mbr_util_perc,
            avg_util_mbrs / (mm / 12) AS avg_mbr_util_perc,
            (rx / mm) * 1000 AS rx_pkmpy,
            (rx_norm / mm) * 1000 AS rxnorm_pkmpy,
            allw / mm AS allw_pmpm,
            mbr_cost / mm AS mbr_cost_pmpm,
            paid / mm AS paid_pmpm
        FROM R12_CLM a
        LEFT JOIN R12_MM b
          ON a.LOB = b.LOB AND a.YRMO = b.YRMO
    ]';

    EXECUTE IMMEDIATE q'[DELETE FROM R12_REPORT WHERE LOB = 'Commercial FI']';
    EXECUTE IMMEDIATE q'[DELETE FROM R12_MM WHERE LOB = 'Commercial FI']';
    EXECUTE IMMEDIATE q'[DELETE FROM R12_CLM WHERE LOB = 'Commercial FI']';

    COMMIT;
END MONTHLY_RITM3113111_DIAB_R12_RXTREND_V04;
/
