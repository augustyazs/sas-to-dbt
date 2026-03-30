/*==================================================================*/
/* Procedure: CLASSIFY_GLP1_MEMBERS                                 */
/* Purpose:                                                         */
/*   Identify members using GLP1 drugs for:                         */
/*     (1) Weight Loss                                              */
/*     (2) Diabetes Management                                      */
/*                                                                  */
/* Parameters:                                                      */
/*   p_rxdata   IN VARCHAR2  — Pharmacy claims table name           */
/*   p_meddata  IN VARCHAR2  — Medical claims/diagnosis table name  */
/*   p_outdata  IN VARCHAR2  — Final output table name              */
/*   p_startdt  IN DATE      — Start date                           */
/*   p_enddt    IN DATE      — End date                             */
/*==================================================================*/

CREATE OR REPLACE PROCEDURE classify_glp1_members (
    p_rxdata  IN VARCHAR2,
    p_meddata IN VARCHAR2,
    p_outdata IN VARCHAR2,
    p_startdt IN DATE,
    p_enddt   IN DATE
)
AS
    /*------------------------------------------------------------------*/
    /* 1. NDC and ICD-10 reference lists                                */
    /*    SAS used macro variable IN-lists; here we use temporary       */
    /*    collection types so the same values are referenced by name    */
    /*    throughout the procedure without repetition.                  */
    /*------------------------------------------------------------------*/

    -- Weight-loss GLP1 NDCs (Wegovy / semaglutide)
    TYPE t_ndc_list IS TABLE OF VARCHAR2(20);

    v_wl_ndcs  t_ndc_list := t_ndc_list(
        '0169-7501-12', '0169-7501-13', '0169-7501-14'
    );

    -- Diabetes GLP1 NDCs (Ozempic, Trulicity, Victoza, Rybelsus)
    v_dm_ndcs  t_ndc_list := t_ndc_list(
        '0169-3935-11', '0169-3936-11', '0169-3937-11',   -- Ozempic
        '00002-8215-01', '00002-8215-59',                  -- Trulicity
        '00173-7085-01', '00173-7085-02',                  -- Victoza
        '00024-0590-30', '00024-0591-30'                   -- Rybelsus
    );

    -- Diabetes ICD-10 prefixes (3-character)
    TYPE t_icd_list IS TABLE OF VARCHAR2(3);
    v_icd10    t_icd_list := t_icd_list('E08','E09','E10','E11','E13');

    v_sql      VARCHAR2(4000);

BEGIN

    /*------------------------------------------------------------------*/
    /* 2. Drop work tables from any prior run                           */
    /*------------------------------------------------------------------*/
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE glp1_rx_work';
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE diabetes_dx_work';
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE classified_work';
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_outdata;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;


    /*------------------------------------------------------------------*/
    /* 3. Filter pharmacy claims for GLP-1 NDCs within date range       */
    /*    SAS: DATA glp1_rx; SET &RXDATA; WHERE fill_date BETWEEN ...   */
    /*------------------------------------------------------------------*/
    v_sql :=
        'CREATE TABLE glp1_rx_work AS ' ||
        'SELECT member_id, ndc, fill_date ' ||
        'FROM ' || p_rxdata || ' ' ||
        'WHERE fill_date BETWEEN :1 AND :2 ' ||
        '  AND ndc IN ( ' ||
        -- weight-loss NDCs
        '    SELECT column_value FROM TABLE(:3) ' ||
        '    UNION ALL ' ||
        -- diabetes NDCs
        '    SELECT column_value FROM TABLE(:4) ' ||
        '  )';

    EXECUTE IMMEDIATE v_sql USING p_startdt, p_enddt, v_wl_ndcs, v_dm_ndcs;


    /*------------------------------------------------------------------*/
    /* 4. Identify diabetes diagnoses from medical claims               */
    /*    SAS: WHERE SUBSTR(dx_code,1,3) IN (&DIABETES_ICD10.)          */
    /*    SAS PROC SORT NODUPKEY → DISTINCT on member_id                */
    /*------------------------------------------------------------------*/
    v_sql :=
        'CREATE TABLE diabetes_dx_work AS ' ||
        'SELECT DISTINCT member_id ' ||
        'FROM ' || p_meddata || ' ' ||
        'WHERE SUBSTR(dx_code, 1, 3) IN ( ' ||
        '    SELECT column_value FROM TABLE(:1) ' ||
        ')';

    EXECUTE IMMEDIATE v_sql USING v_icd10;


    /*------------------------------------------------------------------*/
    /* 5. Classify GLP-1 members                                        */
    /*    SAS: PROC SQL LEFT JOIN + CASE                                */
    /*    NVL not needed — CASE WHEN dx.member_id IS NOT NULL covers it */
    /*------------------------------------------------------------------*/
    v_sql :=
        'CREATE TABLE classified_work AS ' ||
        'SELECT ' ||
        '    rx.member_id, ' ||
        '    rx.ndc, ' ||
        '    rx.fill_date, ' ||
        '    CASE ' ||
        '        WHEN rx.ndc IN (SELECT column_value FROM TABLE(:1)) ' ||
        '            THEN ''Weight Loss GLP1'' ' ||
        '        WHEN rx.ndc IN (SELECT column_value FROM TABLE(:2)) ' ||
        '            THEN ''Diabetes GLP1'' ' ||
        '    END AS drug_category, ' ||
        '    CASE ' ||
        '        WHEN dx.member_id IS NOT NULL THEN 1 ELSE 0 ' ||
        '    END AS has_diabetes_dx ' ||
        'FROM glp1_rx_work rx ' ||
        'LEFT JOIN diabetes_dx_work dx ' ||
        '    ON rx.member_id = dx.member_id';

    EXECUTE IMMEDIATE v_sql USING v_wl_ndcs, v_dm_ndcs;


    /*------------------------------------------------------------------*/
    /* 6. Final classification logic → output table                     */
    /*    SAS IF/ELSE → CASE expression                                 */
    /*    Rules:                                                         */
    /*      Weight Loss GLP1               → 'Weight Loss'              */
    /*      Diabetes GLP1 + dx present     → 'Diabetes Management'      */
    /*      Diabetes GLP1 + no dx          → 'Likely Weight Loss'       */
    /*------------------------------------------------------------------*/
    v_sql :=
        'CREATE TABLE ' || p_outdata || ' AS ' ||
        'SELECT ' ||
        '    member_id, ' ||
        '    ndc, ' ||
        '    fill_date, ' ||
        '    drug_category, ' ||
        '    has_diabetes_dx, ' ||
        '    CASE ' ||
        '        WHEN drug_category = ''Weight Loss GLP1'' ' ||
        '            THEN ''Weight Loss'' ' ||
        '        WHEN drug_category = ''Diabetes GLP1'' AND has_diabetes_dx = 1 ' ||
        '            THEN ''Diabetes Management'' ' ||
        '        WHEN drug_category = ''Diabetes GLP1'' AND has_diabetes_dx = 0 ' ||
        '            THEN ''Likely Weight Loss'' ' ||
        '    END AS final_category ' ||
        'FROM classified_work';

    EXECUTE IMMEDIATE v_sql;


    /*------------------------------------------------------------------*/
    /* 7. Cleanup work tables                                           */
    /*    SAS WORK library tables are auto-dropped; we do it explicitly */
    /*------------------------------------------------------------------*/
    EXECUTE IMMEDIATE 'DROP TABLE glp1_rx_work';
    EXECUTE IMMEDIATE 'DROP TABLE diabetes_dx_work';
    EXECUTE IMMEDIATE 'DROP TABLE classified_work';

    COMMIT;

    /*------------------------------------------------------------------*/
    /* Note: PROC FREQ summary report is not converted.                 */
    /* Run the following query manually to get the equivalent summary:  */
    /*                                                                  */
    /* SELECT final_category, COUNT(*) AS member_count                  */
    /* FROM <p_outdata>                                                 */
    /* GROUP BY final_category                                          */
    /* ORDER BY final_category;                                         */
    /*------------------------------------------------------------------*/

EXCEPTION
    WHEN OTHERS THEN
        -- Roll back any partial DML and re-raise
        ROLLBACK;
        RAISE;

END classify_glp1_members;
/


/*==================================================================*/
/* Example Call (equivalent to the SAS macro invocation)           */
/*==================================================================*/

BEGIN
    classify_glp1_members(
        p_rxdata  => 'pharmacy_claims',
        p_meddata => 'medical_claims',
        p_outdata => 'glp1_member_classification',
        p_startdt => DATE '2023-01-01',
        p_enddt   => DATE '2023-12-31'
    );
END;
/