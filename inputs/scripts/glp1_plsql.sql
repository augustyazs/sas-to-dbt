CREATE OR REPLACE PROCEDURE CLASSIFY_GLP1_MEMBERS (
    RXDATA   IN VARCHAR2,
    MEDDATA  IN VARCHAR2,
    OUTDATA  IN VARCHAR2,
    STARTDT  IN DATE,
    ENDDT    IN DATE
)
AS
    GLP1_WEIGHTLOSS_NDCS CONSTANT VARCHAR2(200) :=
        '''0169-7501-12'',''0169-7501-13'',''0169-7501-14''';

    GLP1_DIABETES_NDCS CONSTANT VARCHAR2(400) :=
        '''0169-3935-11'',''0169-3936-11'',''0169-3937-11'',' ||
        '''00002-8215-01'',''00002-8215-59'',' ||
        '''00173-7085-01'',''00173-7085-02'',' ||
        '''00024-0590-30'',''00024-0591-30''';

    DIABETES_ICD10 CONSTANT VARCHAR2(80) :=
        '''E08'',''E09'',''E10'',''E11'',''E13''';

    V_SQL CLOB;
BEGIN
    /*------------------------------------------------------------------*/
    /* 1. Define GLP-1 NDC Lists                                        */
    /*------------------------------------------------------------------*/

    /*------------------------------------------------------------------*/
    /* 2. Diabetes ICD-10 Codes                                         */
    /*------------------------------------------------------------------*/

    /*------------------------------------------------------------------*/
    /* 3. Filter Pharmacy Claims for GLP-1                              */
    /*------------------------------------------------------------------*/
    V_SQL :=
        'CREATE TABLE glp1_rx AS ' ||
        'SELECT member_id, ndc, fill_date ' ||
        'FROM ' || RXDATA || ' ' ||
        'WHERE TRUNC(fill_date) BETWEEN TRUNC(:1) AND TRUNC(:2) ' ||
        '  AND ndc IN (' || GLP1_WEIGHTLOSS_NDCS || ',' || GLP1_DIABETES_NDCS || ')';

    EXECUTE IMMEDIATE V_SQL USING STARTDT, ENDDT;

    /*------------------------------------------------------------------*/
    /* 4. Identify Diabetes Diagnosis from Medical Claims               */
    /*------------------------------------------------------------------*/
    V_SQL :=
        'CREATE TABLE diabetes_dx AS ' ||
        'SELECT member_id, dx_code, service_date ' ||
        'FROM ' || MEDDATA || ' ' ||
        'WHERE SUBSTR(dx_code, 1, 3) IN (' || DIABETES_ICD10 || ')';

    EXECUTE IMMEDIATE V_SQL;

    EXECUTE IMMEDIATE
        'CREATE TABLE diabetes_dx_sort AS ' ||
        'SELECT member_id, dx_code, service_date ' ||
        'FROM (' ||
        '    SELECT member_id, dx_code, service_date, ' ||
        '           ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY member_id) AS rn ' ||
        '    FROM diabetes_dx' ||
        ') ' ||
        'WHERE rn = 1';

    EXECUTE IMMEDIATE 'DROP TABLE diabetes_dx PURGE';
    EXECUTE IMMEDIATE 'ALTER TABLE diabetes_dx_sort RENAME TO diabetes_dx';

    /*------------------------------------------------------------------*/
    /* 5. Classify GLP-1 Members                                        */
    /*------------------------------------------------------------------*/
    V_SQL :=
        'CREATE TABLE classified AS ' ||
        'SELECT rx.member_id, ' ||
        '       rx.ndc, ' ||
        '       rx.fill_date, ' ||
        '       CASE ' ||
        '           WHEN rx.ndc IN (' || GLP1_WEIGHTLOSS_NDCS || ') THEN ''Weight Loss GLP1'' ' ||
        '           WHEN rx.ndc IN (' || GLP1_DIABETES_NDCS || ') THEN ''Diabetes GLP1'' ' ||
        '       END AS drug_category, ' ||
        '       CASE WHEN dx.member_id IS NOT NULL THEN 1 ELSE 0 END AS has_diabetes_dx ' ||
        'FROM glp1_rx rx ' ||
        'LEFT JOIN diabetes_dx dx ' ||
        '  ON rx.member_id = dx.member_id';

    EXECUTE IMMEDIATE V_SQL;

    /*------------------------------------------------------------------*/
    /* 6. Final Classification Logic                                    */
    /*------------------------------------------------------------------*/
    V_SQL :=
        'CREATE TABLE ' || OUTDATA || ' AS ' ||
        'SELECT member_id, ' ||
        '       ndc, ' ||
        '       fill_date, ' ||
        '       drug_category, ' ||
        '       has_diabetes_dx, ' ||
        '       CASE ' ||
        '           WHEN drug_category = ''Weight Loss GLP1'' THEN ''Weight Loss'' ' ||
        '           WHEN drug_category = ''Diabetes GLP1'' AND has_diabetes_dx = 1 THEN ''Diabetes Management'' ' ||
        '           WHEN drug_category = ''Diabetes GLP1'' AND has_diabetes_dx = 0 THEN ''Likely Weight Loss'' ' ||
        '       END AS final_category ' ||
        'FROM classified';

    EXECUTE IMMEDIATE V_SQL;

    /*------------------------------------------------------------------*/
    /* 7. Summary Report                                                */
    /*------------------------------------------------------------------*/
END CLASSIFY_GLP1_MEMBERS;
/
BEGIN
    CLASSIFY_GLP1_MEMBERS(
        RXDATA  => 'pharmacy_claims',
        MEDDATA => 'medical_claims',
        OUTDATA => 'glp1_member_classification',
        STARTDT => DATE '2023-01-01',
        ENDDT   => DATE '2023-12-31'
    );
END;
/