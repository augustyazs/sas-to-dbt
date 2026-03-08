/*==================================================================*/
/* Macro: CLASSIFY_GLP1_MEMBERS                                     */
/* Purpose:                                                          */
/*   Identify members who are using GLP1 drugs for:                  */
/*      (1) Weight Loss                                              */
/*      (2) Diabetes Management                                      */
/*                                                                    */
/* Parameters:                                                       */
/*   RXDATA   = Pharmacy claims dataset                               */
/*   MEDDATA  = Medical claims/diagnosis dataset                      */
/*   OUTDATA  = Final output dataset                                  */
/*   STARTDT  = Start date (YYYY-MM-DD)                               */
/*   ENDDT    = End date (YYYY-MM-DD)                                 */
/*==================================================================*/

%macro CLASSIFY_GLP1_MEMBERS(RXDATA=, MEDDATA=, OUTDATA=, STARTDT=, ENDDT=);

/*------------------------------------------------------------------*/
/* 1. Define GLP-1 NDC Lists                                         */
/*------------------------------------------------------------------*/

/* Weight-loss GLP1 drugs */
%let GLP1_WEIGHTLOSS_NDCS =
    "0169-7501-12","0169-7501-13","0169-7501-14"   /* Wegovy (semaglutide) */
;

/* Diabetes GLP1 drugs */
%let GLP1_DIABETES_NDCS =
    "0169-3935-11","0169-3936-11","0169-3937-11"   /* Ozempic */
    "00002-8215-01","00002-8215-59"                /* Trulicity */
    "00173-7085-01","00173-7085-02"                /* Victoza */
    "00024-0590-30","00024-0591-30"                /* Rybelsus */
;

/*------------------------------------------------------------------*/
/* 2. Diabetes ICD-10 Codes                                         */
/*------------------------------------------------------------------*/
%let DIABETES_ICD10 = 
    "E08","E09","E10","E11","E13"
;

/*------------------------------------------------------------------*/
/* 3. Filter Pharmacy Claims for GLP-1                              */
/*------------------------------------------------------------------*/

data glp1_rx;
    set &RXDATA.;
    where fill_date between "&STARTDT."d and "&ENDDT."d
      and ndc in (&GLP1_WEIGHTLOSS_NDCS., &GLP1_DIABETES_NDCS.);
run;

/*------------------------------------------------------------------*/
/* 4. Identify Diabetes Diagnosis from Medical Claims               */
/*------------------------------------------------------------------*/

data diabetes_dx;
    set &MEDDATA.;
    where substr(dx_code,1,3) in (&DIABETES_ICD10.);
    keep member_id dx_code service_date;
run;

/* Get unique diabetes members */
proc sort data=diabetes_dx nodupkey;
    by member_id;
run;

/*------------------------------------------------------------------*/
/* 5. Classify GLP-1 Members                                        */
/*------------------------------------------------------------------*/

proc sql;
    create table classified as
    select  rx.member_id,
            rx.ndc,
            rx.fill_date,

            /* Drug-based classification */
            case 
                when rx.ndc in (&GLP1_WEIGHTLOSS_NDCS.) then "Weight Loss GLP1"
                when rx.ndc in (&GLP1_DIABETES_NDCS.)   then "Diabetes GLP1"
            end as drug_category length=25,

            /* Diagnosis-based classification */
            case 
                when dx.member_id is not null then 1 else 0
            end as has_diabetes_dx

    from glp1_rx rx
    left join diabetes_dx dx
         on rx.member_id = dx.member_id;
quit;

/*------------------------------------------------------------------*/
/* 6. Final Classification Logic                                    */
/*------------------------------------------------------------------*/
/*
    Rules:
    - If member is taking a weight-loss GLP1 → Weight Loss user
    - If taking diabetes GLP1 AND has diabetes diagnosis → Diabetes user
    - If taking diabetes GLP1 BUT no diabetes diagnosis → Possible Weight Loss use
*/

data &OUTDATA.;
    set classified;

    length final_category $40;

    if drug_category = "Weight Loss GLP1" then 
        final_category = "Weight Loss";

    else if drug_category = "Diabetes GLP1" and has_diabetes_dx = 1 then 
        final_category = "Diabetes Management";

    else if drug_category = "Diabetes GLP1" and has_diabetes_dx = 0 then 
        final_category = "Likely Weight Loss";

run;

/*------------------------------------------------------------------*/
/* 7. Summary Report                                                */
/*------------------------------------------------------------------*/

proc freq data=&OUTDATA.;
    tables final_category / nocum nopercent;
    title "GLP-1 Classification Summary";
run;

%mend;

/*==================================================================*/
/* Example Macro Call                                               */
/*==================================================================*/

%CLASSIFY_GLP1_MEMBERS(
    RXDATA   = pharmacy_claims,
    MEDDATA  = medical_claims,
    OUTDATA  = glp1_member_classification,
    STARTDT  = 2023-01-01,
    ENDDT    = 2023-12-31
);
