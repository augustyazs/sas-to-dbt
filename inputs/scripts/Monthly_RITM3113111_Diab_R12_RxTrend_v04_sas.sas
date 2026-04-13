********************************************************************************************;
***	
RITM3113111 (DAA74961) GLP-1 Rolling 12 Mn
- Target Diabetes Drugs from 'Report Template_GLP1 Diabetes and Weight Loss_20230525v2.xlsx' 
- Priority is Commercial FI, but should ultimately include: 
	Tot Commercial, Tot Medicare, Commercial FI, IFP, CORE, PREM, SG
- Non-GLP1: DM, GLP1: DM only, GLP-1: DM with WL, GLP-1: WL only, Non-GLP1: DM
- Non-GLP1: DM, GLP-1: DM only, OZEMPIC, RYBELSUS, TRULICITY, MOUNJARO, SAXENDA, WEGOVY, Non-GLP1: DM
- If Days < 30, then Rx Norm = 1, else Days / 30

Dev Comments
- 2019 to 2020 mbrshp from Netezza member_months. 2021 onward from BIS
- Recreating MbrMns/Clms from 2019 to current is too large and resource intensive
	- Keep history and refresh most recent 6 mns data
	- MbrMn History (A.R12_MM)/Clm History (A.R12_CLM) created by Diabetes_R12_rpt_JL_20230607.egp
- 6/15/23 No need to use SSC data. 
	BIS has mbrship back to 2019/2020 using SNPSHT_DT = '2021-01-01'
	Use SNPSHT_DT = MAX for 2021 to cur mbrshp (capture retroactivity)
	Recreate 2019-2020 using BIS. Now, SNPSHT_DT = MAX has mbrshp back to 202006
- 6/27/23 Research into Comm FI mbrshp (using COA)
	- Automation cannot use SDM/BIS COAs due to need for Rx categorization
	- Will add FUNDING col to MAP_COA table for FI COAs (using initial COA file from KY)
		- Will create monthly mbrshp COA check for new COAs (add to MAP_COA)
	- Add Comm FI into Rx_Trend_DB.sas to leverage/have consistent mbrshp data
- 6/28/23 Reload history
	- Use 1/2/22 SNPSHT for mbrshp history back to 201901 (R12_MM)
	- Refresh CLM on 6/28 back to 201901 (R12_CLM)

HCQA116927 GLP-1 Rolling 12 Mn - Add YTD and Weekly to Email
- 3/5/24 Dev Comments
	- Load new Drugs, Retire old list
	- Rerun claims to include new drugs (Use Drug_Load_Ini_Data_Load.egp)
	- Add code to populate YTD and Weekly R12 tbls (Macros LOOP_3/LOOP_4)
	- Add code to create email grids 

HCQA128138 GLP-1 Rolling 12 Mn 
- 4/26/24 Mtg w KW
	- KY uses 'Commercial UW Total Member' from HH Drug Card Mbrshp Rpt
	- 'Commercial UW Total Member' is all Non-ASO COAs
	- Will add 'Commercial UW Total Member' component to report.

HCQA140782 6/10/24 Remove all references to 'Commercial FI' throughout the report. 

********************************************************************************************;

%SYSMSTORECLEAR;
LIBNAME RXMAC "/sasdata/pharmacy/projects/RxReferences/macros";
options mstored symbolgen mprint sasmstore=RXMAC LRECL=max COMPRESS=YES;
%RXLIB;

%LET Request=DAA74961;
%LET dir=/sasdata3/pharmacy/projects/rxana_rpts/production/data/output/phi/GLP1_Rpts;
LIBNAME A "&dir.";
/*LIBNAME A_PREV "/sasdata/pharmacy/users/mnathe01/projects/pharmacy/automated_rpts/diabetes";*/

%LET NAME=GLP-1 Rolling 12 Months Report;

DATA _NULL_;
	DT_RPT = TODAY();
	DB_START=INTNX('MONTH',TODAY(),-12,'B'); *Begin prior 12 Months;
	DB_END=INTNX('MONTH',TODAY(),-1, 'E');	*End of prior month;
	CALL SYMPUT('DB_START',CATS("'",PUT(DB_START,YYMMDDD10.),"'"));
	CALL SYMPUT('DB_END',CATS("'",PUT(DB_END,YYMMDDD10.),"'"));
	CALL SYMPUT('DT_RPT',PUT(DT_RPT,YYMMDDN8.));
	CALL SYMPUT('RUN_DATE',PUT(DT_RPT,MMDDYYD10.));
	CALL SYMPUT('YR',PUT(TODAY(),YEAR.));
	CALL SYMPUT('START_DT',CATS(PUT(DB_START,MMDDYYS10.)));
	CALL SYMPUT('END_DT',CATS(PUT(DB_END,MMDDYYS10.)));
RUN;
%PUT &RUN_DATE. &DB_START. &DB_END. &START_DT. &END_DT. ;

DATA REPORT_PARMS;
     LENGTH SPEC  $5000.;
	 LABEL SPEC = "Specifications";OUTPUT;
	 SPEC = " Report Specifications";OUTPUT;
	 SPEC = " Request #: &Request.";OUTPUT;
	 SPEC = " Report Title: &NAME.";OUTPUT;
	 SPEC = " Objective/Purpose: Obtain data on GLP1s to improve pharmacy trend forecasting";OUTPUT;
	 SPEC = " Prior Request: Krista Yokoyama";OUTPUT;
	 SPEC = " Analyst Name: John Le";OUTPUT;
	 SPEC = " Requestor Name: Krista Yokoyama";OUTPUT;
	 SPEC = " Report Run Date: &RUN_DATE.";OUTPUT;
	 SPEC = " Report Start Date: 12/1/2019";OUTPUT;
	 SPEC = " Claim Refresh Start Date: &START_DT.";OUTPUT;
	 SPEC = " Claim Refresh End Date: &END_DT.";OUTPUT;
	 SPEC = " LOB(s): BSC Commercial and Medicare";OUTPUT;
	 SPEC = " Request Criteria: ";OUTPUT;
	 SPEC = "  - Target Diabetes Drugs from 'Report Template_GLP1 Diabetes and Weight Loss_20230525v2.xlsx'";OUTPUT;
	 SPEC = "  - Priority is Commercial UW, but should ultimately include: Tot Commercial, Tot Medicare, Commercial UW, IFP, CORE, PREM, SG";OUTPUT;
	 SPEC = "  - Non-GLP1: DM, GLP1: DM only, GLP-1: DM with WL, GLP-1: WL only, Non-GLP1: DM";OUTPUT;
	 SPEC = "  - Non-GLP1: DM, GLP-1: DM only, OZEMPIC, RYBELSUS, TRULICITY, MOUNJARO, SAXENDA, WEGOVY, Non-GLP1: DM";OUTPUT;
	 SPEC = "  - If Days < 30, then Rx Norm = 1, else Days / 30 ";OUTPUT;
	 SPEC = " Inclusion Criteria: Target Diabetes Drugs from 'Report Template_GLP1 Diabetes and Weight Loss_20230525v2.xlsx' file.";OUTPUT;
	 SPEC = " Exclusion Criteria: ";OUTPUT;
	 SPEC = " Requestor Comments: ";OUTPUT;
	 SPEC = " Analyst Comments: History is captured back to Jan 2019 minimizing processing load. Only most current 12 months of claims data is refreshed at each run. Eligibility uses the Rx Trend data which includes a 36 month look back period";OUTPUT;
RUN;


*********************************************************************************;
*** BEGIN - CREATE MM DATA FROM RX_TREND (MEMBERSHIP_SUMMARY) AND APND TO A.R12_MM;


/* Pull membership from Rx Trend data (Only 12 months in Rx_Trend) */
PROC SQL;
CONNECT to NETEZZA (server="npsexp01" database="ADH_SBX_RX_DATA" Authdomain="AD_Dom_Auth");
CREATE TABLE R12_MM_DATA AS SELECT * FROM CONNECTION TO NETEZZA
(SELECT DISTINCT YEARMO
	,SUM(COMMERCIAL_MBR_CNT) AS TOT_COMM	/* Comm */
	,SUM(COM_UW_MBR_CNT) AS TOT_COMM_UW		/* 4/26/24 HCQA128138 Add Non-UW Comm */
	,SUM(COM_FI_MBR_CNT) AS TOT_COMM_FI		/* Rx Trend use COA for FI Comm */
	,SUM(NOVATION_IMAPD) AS TOT_PHP_MDCR	/* Confirmed w KY Mdcr should incl PHP IMAPD */
	,SUM(IMAPD_MBR_CNT) + SUM(GMAPD_MBR_CNT) AS MAPD_NO_PHP
	,SUM(IMAPD_ALL_MBR_CNT) + SUM(GMAPD_MBR_CNT) AS MAPD_ALL
	,SUM(IPDP_MBR_CNT) + SUM(GPDP_MBR_CNT) AS PDP_ALL
	,SUM(TOT_MDCR_RX) AS TOT_MDCR
	,SUM(PHP_CMC_CNT) AS TOT_CMC
	,SUM(TOT_MDCR_RX) + SUM(PHP_CMC_CNT) AS TOT_MDCR_W_CMC
	,SUM(CORE_MBR_CNT) AS TOT_CORE
	,SUM(PREM_MBR_CNT) AS TOT_PREM
	,SUM(TOT_SG_MBR_CNT) AS TOT_SG
	,SUM(TOT_IFP_MBR_CNT) AS TOT_IFP
FROM MEMBERSHIP_SUMMARY GROUP BY 1 ORDER BY 1) A
;DISCONNECT FROM NETEZZA; QUIT;


/* MbrMn History (A.R12_MM)/Clm History (A.R12_CLM) created by Diabetes_R12_rpt_JL_20230607.egp*/
PROC SQL;
DELETE FROM A.R12_MM WHERE YRMO IN (SELECT DISTINCT 
INPUT(PUT(CATS(SUBSTR(YEARMO,1,4),SUBSTR(YEARMO,6,2)),$6.),YYMMN6.) AS YRMO FORMAT=YYMMN6.
FROM R12_MM_DATA);
QUIT;


/* Pull each LOB for R12_MM */
%MACRO LOB_MM(NM,LOB,SUM_COL,TBL,strWHERE);
PROC SQL;
create table &NM. as
select &LOB. as LOB, 
INPUT(PUT(CATS(SUBSTR(YEARMO,1,4),SUBSTR(YEARMO,6,2)),$6.),YYMMN6.) AS YRMO FORMAT=YYMMN6.,
sum(&SUM_COL.) as MM
from &TBL.
&strWHERE.
group by 1,2 order by 1,2; QUIT;

PROC APPEND BASE=A.R12_MM DATA=&NM.; RUN;
%MEND;

/* Use LOB_MM macro above to create LOB tables and Apnd to R12_MM*/
%LOB_MM(Comm,'Commercial',TOT_COMM,R12_MM_DATA,);
%LOB_MM(FI,'Commercial FI',TOT_COMM_FI,R12_MM_DATA,);
%LOB_MM(UW,'Commercial UW',TOT_COMM_UW,R12_MM_DATA,);
%LOB_MM(Mdcr,'Medicare',TOT_MDCR_W_CMC,R12_MM_DATA,);
%LOB_MM(CORE,'CORE',TOT_CORE,R12_MM_DATA,);
%LOB_MM(PREM,'PREM',TOT_PREM,R12_MM_DATA,);
%LOB_MM(IFP,'IFP',TOT_IFP,R12_MM_DATA,);
%LOB_MM(SG,'SG',TOT_SG,R12_MM_DATA,);

PROC SORT DATA=A.R12_MM; BY LOB YRMO; RUN;

*** END - CREATE CURRENT MM DATA AND APND TO A.R12_MM ;


***************************************************************;
*** BEGIN - CREATE R12_MM for final rpt denominator ;

%macro R12_MM(yrmo,YRMO_BEG);
proc sql;
create table r12_temp_mm as
select LOB,
	&yrmo. as yrmo FORMAT=YYMMN6.,
	sum(mm) as mm
from A.R12_MM
where yrmo BETWEEN &YRMO_BEG. AND &yrmo.
group by 1,2;

proc append data=r12_temp_mm base=r12_mm_RPT force;run;
%mend;


%MACRO R12_MM2();

PROC SQL;
CREATE TABLE R12_MN AS 
SELECT DISTINCT YRMO, 
INTNX('MONTH',YRMO,-11,'B') AS R12_BEG FORMAT=YYMMN6.
FROM A.R12_MM WHERE YRMO >= '01DEC2019'd ORDER BY YRMO; QUIT;

%OBSCHECK(R12_MN);

PROC DELETE DATA=r12_mm_RPT; RUN;

%DO CNT = 1 %TO &nobs. %BY 1;
	DATA _null_;
		SET R12_MN(OBS=&CNT.);
			CALL SYMPUTX('YRMO',YRMO);
			CALL SYMPUTX('R12_BEG',R12_BEG);
	RUN;

%R12_MM(&YRMO.,&R12_BEG.);

%END;
%MEND;

%R12_MM2;

*** END - CREATE R12_MM for final rpt denominator ;


****************************************************************;
*** BEGIN - CREATE CURRENT 12 MNS CLM DATA AND APD TO A.LOB_CLM ;

/* Target Diabetes Drugs from 'Report Template_GLP1 Diabetes and Weight Loss_20230525v2.xlsx' */

PROC SQL NOPRINT;
SELECT DISTINCT STRIP(CATS("'",NDC,"'")) INTO :NDC_DIAB SEPARATED BY ","
FROM A.R12_DIAB_DRUGS;
QUIT;
%PUT &NDC_DIAB.;

PROC SQL NOPRINT;
SELECT DISTINCT STRIP(CATS("'",NDC,"'")) INTO :NDC_GPI4 SEPARATED BY ","
FROM RX_DATA.MEDISPAN
WHERE GPI4 IN (SELECT DISTINCT GPI4 FROM A.R12_DIAB_DRUGS_GPI4);
QUIT;
%PUT &NDC_GPI4.;

/* Pull COAs */
%MACRO COA_EXTRACT(MCR_NM,VAL);
%global &MCR_NM.;
PROC SQL NOPRINT;
SELECT DISTINCT "'"||COA||"'" INTO:&MCR_NM. SEPARATED BY ","
FROM RX_DATA.MAP_COA WHERE FUNDING IN (&VAL.);
QUIT;
%PUT &&&MCR_NM.;
%MEND COA_EXTRACT;
%COA_EXTRACT(COA_FI,'FI');
%COA_EXTRACT(COA_ASO,'ASO');


/* Pull clm data for most current month period set in Date criteria step above */

PROC SQL;
CONNECT to NETEZZA 
(server="npsexp01" database="ADH_SBX_RX_DATA" Authdomain="AD_Dom_Auth");
CREATE TABLE RAWDATA AS
SELECT DISTINCT 
	CASE WHEN B.NDC IS MISSING THEN 'Non-GLP1: DM' ELSE 'Drug Type 1'n END AS drug_type1
	,CASE WHEN B.NDC IS MISSING THEN 'Non-GLP1: DM' ELSE 'Drug Type2'n END AS drug_type2
	,INPUT(PUT(A.yrmo,$6.),YYMMN6.) as YRMO FORMAT=YYMMN6.
	,YEAR(date_filled) as YR 
	,INTNX('WEEK',DATE_FILLED,0,'B') AS WEEK_OF FORMAT=DATE9.
	,A.*
FROM CONNECTION TO NETEZZA
	(SELECT	DISTINCT
	CD.lob
	,CD.lob2
	,CD.lob3
	,CASE WHEN COA in(&COA_FI.) THEN 'Y' ELSE ' ' END AS FI_IND
	,CASE WHEN COA not in(&COA_ASO.) THEN 'Y' ELSE ' ' END AS UW_IND
	,CD.GPI2_DRUG_GROUP
	,CD.GPI4
	,CD.DRUG_NDC as ndc
	,CD.drug_brand_name as brand
	,CD.GENERIC_PRODUCT_ID as gpi14
	,CD.drug_generic_name
	,CASE WHEN LENGTH(CD.MEMBER_ID) = 9 THEN CD.MEMBER_ID || '00'
		WHEN SUBSTRING(CD.MEMBER_ID,1,7) = '0000000' THEN SUBSTRING(CD.MEMBER_ID,8,11)
		ELSE CD.MEMBER_ID END AS MEMBER_ID
	,TO_CHAR(CD.date_filled,'YYYYMM') AS yrmo
	,CD.date_filled
	,CD.CLAIM_PLAN_DAYS_SUPPLY as days
/*	SUM(CEIL(CD.CLAIM_PLAN_DAYS_SUPPLY / 30.0)) AS rx_norm,	Per KY use calc, decimals ok */
	,(case when CD.CLAIM_PLAN_DAYS_SUPPLY<30 then 1 
		else CD.CLAIM_PLAN_DAYS_SUPPLY/30 end) as rx_norm
	,CD.RX_COUNT as rx
	,CD.AMT_MEMBER_LIAB as mbr_cost
	,CD.AMT_PLAN_PAID as paid
	,CD.AMT_TOTAL_COST AS AMT_TOTAL_COST
	,CD.AMT_PLAN_DISP_FEE+CD.AMT_PLAN_ING_COST as allw
					
FROM CLAIM_DETAIL_PAID CD 
													
WHERE  
	CD.DATE_FILLED BETWEEN &DB_START. AND &DB_END.
/*	CD.DATE_FILLED BETWEEN '2019-01-01' AND '2023-05-31'*/
	AND CD.DRUG_NDC IN (&NDC_DIAB.,&NDC_GPI4.) 
	AND CD.CLAIM_CICS_STATUS_CODE NOT IN ('HIS','HRV')

) A

LEFT JOIN A.R12_DIAB_DRUGS B on A.NDC=B.NDC

;DISCONNECT FROM NETEZZA; QUIT;

/* Manual fix of LOB2 for IFP/SG */
DATA RAWDATA; SET RAWDATA;
IF LOB2=' ' AND FIND(LOB3,"IFP") THEN LOB2='IFP';
ELSE IF LOB2=' ' AND FIND(LOB3,"SG") THEN LOB2='SG';
RUN;


/* Capture last run mmm yrmo */
PROC SQL; CREATE TABLE R12_CLM_CHK_RELOAD2 AS 
SELECT YRMO, count(distinct(member_id)) as util_mbrs, sum(rx) as rx
FROM RAWDATA GROUP BY 1; QUIT;


/* Macro to Loop thru LOB Assignments providing Summary Counts */
%MACRO LOB_CLM(NM,LOB,COL,TBL,strWHERE);
proc sql;
create table &NM. as
select
	&LOB. AS lob
	,drug_type1
	,drug_type2
	,&COL.	/*yrmo FORMAT=YYMMN6.*/
	,count(distinct(member_id)) as util_mbrs
	,sum(rx) as rx
	,sum(rx_norm) as rx_norm
	,sum(days)/sum(rx) as days_rx
	,sum(allw) as allw
	,sum(mbr_cost) as mbr_cost
	,sum(paid) as paid
	,sum(days) as days
from &TBL.	/*rawdata*/
&strWHERE.
group by 1,2,3,4 order by 1,2,3,4;
%MEND;

/* Loop to populate Rolling 12M, YTD, Wk Of data per each LOB Assignments */
%MACRO LOOP_3(PER,COL);

%LOB_CLM(lob1&PER.,lob,%STR(&COL.),RAWDATA,
	%STR(WHERE LOB IN ('Commercial','Medicare')));
%LOB_CLM(lob1a&PER.,%STR('Commercial FI'),%STR(&COL.),RAWDATA,
	%STR(WHERE LOB IN ('Commercial') AND FI_IND = 'Y'));
%LOB_CLM(lob2&PER.,lob2,%STR(&COL.),RAWDATA,
	%STR(WHERE LOB2 IN ('IFP','SG')));
%LOB_CLM(lob3&PER.,lob3,%STR(&COL.),RAWDATA,
	%STR(WHERE LOB3 IN ('CORE','PREM')));
%LOB_CLM(lob4&PER.,%STR('Commercial UW'),%STR(&COL.),RAWDATA,
	%STR(WHERE LOB IN ('Commercial') AND UW_IND = 'Y'));

DATA LOB_CLM_TMP&PER.; SET lob1&PER. lob1a&PER. lob2&PER. lob3&PER. lob4&PER.; RUN;

%MEND LOOP_3;

%LOOP_3(,%STR(yrmo FORMAT=YYMMN6.));
%LOOP_3(_YTD,YR);
%LOOP_3(_WK,%STR(WEEK_OF FORMAT=DATE9.));


/* Load to R12_CLM */
%MACRO LOOP_4(PER,COL);

PROC SQL;
DELETE FROM A.R12_CLM&PER. WHERE &COL. IN (SELECT DISTINCT &COL. FROM LOB_CLM_TMP&PER.);
QUIT;

PROC APPEND BASE=A.R12_CLM&PER. DATA=LOB_CLM_TMP&PER.; RUN;
PROC SORT DATA=A.R12_CLM&PER.; BY LOB &COL.; RUN;

%MEND LOOP_4;

%LOOP_4(,YRMO);
%LOOP_4(_YTD,YR);
%LOOP_4(_WK,WEEK_OF);


*** END - CREATE CURRENT 12 MNS CLM DATA                       ;
***************************************************************;

/* YTD/Weekly Rpt Out data */
%MACRO RPTOUT(NM,PER_COL,DRUG_COL,TBL,strWHERE);
PROC SQL;
CREATE TABLE RPTOUT_&NM. AS 
SELECT LOB
	,&PER_COL.
	,&DRUG_COL.
	,SUM(util_mbrs)	AS MBRS	FORMAT=COMMA15.	LABEL='Util Mbrs' 
	,SUM(paid)		AS PAID	FORMAT=COMMA15.	LABEL='Plan Paid' 
	,SUM(rx)		AS RXS	FORMAT=COMMA15. LABEL='RXs'
FROM A.R12_CLM&TBL.
&strWHERE.
GROUP BY 1,2,3;
QUIT;
%MEND RPTOUT;

%RPTOUT(YR_D1,%STR(YR LABEL='Yearly'),%STR(drug_type1 LABEL='Drug Type1'),_YTD,);
%RPTOUT(YR_D2,%STR(YR LABEL='Yearly'),%STR(drug_type2 LABEL='Drug Type2'),_YTD,);
%RPTOUT(WK_D1,%STR(WEEK_OF FORMAT=MMDDYYD10. LABEL='Week Of'),%STR(drug_type1 LABEL='Drug Type1')
	,_WK,%STR(WHERE WEEK_OF >= INTNX('WEEK',TODAY(),-6,'B')));
%RPTOUT(WK_D2,%STR(WEEK_OF FORMAT=MMDDYYD10. LABEL='Week Of'),%STR(drug_type2 LABEL='Drug Type2')
	,_WK,%STR(WHERE WEEK_OF >= INTNX('WEEK',TODAY(),-6,'B')));


***************************************************************;
*** BEGIN - CREATE R12_CLM for final rpt numerator ;

%macro R12_CLM(yrmo,YRMO_BEG);
proc sql;
create table r12_temp_clm as
select
LOB,
drug_type1,
drug_type2,
&yrmo. as yrmo FORMAT=YYMMN6.,
sum(util_mbrs) as tot_util_mbrs, /*sum of util_mbrs for 12 months - mbr can be counted up to 12x*/
mean(util_mbrs) as avg_util_mbrs,
sum(rx) as rx,
sum(rx_norm) as rx_norm,
sum(days)/sum(rx) as days_rx,
sum(allw) as allw,
sum(mbr_cost) as mbr_cost,
sum(paid) as paid,
sum(allw)/sum(rx_norm) as allw_rx_norm
from A.R12_CLM
where yrmo BETWEEN &YRMO_BEG. AND &yrmo.
group by 1,2,3;

	proc append data=r12_temp_clm base=R12_CLM_RPT force;run;

%mend;

%MACRO R12_CLM2();

PROC DELETE DATA=R12_CLM_RPT; RUN;

PROC SQL;
CREATE TABLE R12_MN AS 
SELECT DISTINCT YRMO, 
INTNX('MONTH',YRMO,-11,'B') AS R12_BEG FORMAT=YYMMN6.
FROM A.R12_CLM WHERE YRMO >= '01DEC2019'd ORDER BY YRMO; QUIT;

%OBSCHECK(R12_MN);

%DO CNT = 1 %TO &nobs. %BY 1;
	DATA _null_;
		SET R12_MN(OBS=&CNT.);
			CALL SYMPUTX('YRMO',YRMO);
			CALL SYMPUTX('R12_BEG',R12_BEG);
	RUN;

%R12_CLM(&YRMO.,&R12_BEG.);

%END;
%MEND;

%R12_CLM2;

*** END - CREATE R12_CLM_RPT for final rpt numerator ;


/*         ***********************************                   */                    
/***************** OUTPUT TO EXCEL *******************************/
/*         ***********************************                   */

proc sql;
create table r12_report as
select
a.LOB,
drug_type1,
drug_type2,
a.yrmo,
tot_util_mbrs, /*sum of util_mbrs for 12 months - mbr can be counted up to 12x*/
avg_util_mbrs,
mm,
rx,
rx_norm,
days_rx,
allw,
mbr_cost,
paid,
allw_rx_norm,
tot_util_mbrs/mm as tot_mbr_util_perc, /*% Mbrs Utilizing */
avg_util_mbrs/(mm/12) as avg_mbr_util_perc,/*% Mbrs Utilizing */
(rx/mm)*1000 as rx_pkmpy, /* Rx PKMPY */
(rx_norm/mm)*1000 as rxnorm_pkmpy, /* RxNorm PKMPY */
allw/mm as allw_pmpm, /*Allowed PMPM */
mbr_cost/mm as mbr_cost_pmpm, /* Mbr Cost PMPM */
paid/mm as paid_pmpm /* Paid PMPM */

from R12_CLM_RPT a left join R12_MM_RPT b
	on a.lob=b.lob and a.yrmo=b.yrmo
;QUIT;


*************************************************************************************;

/* BEGIN - HCQA140782 6/10/24 Remove all references to 'Commercial FI' */
%MACRO REMOVE_FI(TBL,SRC);
DATA &TBL.; SET &SRC.; WHERE LOB<>'Commercial FI'; RUN;
%MEND;

%REMOVE_FI(r12_report,r12_report)
%REMOVE_FI(R12_MM,A.R12_MM);
%REMOVE_FI(R12_CLM,A.R12_CLM);
%REMOVE_FI(RPTOUT_YR_D1,RPTOUT_YR_D1);
%REMOVE_FI(RPTOUT_WK_D1,RPTOUT_WK_D1);
%REMOVE_FI(RPTOUT_YR_D2,RPTOUT_YR_D2);
%REMOVE_FI(RPTOUT_WK_D2,RPTOUT_WK_D2);
/* END - HCQA140782 6/10/24 Remove all references to 'Commercial FI' */

*************************************************************************************;

options nocenter;
ODS EXCEL FILE="&DIR/&Request. &NAME._&DT_RPT..xlsx" STYLE=NORMAL;

ods excel OPTIONS (sheet_interval="none" Sheet_Name = "Report Specs" 
ABSOLUTE_COLUMN_WIDTH='150' embedded_titles='no' embedded_footnotes='no');

%output(REPORT_PARMS); *Call Output Macro;

%NEW_SHEET

ODS EXCEL OPTIONS(sheet_interval="proc" EMBEDDED_TITLES='YES' SHEET_NAME="Rolling 12 Months" flow="tables" 
frozen_headers = 'YES' autofilter= 'YES' absolute_column_width='10,15,15,8,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12');
PROC PRINT DATA=r12_report LABEL NOOBS split='*';
/*style(header) = {background=bibg};*/
TITLE1 "Diabetes: Rolling 12 Months";
TITLE2 "Report Period: 12/01/2019 - &END_DT.";
VAR _ALL_ /*/ style(column)=data[width=100%]*/;
FORMAT 
util_mbrs COMMA20.0
mm COMMA20.0
rx COMMA20.0
rx_norm COMMA20.0
days_rx COMMA20.0
allw DOLLAR20.2
mbr_cost DOLLAR20.2
paid DOLLAR20.2
allw_rx_norm DOLLAR20.2
tot_mbr_util_perc PERCENT10.1 /*% Mbrs Utilizing */
avg_mbr_util_perc PERCENT10.1 /*% Mbrs Utilizing */
rx_pkmpy COMMA20.0 /* Rx PKMPY */
rxnorm_pkmpy COMMA20.0 /* RxNorm PKMPY */
allw_pmpm DOLLAR20.2 /*Allowed PMPM */
mbr_cost_pmpm DOLLAR20.2 /* Mbr Cost PMPM */
paid_pmpm DOLLAR20.2 ;
LABEL
LOB				="LOB"
yrmo			="R12 End Period"
drug_type1		="DRUG TYPE1"
drug_type2		="DRUG TYPE2"
util_mbrs		="UTIL MBRS"
rx				="TOT RX"
rx_norm			="TOT RX NORM"
days_rx			="DAYS PER RX"
allw			="TOT ALLOWED"
mbr_cost		="TOT MBR COST"
paid			="TOT PLAN PAID"
allw_rx_norm	="ALLOWED PER RX NORM"
mm				="MBRSHP"
tot_mbr_util_perc	="% TOT Utilizing Mbrs*Over TOT Mbr Mths"
avg_mbr_util_perc	="% AVG Utilizing Mbrs*AVG Mbr Mths"
rx_pkmpy		="Rx PKMPY"
rxnorm_pkmpy	="RxNorm PKMPY"
allw_pmpm		="Allowed PMPM"
mbr_cost_pmpm	="Mbr Cost PMPM"
paid_pmpm		="Paid PMPM"
; RUN;


ods excel OPTIONS (sheet_interval="none" sheet_name="Target Drugs" embedded_titles='no' 
embedded_footnotes='no' absolute_column_width='8,35,14,16,34,9,9');
%output(A.R12_DIAB_DRUGS_GPI4); *Call Output Macro;

ods excel OPTIONS (sheet_interval="none" sheet_name="Target Drugs" embedded_titles='no' 
embedded_footnotes='no' Autofilter = "Yes" absolute_column_width='8,35,14,16,34,9,9');
%output(A.R12_DIAB_DRUGS); *Call Output Macro;

/*ods excel OPTIONS (sheet_interval="proc" Sheet_Name = "Fully Insured COAs" autofilter= 'yes' */
/*embedded_titles='no' embedded_footnotes='no' absolute_column_width='8,9,10,15,10,18,7,13,10,8');*/
/*%output(COA_FI); *Call Output Macro;*/

ods excel OPTIONS (sheet_interval="proc" Sheet_Name = "Member Month Totals" autofilter= 'yes' 
embedded_titles='no' embedded_footnotes='no' absolute_column_width='20,8,14');
%output(R12_MM); *Call Output Macro;

ods excel OPTIONS (sheet_interval="proc" Sheet_Name = "Claim Totals By Month" autofilter= 'yes' 
embedded_titles='no' embedded_footnotes='no' absolute_column_width='10,15,15,8,12,12,12,12,12,12,12,12,12');
%output(R12_CLM); *Call Output Macro;

ODS EXCEL CLOSE;


OPTIONS NOCENTER;

proc template;
 define style Custom;
 parent = Styles.HTMLBLUE;
 STYLE Header /
 FONT_FACE = "Century Gothic"
 FONT_SIZE = 9pt
 FONT_WEIGHT = medium
;

class systitleandfootercontainer /
      htmlstyle="border:none";

STYLE SystemTitle /
 FONT_FACE = "Century Gothic"
 FONT_SIZE = 12pt
 FONT_WEIGHT = bold
;

 STYLE SystemFooter /
 FONT_FACE = "Century Gothic"
 FONT_SIZE = 12pt
;

 STYLE Table /
 FONT_FACE = "Century Gothic"
 FONT_SIZE = 9pt
 FONT_WEIGHT = medium
 backgroundcolor = #f0f0f0
 bordercolor = black
 borderstyle = solid
 borderwidth = 1pt
 cellpadding = 5pt
 cellspacing = 0pt
 frame = void
 rules = groups 
;

 STYLE Data /
 FONT_FACE = "Century Gothic"
 FONT_SIZE = 9pt
 FONT_WEIGHT = medium
;

 STYLE Body /
 FONT_FACE = "Century Gothic"
 FONT_SIZE = 10pt
 FONT_WEIGHT = medium
;
END;
RUN;


%LET EMAIL_DISTRO = 'Krista.Yokoyama@blueshieldca.com' 'Kevin.Wu@blueshieldca.com';	/**/
%LET EMAIL_CC ='John.Le@blueshieldca.com'; /**/

ods _all_ close;

OPTIONS missing='-' NOCENTER NOBYLINE;

OPTIONS NOCENTER;

ODS _ALL_ CLOSE;
TITLE;

FILENAME mymail email to=(&EMAIL_DISTRO.)
					  CC=(&EMAIL_CC.)
subject="&NAME. &DT_RPT."
attach=("&DIR/&Request. &NAME._&DT_RPT..xlsx" lrecl=32767 content_type="application/xlsx")
type="text/html" lrecl=5000
;

ods listing close;
ods html body=mymail rs=none style=CUSTOM;
TITLE;
FOOTNOTE;

proc odstext;
p "<b>This e-mail contains Summarized Data over Paid Pharmacy claims for specified LOBs.</b>";
p "<b>For targeted GLP-1 Drugs (see attachment for list of drugs).</b>";
p "<b>Yearly Summary includes claims from 01/01/2019 to &END_DT.</b>";
p "<b>Weekly Summary includes claims from most current 6 weeks.</b>";
run;

proc odstext;
   p "<b><u>Yearly Summary for Drug Type1</b></u>";
 run;

PROC REPORT DATA=RPTOUT_YR_D1 missing nowd;
column LOB drug_type1 YR,(MBRS PAID RXS);
DEFINE LOB / GROUP NOZERO ORDER=INTERNAL;
DEFINE drug_type1 / GROUP NOZERO ORDER=INTERNAL;
DEFINE YR / ACROSS ORDER=INTERNAL;
BREAK AFTER LOB / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];
RBREAK AFTER / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];

compute LOB;
LINE_COUNT +1;
IF MOD(LINE_COUNT,2) = 0 then do;
CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXCCFFFF]");
end;
ELSE CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXFFFFFF]");
endcomp;

COMPUTE  drug_type1;
IF _BREAK_ = 'LOB' THEN drug_type1 = 'Sub-totals';
ELSE IF _BREAK_ = '_RBREAK_' THEN DO; LOB='All'; drug_type1 = 'Grand Total'; END;
ENDCOMP;
RUN;


proc odstext;
   p "<b><u>Weekly Summary for Drug Type1</b></u>";
 run;

PROC REPORT DATA=WORK.RPTOUT_WK_D1 missing nowd;
column LOB drug_type1 WEEK_OF,(MBRS PAID RXS);
DEFINE LOB / GROUP NOZERO ORDER=INTERNAL;
DEFINE drug_type1 / GROUP NOZERO ORDER=INTERNAL;
DEFINE WEEK_OF / ACROSS ORDER=INTERNAL;
BREAK AFTER LOB / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];
RBREAK AFTER / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];

compute LOB;
LINE_COUNT +1;
IF MOD(LINE_COUNT,2) = 0 then do;
	CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXCCFFFF]");
	end;
ELSE CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXFFFFFF]");
endcomp;

COMPUTE  drug_type1;
IF _BREAK_ ='LOB' THEN drug_type1 = 'Sub-totals';
ELSE IF _BREAK_ = '_RBREAK_' THEN DO; LOB='All'; drug_type1 = 'Grand Total'; END;
ENDCOMP;
RUN;


proc odstext;
   p "<b><u>Yearly Summary for Drug Type2</b></u>";
 run;

PROC REPORT DATA=RPTOUT_YR_D2 missing nowd;
column LOB drug_type2 YR,(MBRS PAID RXS);
DEFINE LOB / GROUP NOZERO ORDER=INTERNAL;
DEFINE drug_type2 / GROUP NOZERO ORDER=INTERNAL;
DEFINE YR / ACROSS ORDER=INTERNAL;
BREAK AFTER LOB / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];
RBREAK AFTER / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];

compute LOB;
LINE_COUNT +1;
IF MOD(LINE_COUNT,2) = 0 then do;
CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXCCFFFF]");
end;
ELSE CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXFFFFFF]");
endcomp;

COMPUTE  drug_type2;
IF _BREAK_ = 'LOB' THEN drug_type2 = 'Sub-totals';
ELSE IF _BREAK_ = '_RBREAK_' THEN DO; LOB='All'; drug_type2 = 'Grand Total'; END;
ENDCOMP;
RUN;


proc odstext;
   p "<b><u>Weekly Summary for Drug Type2</b></u>";
 run;

PROC REPORT DATA=WORK.RPTOUT_WK_D2 missing nowd;
column LOB drug_type2 WEEK_OF,(MBRS PAID RXS);
DEFINE LOB / GROUP NOZERO ORDER=INTERNAL;
DEFINE drug_type2 / GROUP NOZERO ORDER=INTERNAL;
DEFINE WEEK_OF / ACROSS ORDER=INTERNAL;
BREAK AFTER LOB / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];
RBREAK AFTER / summarize skip ol style=[foreground=black font=("Century Gothic",10pt,bold)];

compute LOB;
LINE_COUNT +1;
IF MOD(LINE_COUNT,2) = 0 then do;
	CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXCCFFFF]");
	end;
ELSE CALL DEFINE(_ROW_, "STYLE","STYLE=[BACKGROUND=CXFFFFFF]");
endcomp;

COMPUTE  drug_type2;
IF _BREAK_ ='LOB' THEN drug_type2 = 'Sub-totals';
ELSE IF _BREAK_ = '_RBREAK_' THEN DO; LOB='All'; drug_type2 = 'Grand Total'; END;
ENDCOMP;
RUN;

proc odstext;
   p "Please contact me with any questions.";
   p " ";
   p "Regards,";
   p "John Le";
   p "_____________________________________";
   p "This message (including any attachments) contains business proprietary/confidential information intended for a specific individual and purpose, and is protected by law.";
   p "If you are not the intended recipient, you should delete this message. Any disclosure, copying, or distribution of this message, or the taking of any action based on it, without the express permission of the originator, is strictly prohibited.";
run;

ods html close;
