********************************************************************************************;
***	
RITM3113117 (DAA74957) GLP-1 Weekly Diabetes and Weight Loss
Duplicate Therapy criteria 		
	- Monthly refresh
	- Please check for 45-day overlap at the GPI-10 and GPI-14 level		
	- GLP-1s:	PRODUCT_NAME	GPI10
				ADLYXIN			2717005600
				BYDUREON		2717002000
				BYETTA			2717002000
				MOUNJARO		2717308000
				OZEMPIC			2717007000
				RYBELSUS		2717007000
				SAXENDA			6125205000
				TRULICITY		2717001500
				VICTOZA			2717005000
				WEGOVY			6125207000

Dev Comments
	7/10/23 Use RITM3113113 (DAA74960) GLP-1 Weekly Diab as templ
	- Some drugs have same GPI_10, need to use GPI12 to diff
	- Clms exp from 1/1/22 to current
	7/13/23 
	- MBR_SUM has dups in LOB due to timing, take latest fill dt LOB
	- For overlap Therapies, limit clms to only mbrs w >1 DrugType1
	7/14/23 
	- Use LAG function to compare prior DrugType2, fill+days to cur DrugType2
	- Some COAs missing on clm, cannot use MAP_COA. LOB in CD using CAG logic so no blanks
	- Seeing variance in SAS data vs sample xls. Review shows SAS data good.
	- Use PROC RPT to recreate PIVOT in sample Xls
********************************************************************************************;

%SYSMSTORECLEAR;
LIBNAME RXMAC "/sasdata/pharmacy/projects/RxReferences/macros";
options mstored symbolgen mprint sasmstore=RXMAC LRECL=max COMPRESS=YES;
%RXLIB;

%LET Request=DAA74957;
%LET dir=/sasdata3/pharmacy/projects/rxana_rpts/production/data/output/phi/GLP1_Rpts;
LIBNAME A "&dir.";
/*LIBNAME A_PREV "/sasdata/pharmacy/users/mnathe01/projects/pharmacy/automated_rpts/diabetes";*/

%LET NAME=Duplicate Therapy Report - GLP1s for Diabetes and Weight Loss;

DATA _NULL_;
	DT_RPT = TODAY();
	DB_START='01JAN2022'd; *From 2022 to cur;
	DB_END=INTNX('MONTH',TODAY(),-1, 'E');	*End of prior month;
	CALL SYMPUT('DB_START',CATS("'",PUT(DB_START,YYMMDDD10.),"'"));
	CALL SYMPUT('DB_END',CATS("'",PUT(DB_END,YYMMDDD10.),"'"));
	CALL SYMPUT('DT_RPT',PUT(DT_RPT,YYMMDDN8.));
	CALL SYMPUT('RUN_DATE',PUT(DT_RPT,MMDDYYS10.));
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
	 SPEC = " Report Start Date: 1/1/2023";OUTPUT;
	 SPEC = " Claim Refresh Start Date: &START_DT.";OUTPUT;
	 SPEC = " Claim Refresh End Date: &END_DT.";OUTPUT;
	 SPEC = " LOB(s): BSC Commercial and Medicare";OUTPUT;
	 SPEC = " Request Criteria: ";OUTPUT;
	 SPEC = " Duplicate Therapy criteria ";OUTPUT;
	 SPEC = "   1. Please check for 45-day overlap at the GPI-10 and GPI-14 level ";OUTPUT;
	 SPEC = "   2. GLP-1s:	PRODUCT_NAME	GPI10 ";OUTPUT;
	 SPEC = "   	ADLYXIN		2717005600 ";OUTPUT;
	 SPEC = "   	BYDUREON	2717002000 ";OUTPUT;
	 SPEC = "   	BYETTA		2717002000 ";OUTPUT;
	 SPEC = "   	MOUNJARO	2717308000 ";OUTPUT;
	 SPEC = "   	OZEMPIC		2717007000 ";OUTPUT;
	 SPEC = "   	RYBELSUS	2717007000 ";OUTPUT;
	 SPEC = "   	SAXENDA		6125205000 ";OUTPUT;
	 SPEC = "   	TRULICITY	2717001500 ";OUTPUT;
	 SPEC = "   	VICTOZA		2717005000 ";OUTPUT;
	 SPEC = "   	WEGOVY		6125207000 ";OUTPUT;
	 SPEC = "   3. I think this can be a monthly refresh ";OUTPUT;
	 SPEC = " Inclusion Criteria: ";OUTPUT;
	 SPEC = " Exclusion Criteria: ";OUTPUT;
	 SPEC = " Requestor Comments: See the existing file I created for Nhi here:
\\bsc\hcs\Phrmapps\PS&A_Rxteam\Pharmacy CoHC\GLP1s Diabetes and Weight Loss_Feb2023\Clinical_UM
Concurrent GLP1 Utilization_Jan2021-Feb2023_20230309.xlsx";OUTPUT;
	 SPEC = " Analyst Comments: Need to use GPI12 to separate OZEMPIC/RYBELSUS ";OUTPUT;
RUN;


****************************************************************;
*** Utility Macros;
%MACRO CHK_COL(OUT,COL,TBL);
PROC SQL;
CREATE TABLE CKCOL_&OUT. AS 
SELECT &COL.,COUNT(*) AS CNT FROM &TBL.
GROUP BY 1; QUIT;
%MEND;

%MACRO CHK_DUP(OUT,COL,TBL);
PROC SQL;
CREATE TABLE CKDUP_&OUT. AS
SELECT * FROM &TBL.
WHERE &COL. IN 
	(SELECT &COL. FROM &TBL. GROUP BY 1 HAVING COUNT(*)>1)
ORDER BY &COL.; QUIT;
%MEND;


****************************************************************;
*** BEGIN - Target drugs and claims;

/* Target Drugs from 'Report Template_GLP1 Diabetes and Weight Loss_20230525v2.xlsx' 
	5. Duplicate Therapy worksheet */
%LET CRIT_GPI10='2717005600','2717002000','2717308000','2717007000','6125205000'
	,'2717001500','2717005000','6125207000';
%PUT &CRIT_GPI10.;

PROC SQL NOPRINT;
SELECT DISTINCT STRIP(CATS("'",NDC,"'")) INTO :NDC_GPI10 SEPARATED BY ","
FROM RX_DATA.MEDISPAN
WHERE GPI10 IN (&CRIT_GPI10.);
QUIT;
%PUT &NDC_GPI10.;

PROC SQL;
CREATE TABLE DRUG_TYPE AS 
SELECT  
	SUBSTR(GENERIC_PRODUCT_ID,1,10) AS GPI10
	,SUBSTR(GENERIC_PRODUCT_ID,1,12) AS GPI12
	,'Drug Type 1'n	AS drug_type1
	,'Drug Type2'n	AS drug_type2
FROM A.R12_DIAB_DRUGS;
QUIT;


/* Pull clm data for most current month period set in Date criteria step above */
PROC SQL;
CONNECT to NETEZZA 
(server="npsexp01" database="ADH_SBX_RX_DATA" Authdomain="AD_Dom_Auth");
CREATE TABLE RAWDATA AS
SELECT DISTINCT 
	B.drug_type1
	,B.drug_type2
	,A.*
FROM CONNECTION TO NETEZZA
	(SELECT	DISTINCT
	CASE WHEN LENGTH(CD.MEMBER_ID) = 9 THEN CD.MEMBER_ID || '00'
		WHEN SUBSTRING(CD.MEMBER_ID,1,7) = '0000000' THEN SUBSTRING(CD.MEMBER_ID,8,11)
		ELSE CD.MEMBER_ID END AS MEMBER_ID,
	CD.LOB,
	CD.DATE_FILLED,
	CD.CLAIM_NUMBER,
	CD.LOB2,
	CD.LOB3,
	CD.DRUG_NDC as NDC,
	CD.DRUG_LABEL_NAME as LABEL_NAME,
	CD.DRUG_BRAND_NAME as BRAND_NAME,
	CD.RX_COUNT		AS RXS,
	CD.CLAIM_PLAN_DAYS_SUPPLY	AS DAYS,
	CD.CLAIM_PLAN_MET_DEC_QTY	AS QTY,
	CD.AMT_PLAN_ING_COST		AS ING_COST,
	CD.AMT_PLAN_DISP_FEE		AS DISP_FEE,
	CD.AMT_TOTAL_COST			AS ALLOWED,
	CD.AMT_MEMBER_COPAY			AS COPAY,
	CD.AMT_MEMBER_DED_COST		AS DEDUCT,
	CD.AMT_MEMBER_LIAB			AS MBR_COST,
	CD.AMT_PLAN_PAID			AS PLAN_PAID,

	SUBSTRING(CD.GENERIC_PRODUCT_ID,1,10)	AS GPI10,
	SUBSTRING(CD.GENERIC_PRODUCT_ID,1,12)	AS GPI12,
	CD.GENERIC_PRODUCT_ID as GPI14

				
FROM CLAIM_DETAIL_PAID CD 
													
WHERE  
	CD.DATE_FILLED BETWEEN &DB_START. AND &DB_END.
	AND CD.DRUG_NDC IN (&NDC_GPI10.) 
	AND CD.CLAIM_CICS_STATUS_CODE NOT IN ('HIS','HRV')

) A

LEFT JOIN DRUG_TYPE B on A.GPI12=B.GPI12

;DISCONNECT FROM NETEZZA; QUIT;

/* Audits */
/*%CHK_COL(LOB,LOB,RAWDATA);*/
/*%CHK_COL(GPI12,GPI12,RAWDATA);*/
/*%CHK_COL(drug_type1,drug_type1,RAWDATA);*/
/*%CHK_DUP(CLAIM_NUMBER,CLAIM_NUMBER,RAWDATA);*/
/*%CHK_COL(BRAND_NAME,BRAND_NAME,RAWDATA);*/

*** END - Target drugs and claims;
****************************************************************;


*****************************************************************************;
/* MBR SUMMARY - Begin */

PROC SQL;
CREATE TABLE MBR_SUM AS 
SELECT A.MEMBER_ID
,CASE WHEN A.LOB ='PHP CMC' THEN 'Medicare' ELSE A.LOB END AS LOB
,CASE WHEN D.MEMBER_ID IS NOT MISSING THEN 'Y' ELSE 'N' END AS DM_ONLY	LABEL='GLP-1: DM only'
,CASE WHEN DW.MEMBER_ID IS NOT MISSING THEN 'Y' ELSE 'N' END AS DM_W_WL	LABEL='GLP-1: DM w WL'
,CASE WHEN W.MEMBER_ID IS NOT MISSING THEN 'Y' ELSE 'N' END AS WL_ONLY	LABEL='GLP-1: WL only'
,MAX(DATE_FILLED)	AS MAX_FILL		FORMAT=MMDDYYS10.

FROM RAWDATA A

LEFT JOIN (SELECT DISTINCT MEMBER_ID FROM RAWDATA WHERE drug_type1='GLP-1: DM ONLY') D
	ON A.MEMBER_ID=D.MEMBER_ID

LEFT JOIN (SELECT DISTINCT MEMBER_ID FROM RAWDATA WHERE drug_type1='GLP-1: DM w WL') DW
	ON A.MEMBER_ID=DW.MEMBER_ID

LEFT JOIN (SELECT DISTINCT MEMBER_ID FROM RAWDATA WHERE drug_type1='GLP-1: WL only') W
	ON A.MEMBER_ID=W.MEMBER_ID
GROUP BY 1,2,3,4,5
ORDER BY MEMBER_ID, MAX_FILL;
QUIT;


%CHK_DUP(MBR_SUM,MEMBER_ID,MBR_SUM); * Dups exist due to mbrs chg from Comm to Mdcr;
/* Keep only most current LOB */
PROC SORT DATA=MBR_SUM; BY MEMBER_ID MAX_FILL; RUN;
DATA MBR_SUM; SET MBR_SUM; BY MEMBER_ID MAX_FILL; IF LAST.MEMBER_ID; RUN;


/* GLP1 Utilization and GLP Therapy Logic
D 	D/WL WL		GLP1 Utilization	GLP Therapy
No	No	 Yes	Monotherapy			GLP-1: WL only
No	Yes	 No		Monotherapy			GLP-1: DM w WL
Yes	No	 No		Monotherapy			GLP-1: DM only
No	Yes	 Yes	Dual Therapy		GLP-1: DM w WL + GLP-1: WL only
Yes	No	 Yes	Dual Therapy		GLP-1: DM only + GLP-1: WL only
Yes	Yes	 No		Dual Therapy		GLP-1: DM only + GLP-1: DM w WL
Yes	Yes	 Yes	Triple Therapy		All GLP1	*/

DATA MBR_SUM; SET MBR_SUM; 
FORMAT UTIL $15. THER $35.;
LABEL UTIL='GLP1 Utilization' THER='GLP Therapy';
* UTIL Assign;
IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('NNY','NYN','YNN') THEN UTIL='Monotherapy';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('NYY','YNY','YYN') THEN UTIL='Dual Therapy';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('YYY') THEN UTIL='Triple Therapy';
* THER Assign;
IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('NNY') THEN THER='GLP-1: WL only';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('NYN') THEN THER='GLP-1: DM w WL';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('YNN') THEN THER='GLP-1: DM only';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('NYY') THEN THER='GLP-1: DM w WL + GLP-1: WL only';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('YNY') THEN THER='GLP-1: DM only + GLP-1: WL only';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('YYN') THEN THER='GLP-1: DM only + GLP-1: DM w WL';
ELSE IF CATS(DM_ONLY,DM_W_WL,WL_ONLY) IN ('YYY') THEN THER='All GLP1';
RUN;

/* MBR SUMMARY - End */
*****************************************************************************;


*****************************************************************************;
/* Potential Concurrent GLP1 Utilizers - Begin */

PROC SQL;
CREATE TABLE MBR_GLP AS 
SELECT MEMBER_ID, drug_type1, SUM(RXS) AS RXS
FROM RAWDATA GROUP BY 1,2;
QUIT;

/* Dups in MBR_GLP will identify Mbrs w >1 GLP */
%CHK_DUP(MBR_GLP,MEMBER_ID,MBR_GLP); 

/* Limit clms to only members with >1 GLP drugs for analysis */
PROC SQL;
CREATE TABLE CLMS_CONCUR AS 
SELECT * FROM RAWDATA
WHERE MEMBER_ID IN (SELECT DISTINCT MEMBER_ID FROM CKDUP_MBR_GLP);
QUIT;


/* Correct formatting */
%MACRO CHANGE(LIB,DS);
PROC DATASETS LIB=&LIB.;
MODIFY &DS.;
FORMAT DATE_FILLED MMDDYYS10.
DAYS COMMA10.0
ING_COST DISP_FEE ALLOWED COPAY DEDUCT MBR_COST PLAN_PAID DOLLAR20.2;
QUIT;
%MEND;
%CHANGE(WORK,CLMS_CONCUR);

/* Identify GLP Overlap */
PROC SORT DATA=CLMS_CONCUR; BY MEMBER_ID DATE_FILLED; RUN;

/* Use LAG to pull prior drug, fill date, and days */
/* Compare fill date to prior fill date + days */
/* Assign MBR_NO to each mbr */
DATA CLMS_CONCUR;
SET CLMS_CONCUR;
RETAIN MBR_NO 0;
BY MEMBER_ID DATE_FILLED;
/*MBR_NO=0;*/

FORMAT DRG_PRIOR $15. DRG_PRIOR_END MMDDYYS10. OVERLAP $1.;

IF FIRST.MEMBER_ID THEN MBR_NO = SUM(MBR_NO,1);

/* Assign prior fill end */
DRG_PRIOR=LAG(drug_type2);
DRG_PRIOR_END=INTNX('DAY',LAG(DATE_FILLED),LAG(DAYS));

/* If same mbr and drug_type2 diff then apply logic */
IF NOT FIRST.MEMBER_ID AND drug_type2 NE DRG_PRIOR THEN DO;
	IF DATE_FILLED >= DRG_PRIOR_END THEN OVERLAP = 'N';
/* If fill is less then prior fill end then overlap */
	ELSE IF DATE_FILLED < DRG_PRIOR_END THEN OVERLAP = 'Y';
END;

RUN;

/* Clm Rpt Out */
PROC SQL;
CREATE TABLE CLM_RPTOUT AS 
SELECT 

A.MBR_NO		
,A.MEMBER_ID	
,CASE WHEN C.MEMBER_ID IS NOT MISSING THEN 'Concurrent GLP1s' 
	ELSE 'Not Concurrent GLP1s' END AS GLP_OVERLAP	LABEL='GLP OVERLAP'
,B.THER				LABEL='GLP1 THERAPY'
,B.LOB
,A.drug_type1		LABEL='DRUG TYPE1'
,A.drug_type2		LABEL='DRUG TYPE2'
,A.OVERLAP	
,A.DRG_PRIOR		LABEL='PRIOR DRUG'
,A.DRG_PRIOR_END	LABEL='PRIOR DRUG END DATE'
,A.DATE_FILLED		
,A.DAYS	
,A.*

FROM CLMS_CONCUR A

LEFT JOIN MBR_SUM B		ON A.MEMBER_ID=B.MEMBER_ID

LEFT JOIN (SELECT DISTINCT MEMBER_ID FROM CLMS_CONCUR WHERE OVERLAP='Y') C
	ON A.MEMBER_ID=C.MEMBER_ID
ORDER BY MEMBER_ID, DATE_FILLED;
QUIT;


/* Data for Mbr Summ Pivot */
PROC SQL;
CREATE TABLE MBR_SUM_RPTOUT AS
SELECT
	CASE WHEN UTIL='Monotherapy' THEN 1
		WHEN UTIL='Dual Therapy' THEN 2
		WHEN UTIL='Triple Therapy' THEN 3 END AS INT_ORDER
	,UTIL		FORMAT=$50. LENGTH=50
	,DM_ONLY
	,DM_W_WL
	,WL_ONLY
	,LOB		FORMAT=$50. LENGTH=50
	,COUNT(DISTINCT MEMBER_ID) AS MBRS
FROM MBR_SUM GROUP BY 1,2,3,4,5,6 ORDER BY INT_ORDER;

CREATE TABLE CLM_SUM_RPTOUT AS
SELECT  
	LOB				FORMAT=$50. LENGTH=50
	,GLP_OVERLAP	FORMAT=$50. LENGTH=50
	,THER			FORMAT=$50. LENGTH=50
	,COUNT(DISTINCT MEMBER_ID) AS MBRS
	,SUM(RXS) AS RXS
	,SUM(ALLOWED) AS ALLOWED
	,SUM(PLAN_PAID) AS PAID
FROM CLM_RPTOUT 
GROUP BY 1,2,3;
QUIT;


options nocenter;
ODS EXCEL FILE="&DIR/&Request. &NAME. &DT_RPT..xlsx" STYLE=htmlblue;

ods excel OPTIONS (sheet_interval="none" Sheet_Name = "Report Specs" 
ABSOLUTE_COLUMN_WIDTH='70' embedded_titles='no' embedded_footnotes='no');

%output(REPORT_PARMS); *Call Output Macro;

%NEW_SHEET

ODS EXCEL OPTIONS (sheet_interval='Proc' Sheet_Name = "GLP1 Member Counts" embedded_titles='Yes' 
	Autofilter = 'Yes' absolute_column_width='20,20,20,20,15,15,17,15,15,20');

TITLE1 "&NAME."; 
TITLE2 "Fill Dates: &START_DT. - &END_DT.";

FOOTNOTE "Report Run Date: &RUN_DATE.";

/*OPTIONS MISSING='0';*/

PROC REPORT DATA=MBR_SUM_RPTOUT NOWD MISSING style=excel;

TITLE3 "All GLP1 Utilizers";

COLUMNS UTIL DM_ONLY DM_W_WL WL_ONLY
	MBRS, ('-Utilizing Members-' LOB)
	MBRS=TOT_MBRS
	MBRS=MBRS_PERC, ('-% Members-' LOB)
	MBRS=TOT_MBRS_PERC
;

DEFINE UTIL / 	 'GLP Therapy' GROUP ORDER=DATA;
DEFINE DM_ONLY / COMPUTED 'GLP-1: DM only' GROUP;
DEFINE DM_W_WL / COMPUTED 'GLP-1: DM w WL' GROUP;
DEFINE WL_ONLY / COMPUTED 'GLP-1: WL only' GROUP;

DEFINE LOB / ACROSS ' '; 

DEFINE MBRS / ANALYSIS 		' ' FORMAT=COMMA.;
DEFINE TOT_MBRS / ANALYSIS  'Total Members' FORMAT=COMMA.;

DEFINE MBRS_PERC / PCTSUM    	' ' FORMAT=PERCENT9.2;                 
DEFINE TOT_MBRS_PERC / PCTSUM 	'Total % Members' FORMAT=PERCENT9.2;

/* Make sure Y/N values repeat. Proc Rpt removes repeating vals in GROUP */
COMPUTE DM_ONLY; 
	IF DM_ONLY NE ' ' THEN hold_DM_ONLY=DM_ONLY; 
	IF DM_ONLY EQ ' ' THEN DM_ONLY=hold_DM_ONLY; 
	IF _break_ = 'UTIL' THEN DM_ONLY=' '; 
ENDCOMP;
COMPUTE DM_W_WL; 
	IF DM_W_WL NE ' ' THEN hold_DM_W_WL=DM_W_WL; 
	IF DM_W_WL EQ ' ' THEN DM_W_WL=hold_DM_W_WL; 
	IF _break_ = 'UTIL' THEN DM_W_WL=' '; 
ENDCOMP;
COMPUTE WL_ONLY; 
	IF WL_ONLY NE ' ' THEN hold_WL_ONLY=WL_ONLY; 
	IF WL_ONLY EQ ' ' THEN WL_ONLY=hold_WL_ONLY; 
	IF _break_ = 'UTIL' THEN WL_ONLY=' '; 
ENDCOMP;

/* Subtotal by UTIL and assign labels */
BREAK AFTER UTIL / SUMMARIZE;
COMPUTE UTIL;
	IF _break_ = 'UTIL' AND UTIL = 'Monotherapy' THEN UTIL='Monotherapy Subtotal'; 
	IF _break_ = 'UTIL' AND UTIL = 'Dual Therapy' THEN UTIL='Dual Therapy Subtotal'; 
	IF _break_ = 'UTIL' AND UTIL = 'Triple Therapy' THEN UTIL='Triple Therapy Subtotal'; 
ENDCOMP;

RBREAK AFTER/ SUMMARIZE DOL DUL;
COMPUTE AFTER; UTIL='Grand Total'; ENDCOMP;

RUN;

ODS EXCEL OPTIONS (sheet_interval='Proc' Sheet_Name = "Concurrent Therapy" embedded_titles='Yes' 
	Autofilter = 'Yes' absolute_column_width='20,25,30,13,13,13,13,20,15,20,15');

TITLE1 "&NAME."; 
TITLE2 "Fill Dates: &START_DT. - &END_DT.";

FOOTNOTE "Report Run Date: &RUN_DATE.";

OPTIONS MISSING='0';

PROC REPORT DATA=CLM_SUM_RPTOUT style=excel;

TITLE3 "Potential Concurrent GLP1 Utilizers";

COLUMNS LOB GLP_OVERLAP THER
	MBRS=TOT_MBRS
	MBRS=TOT_MBRS_PERC
	RXS=TOT_RXS
	RXS=TOT_RXS_PERC
	ALLOWED=TOT_ALLOWED
	ALLOWED=TOT_ALLOWED_PERC
	PAID=TOT_PAID
	PAID=TOT_PAID_PERC
;

/* Use style(column)={tagattr='wraptext:no'} to format rows/columns/cells*/
DEFINE LOB / 		 'LOB' GROUP ORDER=DATA style(column)={tagattr='wraptext:no' width=100%};
DEFINE GLP_OVERLAP / 'Confirmed GLP Overlap' GROUP style(column)={tagattr='wraptext:no' width=100%};
DEFINE THER / 		 'GLP1 Therapy' GROUP style(column)={tagattr='wraptext:no' width=100%};

DEFINE TOT_MBRS / ANALYSIS  	'Total Members' FORMAT=COMMA.;
DEFINE TOT_MBRS_PERC / PCTSUM 	'% Members' 	FORMAT=PERCENT9.2;
DEFINE TOT_RXS / ANALYSIS  		'Total RXS' 	FORMAT=COMMA.;
DEFINE TOT_RXS_PERC / PCTSUM 	'% RXS' 		FORMAT=PERCENT9.2;
DEFINE TOT_ALLOWED / ANALYSIS  	'Total Allowed' FORMAT=DOLLAR15.2;
DEFINE TOT_ALLOWED_PERC / PCTSUM 	'% Allowed' FORMAT=PERCENT9.2;
DEFINE TOT_PAID / ANALYSIS  	'Total Paid' 	FORMAT=DOLLAR15.2;
DEFINE TOT_PAID_PERC / PCTSUM 	'% Paid' 		FORMAT=PERCENT9.2;

/* Subtotal by GLP_OVERLAP and assign labels and remove repeating LOB value at Subtotal */
BREAK AFTER GLP_OVERLAP / SUMMARIZE;
COMPUTE GLP_OVERLAP;
	IF _break_ = 'GLP_OVERLAP' AND GLP_OVERLAP = 'Not Concurrent GLP1s'
		THEN DO; 
			GLP_OVERLAP='Not Concurrent GLP1s Subtotal'; LOB=' '; 
		END;
	IF _break_ = 'GLP_OVERLAP' AND GLP_OVERLAP = 'Concurrent GLP1s'
		THEN DO;
			GLP_OVERLAP='Concurrent GLP1s Subtotal'; LOB=' '; 
		END;
ENDCOMP;

/* Subtotal by LOB and assign labels */
BREAK AFTER LOB / SUMMARIZE;
COMPUTE LOB;
	IF _break_ = 'LOB' AND LOB='Commercial' THEN LOB='Commercial Subtotal'; 
	IF _break_ = 'LOB' AND LOB='Medicare' THEN LOB='Medicare Subtotal'; 
ENDCOMP;

RBREAK AFTER/ SUMMARIZE DOL DUL;
COMPUTE AFTER; LOB='Grand Total'; ENDCOMP;

RUN;

/* Clear Title and Footnote */
TITLE;
FOOTNOTE;

ODS EXCEL OPTIONS ( Sheet_Name = "Member Summary" embedded_titles='Yes' embedded_footnotes='no'  
	Autofilter = "Yes" absolute_column_width='12,15,15,15,15,12,20,25');

%output(MBR_SUM); *Call Output Macro;

ODS EXCEL OPTIONS ( Sheet_Name = "Claims - Concurrent Utilization" embedded_titles='Yes' embedded_footnotes='no'  
	Autofilter = "Yes" absolute_column_width='6,12,20,20,15,20,20,8,20,12,12,8,15,15,15');

%output(CLM_RPTOUT); *Call Output Macro;

ODS EXCEL CLOSE;
ODS _ALL_ CLOSE;


FILENAME mymail email
attach=("&DIR/&Request. &NAME. &DT_RPT..xlsx" lrecl=32767 content_type="application/xlsx");

DATA _NULL_;
file mymail;
PUT "!EM_TO! 'Krista.Yokoyama@blueshieldca.com' ";
PUT "!EM_CC! 'John.Le@blueshieldca.com' 'Michael.Nathe@blueshieldca.com'";	
PUT "!EM_SUBJECT! &NAME. &DT_RPT."; 
put " ";
put "See the attached file(s)."; 
put " ";
put "Pharmacy Analytics";
put " ";
put "This message (including any attachments) contains business proprietary/confidential information intended for a specific individual and purpose, and is protected by law.";
put "If you are not the intended recipient, you should delete this message. Any disclosure, copying, or distribution of this message, or the taking of any action based on it, without the express permission of the originator, is strictly prohibited.";
RUN;
