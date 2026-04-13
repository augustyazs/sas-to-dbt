********************************************************************************************;
***	
RITM3113116/DAA74958 GLP-1 Monthly PA
	- Monthly refresh
	- BMI and comorbidity data required

Dev Comments
	9/12/23: Rsh BMI data in BOR. Can't find any joins to Mbr/Auth info. 
	9/15/23: Mtg w EJarata, incl Erin Mullens for specs. BMI in case notes col (COMMENTS). 
	- Eff 1/1/23:
	- Comorbid is BMI between 27 and 30. Once >= 30 approved
	- Current BMI of > 27 kg/m2 and patient has one of the following conditions: 
		hypertension, diabetes, coronary artery disease, dyslipidemia, stroke, 
		osteoarthritis, metabolic syndrome, prediabetes, PCOS, NASH, 
		or patient has sleep apnea currently being treated with 
		CPAP (HTN, CAD, CVD, DM, OA, OSA, MI)
	- See \\bsc\it\EAI_ADF\Pharmacy\Analyst\John\Projs_Issues\GLP1_DM_WL
		\DAA74958_RITM3113116_GLP1_Monthly_PA\Rsh_BMI_AuthAccel.sas 
		for BMI extraction research
	10/2/23: Per email from Erin Mullens, Overweight for the data should be a BMI of 27 to 29.9

********************************************************************************************;

%SYSMSTORECLEAR;
LIBNAME RXMAC "/sasdata/pharmacy/projects/RxReferences/macros";
OPTIONS SYMBOLGEN NOQUOTELENMAX MSTORED MLOGIC MPRINT SASMSTORE=RXMAC SASTRACE=',,,d' SASTRACELOC=SASLOG NOSTSUFFIX COMPRESS=yes;
%RXLIB;
ODS RESULTS OFF;

%LET Request=DAA74958;
%LET dir=/sasdata3/pharmacy/projects/rxana_rpts/production/data/output/phi/GLP1_Rpts;
LIBNAME A "&dir.";
%LET NAME=Monthly PA;


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

/* Pull FI COA */
DATA COA_FI; SET RX_DATA.MAP_COA; WHERE FUNDING = 'FI'; RUN;
PROC SQL NOPRINT;
SELECT DISTINCT STRIP(CATS("'",COA,"'")) INTO :COA_FI SEPARATED BY "," 
FROM COA_FI;
QUIT;
%PUT &COA_FI.;

%LET CNT_MN=-1;	* Months Prior;
%LET AUTH_STAT='APPROVED','DENIED';	

DATA _NULL_;
	DT_RPT = TODAY();
	DB_START=INTNX('MONTH',TODAY(),&CNT_MN.,'B'); * Begin of prior Month identified by CNT_MN;
	DB_END=INTNX('MONTH',DB_START,0, 'E');	* End of prior Month;
	CALL SYMPUT('START_SB',CATS("'",PUT(DB_START,DATE9.),"'d"));
	CALL SYMPUT('END_SB',CATS("'",PUT(DB_END,DATE9.),"'d"));
	CALL SYMPUT('START_BIS',CATS("'",PUT(DB_START,YYMMDDD10.),"'"));
	CALL SYMPUT('END_BIS',CATS("'",PUT(DB_END,YYMMDDD10.),"'"));
	CALL SYMPUT('TBL',PUT(DB_START,YYMMN6.));
	CALL SYMPUT('DT_RPT',PUT(DT_RPT,YYMMDDN8.));
	CALL SYMPUT('RUN_DATE',PUT(DT_RPT,MMDDYYD10.));
	CALL SYMPUT('YR',PUT(TODAY(),YEAR.));
	CALL SYMPUT('START_DT',CATS(PUT(DB_START,MMDDYYS10.)));
	CALL SYMPUT('END_DT',CATS(PUT(DB_END,MMDDYYS10.)));
RUN;
%PUT &RUN_DATE. &START_SB. &END_SB. &START_BIS. &END_BIS. &TBL.;


DATA REPORT_PARMS;
     LENGTH SPEC  $500.;
	 LABEL SPEC = "Specifications";OUTPUT;
	 SPEC = " Report Specifications";OUTPUT;
	 SPEC = " Request #: &Request.";OUTPUT;
	 SPEC = " Report Title: &NAME.";OUTPUT;
	 SPEC = " Objective/Purpose: Obtain data on GLP1s to improve pharmacy trend forecasting";OUTPUT;
	 SPEC = " Prior Request: Krista Yokoyama";OUTPUT;
	 SPEC = " Analyst Name: John Le";OUTPUT;
	 SPEC = " Requestor Name: Krista Yokoyama";OUTPUT;
	 SPEC = " Report Run Date: &RUN_DATE.";OUTPUT;
	 SPEC = " Report Start Date: &START_DT.";OUTPUT;
	 SPEC = " Report End Date: &END_DT.";OUTPUT;
	 SPEC = " LOB(s): BSC Commercial and Medicare";OUTPUT;
	 SPEC = " Request Criteria: ";OUTPUT;
	 SPEC = " Inclusion Criteria: ";OUTPUT;
	 SPEC = " Exclusion Criteria: ";OUTPUT;
	 SPEC = " Requestor Comments: ";OUTPUT;
	 SPEC = " Analyst Comments: ";OUTPUT;
	 SPEC = " - BMI value is in case notes column (COMMENTS) ";OUTPUT;
	 SPEC = " - Only ‘Approved’ or ‘Denied’ Auths ";OUTPUT;
	 SPEC = " - Only ‘GLP-1: DM w WL’ or ‘GLP-1: WL only’ drugs (Excludes ‘GLP-1: DM Only’ drugs)";OUTPUT;
	 SPEC = " -	Report can be run on or after the 10th of each month due to data availability in data source ";OUTPUT;
	 SPEC = " - Overweight for the data should be a BMI of 27 to 29.9";OUTPUT;
	 SPEC = " - Comorbid logic:";OUTPUT;
	 SPEC = "   1. Find the following words; 'HYPERTENSION','DYSLIPIDEMIA','PCOS','PREDIABETES','DIABETES', ";OUTPUT;
	 SPEC = "      'STROKE','NASH','OSTEOARTHRITIS','CAD','CVD','HTN','DM','OA','OSA','MI'";OUTPUT;
	 SPEC = "   2. Find the following phrases; ‘CORONARY ARTERY DISEASE’, ";OUTPUT;
	 SPEC = "      ‘SLEEP APNEA CURRENTLY BEING TREATED WITH CPAP’, ‘METABOLIC SYNDROME’";OUTPUT;
RUN;


%MACRO AUTHACCEL(TBL);

PROC SQL;
CREATE TABLE ACCEL_&TBL. AS 
SELECT DISTINCT 
CASE WHEN B.NDC IS MISSING THEN 'Non-GLP1: DM' ELSE 'Drug Type 1'n END AS drug_type1
,CASE WHEN B.NDC IS MISSING THEN 'Non-GLP1: DM' ELSE 'Drug Type2'n END AS drug_type2
,AA.COMMENTS
,AA.LOB
,AA.LOB2
,AA.RECEIVED_DATE
,AA.EFFECTUATION__DATE
,AA.REQUEST_TYPE
,AA.INTAKE_TYPE
,AA.MEMBER_ID 
,AA.PERSON_NUMBER
,AA.MEMBER_FIRST_NAME
,AA.MEMBER_LAST_NAME
,AA.MEMBER_DATE_OF_BIRTH
,AA.AUTHORIZATION_NUMBER 
,AA.NDC_NUMBER 
,AA.LABEL_NAME
,AA.CASE_NUMBER 
,UPCASE(AA.CASE_STATUS)			AS CASE_STATUS
,AA.STATUS_REASON_DESCRIPTION 
,AA.REQUEST_REASON_DESCRIPTION
,AA.MODC_CODE
,AA.COVERAGE_CODE_1
,AA.COVERAGE_CODE_2
,AA.ATTRIBUTE_6
,AA.ATTRIBUTE_7
,AA.CVS_CARRIER
,AA.CVS_ACCOUNT
,AA.CVS_GRP
,AA.CUSTOMER_NUMBER
,AA.CLIENT_NUMBER
,AA.GROUP_CODE
,AA.FORMULARY
,AA.SOURCE_FILE

FROM RX_DATA.AUTHACCEL AA

LEFT JOIN A.R12_DIAB_DRUGS B on AA.NDC_NUMBER=B.NDC

WHERE DATEPART(RECEIVED_DATE) BETWEEN &START_SB. AND &END_SB.
	AND UPCASE(AA.CASE_STATUS) IN (&AUTH_STAT.)
	AND AA.NDC_NUMBER IN (&NDC_DIAB.,&NDC_GPI4.) 

;QUIT;

/* 296 and 665 have dups */
PROC SQL;
CREATE TABLE DUPCK_ACCEL_&TBL. AS
SELECT * FROM ACCEL_&TBL.
WHERE CASE_NUMBER IN 
(SELECT CASE_NUMBER FROM ACCEL_&TBL. GROUP BY 1 HAVING COUNT(*)>1)
ORDER BY CASE_NUMBER, RECEIVED_DATE;
QUIT;

%MEND AUTHACCEL;

%AUTHACCEL(&TBL.);

%CHK_COL(REQ_RSN,REQUEST_REASON_DESCRIPTION,ACCEL_&TBL.);
%CHK_COL(STAT_RSN,STATUS_REASON_DESCRIPTION,ACCEL_&TBL.);


PROC SQL;
CREATE TABLE BMI_TMP AS 
SELECT * FROM ACCEL_&TBL.
WHERE (INDEX(COMMENTS,"BMI") OR INDEX(UPCASE(COMMENTS),"BMI ") OR INDEX(UPCASE(COMMENTS)," BMI")) 
	AND drug_type1 IN ('GLP-1: DM w WL','GLP-1: WL only');
QUIT;

%CHK_COL(drug_type1,drug_type1,BMI_TMP);
%CHK_COL(drug_type2,drug_type2,BMI_TMP);

DATA BMI_TMP;
SET BMI_TMP;

TXT_LEN=LENGTH(COMMENTS);

BMI_POS_1=INDEX(COMMENTS,"BMI");	*Find Caps BMI 1st word match;
TXT_1=SUBSTR(COMMENTS,BMI_POS_1,15);
BMI_NUM_01=INPUT(SUBSTR(COMPRESS(TXT_1,COMPRESS(TXT_1,,"d")),1,2),best.);

COM_TXT_2=SUBSTR(COMMENTS,BMI_POS_1 + 3);	*Find Caps BMI 2nd word Match;
BMI_POS_2=INDEX(COM_TXT_2,"BMI");
TXT_2=SUBSTR(COM_TXT_2,BMI_POS_2,15);
BMI_NUM_02=INPUT(SUBSTR(COMPRESS(TXT_2,COMPRESS(TXT_2,,"d")),1,2),best.);

BMI_POS_3=INDEX(UPCASE(COMMENTS),"BMI ");	*Find lower case bmi 1st word match;
TXT_3=SUBSTR(COMMENTS,BMI_POS_3,15);
BMI_NUM_03=INPUT(SUBSTR(COMPRESS(TXT_3,COMPRESS(TXT_3,,"d")),1,2),best.);

BMI_POS_4=INDEX(UPCASE(COMMENTS)," BMI");	*Find lower case bmi 2nd word match;
TXT_4=SUBSTR(COMMENTS,BMI_POS_4,15);
BMI_NUM_04=INPUT(SUBSTR(COMPRESS(TXT_4,COMPRESS(TXT_4,,"d")),1,2),best.);

TXT_5=SUBSTR(COMMENTS,BMI_POS_1-7,15);		*Find bmi num before Caps BMI;
BMI_NUM_05=INPUT(SUBSTR(COMPRESS(TXT_5,COMPRESS(TXT_5,,"d")),1,2),best.);

TXT_6=SUBSTR(COMMENTS,BMI_POS_3-6,15);		*Find bmi num before lower case bmi;
BMI_NUM_06=INPUT(SUBSTR(COMPRESS(TXT_6,COMPRESS(TXT_6,,"d")),1,2),best.);

BMI_FNL=COALESCE(BMI_NUM_01,BMI_NUM_02,BMI_NUM_03,BMI_NUM_04,BMI_NUM_05,BMI_NUM_06);

LENGTH WORD $20 CO_MORBID $500;
%LET SEARCH_WORDS = 'HYPERTENSION','DYSLIPIDEMIA','PCOS','PREDIABETES','DIABETES',
	'STROKE','NASH','OSTEOARTHRITIS','CAD','CVD','HTN','DM','OA','OSA','MI';

DO CNT=1 BY 1 UNTIL (WORD EQ '');
	WORD = UPCASE(SCAN(TRANSLATE(COMMENTS,' ',':;()[]=>'),CNT));
	IF WORD IN (&SEARCH_WORDS.) THEN CO_MORBID = CATX(",",CO_MORBID,WORD);
END;
WORD_CNT=CNT;

IF FIND(UPCASE(COMMENTS),"CORONARY ARTERY DISEASE") 
	THEN CO_MORBID = CATX(",",CO_MORBID,"CORONARY ARTERY DISEASE");
IF FIND(UPCASE(COMMENTS),"SLEEP APNEA CURRENTLY BEING TREATED WITH CPAP") 
	THEN CO_MORBID = CATX(",",CO_MORBID,"SLEEP APNEA CURRENTLY BEING TREATED WITH CPAP");
IF FIND(UPCASE(COMMENTS),"METABOLIC SYNDROME") 
	THEN CO_MORBID = CATX(",",CO_MORBID,"METABOLIC SYNDROME");

/* Overweight for the data should be a BMI of 27 to 29.9 */
IF BMI_FNL >= 27 AND BMI_FNL < 30 THEN OVERWEIGHT = 'Y';

DROP WORD CNT;

RUN;

PROC SQL;
CREATE TABLE RPTOUT_DTL AS 
SELECT CASE_NUMBER
,CASE_STATUS
,REQUEST_REASON_DESCRIPTION
,MEMBER_ID 
,PERSON_NUMBER
,MEMBER_FIRST_NAME
,MEMBER_LAST_NAME
,MEMBER_DATE_OF_BIRTH		FORMAT=MMDDYYS10.
,AUTHORIZATION_NUMBER 
,NDC_NUMBER 
,LABEL_NAME
,drug_type1		LABEL='DRUG TYPE1'
,BMI_FNL		LABEL='Retrieved BMI'
,CASE WHEN BMI_FNL >= 30 THEN  'Y'
	ELSE ' ' END 	AS OBESE	LABEL='OBESE (BMI >= 30)'
,CASE WHEN OVERWEIGHT = 'Y' AND CO_MORBID <> ' ' THEN 'Y'
	ELSE ' ' END 	AS OVERWT_COMORBID		LABEL='Overweight with comorbid'
,OVERWEIGHT		LABEL='Overweight Flag'
,CO_MORBID		LABEL='Comorbidity'
,COMMENTS

FROM BMI_TMP;

CREATE TABLE RPTOUT_SUM AS 
SELECT 
drug_type1	LABEL='DRUG TYPE1'

,CASE WHEN BMI_FNL >= 30 THEN  'Obese (BMI >= 30)'
	WHEN OVERWEIGHT = 'Y' AND CO_MORBID <> ' ' THEN 'Overweight with comorbid'
	ELSE ' ' END 	AS CAT	LABEL='Category'

,CASE WHEN REQUEST_REASON_DESCRIPTION = 'Reauthorization' THEN 'REAUTH'
	ELSE 'NEW START' END 	AS NEW_REAUTH	

,SUM(CASE WHEN CASE_STATUS = 'APPROVED' THEN 1 ELSE 0 END)	AS APPRVD_CNT
,COUNT(DISTINCT CASE_NUMBER)	AS CNT

FROM BMI_TMP
GROUP BY 1,2,3	
HAVING CALCULATED CAT <> ' '
ORDER BY 1,2,3

;QUIT;

/* Add Approval % */
DATA RPTOUT_SUM;
SET RPTOUT_SUM;
APPRVD_PERC=APPRVD_CNT/CNT;
RUN;


options nocenter;
ODS EXCEL FILE="&DIR/&Request. &NAME. &TBL. Auths &DT_RPT..xlsx" STYLE=htmlblue;

ods excel OPTIONS (sheet_interval="none" Sheet_Name = "Report Specs" 
ABSOLUTE_COLUMN_WIDTH='80' embedded_titles='no' embedded_footnotes='no');

%output(REPORT_PARMS); *Call Output Macro;

%NEW_SHEET

ODS EXCEL OPTIONS (sheet_interval='Proc' Sheet_Name = "PA Summary" embedded_titles='Yes' 
	Autofilter = 'Yes' absolute_column_width='17,22,16,16,16,16,16,16');

TITLE1 "&NAME."; 
TITLE2 "Fill Dates: &START_DT. - &END_DT.";

FOOTNOTE "Report Run Date: &RUN_DATE.";

/*OPTIONS MISSING='0';*/

PROC REPORT DATA=RPTOUT_SUM NOWD MISSING style=excel;

COLUMNS drug_type1 CAT
	CNT, ('-# Request-' NEW_REAUTH)
	APPRVD_CNT, ('-# Approved-' NEW_REAUTH)
	APPRVD_PERC, ('-% Approved-' NEW_REAUTH)
;

DEFINE drug_type1 / 'DRUG TYPE1' GROUP ORDER=DATA;
DEFINE CAT / 'Category' GROUP;

DEFINE NEW_REAUTH / ACROSS ' '; 

DEFINE CNT / ANALYSIS 		' ' FORMAT=COMMA.;
DEFINE APPRVD_CNT / ANALYSIS 		' ' FORMAT=COMMA.;

DEFINE APPRVD_PERC / ANALYSIS    	' ' FORMAT=PERCENT9.2;                 

/* Subtotal by UTIL and assign labels */
/*BREAK AFTER drug_type1 / SUMMARIZE;*/
/*COMPUTE UTIL;*/
/*	IF _break_ = 'UTIL' AND UTIL = 'Monotherapy' THEN UTIL='Monotherapy Subtotal'; */
/*	IF _break_ = 'UTIL' AND UTIL = 'Dual Therapy' THEN UTIL='Dual Therapy Subtotal'; */
/*	IF _break_ = 'UTIL' AND UTIL = 'Triple Therapy' THEN UTIL='Triple Therapy Subtotal'; */
/*ENDCOMP;*/

/*RBREAK AFTER/ SUMMARIZE DOL DUL;*/
/*COMPUTE AFTER; UTIL='Grand Total'; ENDCOMP;*/

RUN;

/* Clear Title and Footnote */
TITLE;
FOOTNOTE;

ODS EXCEL OPTIONS ( Sheet_Name = "Auth Detail" embedded_titles='Yes' embedded_footnotes='no'  
	Autofilter = "Yes" absolute_column_width='17,16,34,14,20,24,23,27,27,16,33,14,15,19,25,17,20,100');

%output(RPTOUT_DTL); *Call Output Macro;

ODS EXCEL CLOSE;
ODS _ALL_ CLOSE;


FILENAME mymail email
attach=("&DIR/&Request. &NAME. &TBL. Auths &DT_RPT..xlsx" lrecl=32767 content_type="application/xlsx");

DATA _NULL_;
file mymail;
PUT "!EM_TO! 'Emmanuel.Jarata@blueshieldca.com' ";
PUT "!EM_CC! 'John.Le@blueshieldca.com' 'Krista.Yokoyama@blueshieldca.com'";	
PUT "!EM_SUBJECT! &NAME. &TBL. Auths &DT_RPT."; 
put " ";
put "See the attached file(s)."; 
put " ";
put "Pharmacy Analytics";
put " ";
put "This message (including any attachments) contains business proprietary/confidential information intended for a specific individual and purpose, and is protected by law.";
put "If you are not the intended recipient, you should delete this message. Any disclosure, copying, or distribution of this message, or the taking of any action based on it, without the express permission of the originator, is strictly prohibited.";
RUN;
