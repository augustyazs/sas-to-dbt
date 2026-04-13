libname RXMAC "/sasdata/pharmacy/projects/RxReferences/macros";

options MPRINT mstored sasmstore=RXMAC symbolgen COMPRESS=YES LRECL=5000;

%RXLIB;

%LET DIR =/sasdata3/pharmacy/projects/RxReferences/production/data/phi;

*libname RXNETD sqlsvr dsn="RXNET_NP" schema="dbo" user="RXNetwork" pass="MYGqzKZlG7OMWGfMuGPj" readbuff=3000;
libname RXNETP sqlsvr dsn="RXNET_PA"  schema="dbo" authdomain="BSC_DOM_AUTH" READBUFF=3000 DBMAX_TEXT=32000;

DATA _NULL_;
CALL SYMPUT ('DTE',PUT(INTNX('MONTH',TODAY(),-0),YYMMDDN8.));
RUN;

x "rm &DIR./ncpdp/*";
x "unzip -uj &DIR./ncpdp/zip/NCPDP_v3.1_Monthly_Master_&DTE..ZIP -d &DIR./ncpdp/";

proc format;
picture phone
.=' '
low - high = '(999) 999-9999'
(PREFIX='(')
;

proc format;
value $hr
'01'='1:00 AM' 
'02'='2:00 AM' 
'03'='3:00 AM' 
'04'='4:00 AM' 
'05'='5:00 AM' 
'06'='6:00 AM' 
'07'='7:00 AM' 
'08'='8:00 AM' 
'09'='9:00 AM' 
'10'='10:00 AM' 
'11'='11:00 AM' 
'12'='12:00 PM' 
'13'='1:00 PM' 
'14'='2:00 PM' 
'15'='3:00 PM' 
'16'='4:00 PM' 
'17'='5:00 PM' 
'18'='6:00 PM' 
'19'='7:00 PM' 
'20'='8:00 PM' 
'21'='9:00 PM' 
'22'='10:00 PM' 
'23'='11:00 PM' 
'24'='12:00 AM' 
'25'='12:30 AM' 
'26'='1:30 AM' 
'27'='2:30 AM' 
'28'='3:30 AM' 
'29'='4:30 AM' 
'30'='5:30 AM' 
'31'='6:30 AM' 
'32'='7:30 AM' 
'33'='8:30 AM' 
'34'='9:30 AM' 
'35'='10:30 AM' 
'36'='11:30 AM' 
'37'='12:30 PM' 
'38'='1:30 PM' 
'39'='2:30 PM' 
'40'='3:30 PM' 
'41'='4:30 PM' 
'42'='5:30 PM' 
'43'='6:30 PM' 
'44'='7:30 PM' 
'45'='8:30 PM' 
'46'='9:30 PM' 
'47'='10:30 PM' 
'48'='11:30 PM' 
;

DATA RX_REF.NCPDP_MASTER;
	INFILE "&DIR./ncpdp/mas.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC;
LENGTH
NCPDP $ 7
NPI	$ 10 
Business_Name	$ 60 
Pharmacy_Name	$ 60 
Physician_Name	$ 60 
Store_Number	$ 10 
Address_1	$ 55 
Address_2	$ 55 
City	$ 30 
State	$ 2 
Zip_Code	$ 5 
Zip_Code_4	$ 4 
Pharmacy_Phone_Raw	8
Pharmacy_Phone $ 15
Pharmacy_Extension	$ 5 
Pharmacy_Fax_Raw	8
Pharmacy_Fax $ 15
Pharmacy_Email	$ 50 
Cross_Street_or_Directions	$ 50 
County_Parish	$ 5 
MSA	$ 4 
PMSA	$ 4 
Open_24_Hour_Operation_Flag	$ 1 
Provider_Hours	$ 35 
Congressional_Voting_District	$ 4 
Language_Code_1	$ 2 
Language_Code_2	$ 2 
Language_Code_3	$ 2 
Language_Code_4	$ 2 
Language_Code_5	$ 2 
Store_Open_Date	8
Store_Closure_Date	8
Mailing_Address_1	$ 55 
Mailing_Address_2	$ 55 
Mailing_Address_City	$ 30 
Mailing_Address_State	$ 2 
Mailing_Address_ZIP	$ 5 
Mailing_Address_ZIP_4	$ 4 
Contact_Last_Name	$ 20 
Contact_First_Name	$ 20 
Contact_Middle_Initial	$ 1 
Contact_Title	$ 30 
Contact_Phone_Raw	8
Contact_Phone $ 15
Contact_Extension	$ 5 
Contact_Email	$ 50 
Dispenser_Class_Code	$ 2 
Primary_Provider_Type_Code	$ 2 
Secondary_Provider_Type_Code	$ 2 
Tertiary_Provider_Type_Code	$ 2 
Medicare_Provider_Supplier_ID	$ 10 
DEA_Registration_ID	$ 12 
DEA_Expiration_Date	8
Federal_Tax_ID_Number	$ 15 
State_Income_Tax_ID_Number	$ 15 
Deactivation_Code	$ 2 
Reinstatement_Code	$ 2 
Reinstatement_Date	8
;


    INPUT @1   NCPDP $7. @;
    IF NCPDP ='9999999' then delete;

 
	INPUT
		@858 NPI $10. 
		@8 Business_Name $60. 
		@68 Pharmacy_Name $60. 
		@128 Physician_Name $60. 
		@188 Store_Number $10. 
		@198 Address_1 $55. 
		@253 Address_2 $55. 
		@308 City $30. 
		@338 State $2. 
		@340 Zip_Code $5. 
		@345 Zip_Code_4 $4. 
		@349 Pharmacy_Phone_Raw 10. 
		@359 Pharmacy_Extension $5. 
		@364 Pharmacy_Fax_Raw 10. 
		@374 Pharmacy_Email $50. 
		@424 Cross_Street_or_Directions $50. 
		@474 County_Parish $5. 
		@479 MSA $4. 
		@483 PMSA $4. 
		@487 Open_24_Hour_Operation_Flag $1. 
		@488 Provider_Hours $35. 
		@523 Congressional_Voting_District $4. 
		@527 Language_Code_1 $2. 
		@529 Language_Code_2 $2. 
		@531 Language_Code_3 $2. 
		@533 Language_Code_4 $2. 
		@535 Language_Code_5 $2. 
		@537 Store_Open_Date anydtdte8.
		@545 Store_Closure_Date anydtdte8.
		@553 Mailing_Address_1 $55. 
		@608 Mailing_Address_2 $55. 
		@663 Mailing_Address_City $30. 
		@693 Mailing_Address_State $2. 
		@695 Mailing_Address_ZIP $5. 
		@700 Mailing_Address_ZIP_4 $4. 
		@704 Contact_Last_Name $20. 
		@724 Contact_First_Name $20. 
		@744 Contact_Middle_Initial $1. 
		@745 Contact_Title $30. 
		@775 Contact_Phone_Raw 10. 
		@785 Contact_Extension $5. 
		@790 Contact_Email $50. 
		@840 Dispenser_Class_Code $2. 
		@842 Primary_Provider_Type_Code $2. 
		@844 Secondary_Provider_Type_Code $2. 
		@846 Tertiary_Provider_Type_Code $2. 
		@848 Medicare_Provider_Supplier_ID $10. 
		@868 DEA_Registration_ID $12. 
		@880 DEA_Expiration_Date anydtdte8. 
		@888 Federal_Tax_ID_Number $15. 
		@903 State_Income_Tax_ID_Number $15. 
		@918 Deactivation_Code $2. 
		@920 Reinstatement_Code $2. 
		@922 Reinstatement_Date anydtdte8. 

;
format Store_Open_Date Store_Closure_Date DEA_Expiration_Date Reinstatement_Date MMDDYY10. HOURS_OF_OPERATION $1000.;

Pharmacy_Phone = PUT(Pharmacy_Phone_Raw,phone.);
Pharmacy_Fax = PUT(Pharmacy_Fax_Raw,phone.);
Contact_Phone= PUT(Contact_Phone_Raw,phone.);

DROP Pharmacy_Phone_Raw Pharmacy_Fax_Raw Contact_Phone_Raw;

FORMAT
Sunday_Open_Hours
Sunday_Close_Hours
Monday_Open_Hours
Monday_Close_Hours
Tuesday_Open_Hours
Tuesday_Close_Hours
Wednesday_Open_Hours
Wednesday_Close_Hours
Thursday_Open_Hours
Thursday_Close_Hours
Friday_Open_Hours
Friday_Close_Hours
Saturday_Open_Hours
Saturday_Close_Hours $100.;

IF Open_24_Hour_Operation_Flag='Y' THEN HOURS_OF_OPERATION="24 hrs/7 days a week";

Sunday_Open_Hours=put(substr(Provider_Hours,2,2),hr.);
Sunday_Close_Hours=put(substr(Provider_Hours,4,2),hr.);
Monday_Open_Hours=put(substr(Provider_Hours,7,2),hr.);
Monday_Close_Hours=put(substr(Provider_Hours,9,2),hr.);
Tuesday_Open_Hours=put(substr(Provider_Hours,12,2),hr.);
Tuesday_Close_Hours=put(substr(Provider_Hours,14,2),hr.);
Wednesday_Open_Hours=put(substr(Provider_Hours,17,2),hr.);
Wednesday_Close_Hours=put(substr(Provider_Hours,19,2),hr.);
Thursday_Open_Hours=put(substr(Provider_Hours,22,2),hr.);
Thursday_Close_Hours=put(substr(Provider_Hours,24,2),hr.);
Friday_Open_Hours=put(substr(Provider_Hours,27,2),hr.);
Friday_Close_Hours=put(substr(Provider_Hours,29,2),hr.);
Saturday_Open_Hours=put(substr(Provider_Hours,32,2),hr.);
Saturday_Close_Hours=put(substr(Provider_Hours,34,2),hr.);
FORMAT LOAD_DATE MMDDYY10.;
LOAD_DATE=INPUT(SYMGET('DTE'),YYMMDD8.);
RUN;

PROC APPEND BASE=RX_DATA.NCPDP_MASTER_ALL(NULLCHAR=NO) DATA=RX_REF.NCPDP_MASTER; RUN;

PROC DATASETS LIB=RX_REF;
   MODIFY NCPDP_MASTER;
      INDEX DELETE _ALL_;
	  INDEX CREATE NCPDP;
	  INDEX CREATE NPI;
  RUN;

DATA RX_REF.NCPDP_CHANGE_OWNERSHIP;
	INFILE "&DIR./ncpdp/mas_coo.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
    IF NCPDP ='9999999' then delete;
 
	INPUT
		@8   NCPDP_Old $7. 
		@15  Old_Store_Close_Date anydtdte8.
		@23  Ownership_Change_Eff_Date anydtdte8.
;
FORMAT Old_Store_Close_Date Ownership_Change_Eff_Date MMDDYY10.;
		RUN;

DATA RX_REF.NCPDP_MEDICAID;
	INFILE "&DIR./ncpdp/mas_md.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
    IF NCPDP ='9999999' then delete;
 
	INPUT
		@8   STATE_CODE $2. 
		@10  MEDICAID_ID $20. 
		@30  Delete_Date anydtdte8.
;
FORMAT Delete_Date MMDDYY10.;
RUN;

DATA RX_REF.NCPDP_STATE_LICENSE;
	INFILE "&dir./ncpdp/mas_stl.txt" TRUNCOVER LRECL=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
    IF NCPDP ='9999999' THEN DELETE;
 
	INPUT
@8 LICENSE_STATE_CODE $2.
@10 STATE_LICENSE_NUMBER $20.
@30 STATE_LICENSE_EXPIRATION_DATE ANYDTDTE8.
@38 DELETE_DATE ANYDTDTE8.
;
FORMAT STATE_LICENSE_EXPIRATION_DATE DELETE_DATE MMDDYY10.;
RUN;

DATA RX_REF.NCPDP_SERVICES;
	INFILE "&DIR./ncpdp/mas_svc.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
		IF NCPDP ='9999999' then delete;
 
	INPUT
		@8 eRX_Ind $1. 
		@9 eRX_Code $2. 
		@11 Delivery_Service_Ind $1. 
		@12 Delivery_Service_Code $2. 
		@14 Compound_Service_Ind $1. 
		@15 Compound_Service_Code $2. 
		@17 Drive_Up_Window_Ind $1. 
		@18 Drive_Up_Window_Code $2. 
		@20 Durable_Medical_Equipment_Ind $1. 
		@21 Durable_Medical_Equipment_Code $2. 
		@23 Walk_In_Clinic_Ind $1. 
		@24 Walk_In_Clinic_Code $2. 
		@26 Emergency_Service_24_Hours_Ind $1. 
		@27 Emergency_Service_24_Hours_Code $2. 
		@29 Multi_Dose_Compl_Pack_Ind $1. 
		@30 Multi_Dose_Compl_Pack_Code $2. 
		@32 Immunization_Provided_Ind $1. 
		@33 Immunization_Provided_Code $2. 
		@35 Handicap_Accessible_Ind $1. 
		@36 Handicap_Accessible_Code $2. 
		@38 Status_340B_Ind $1. 
		@39 Status_340B_Code $2. 
		@41 Closed_Door_Fac_Ind $1. 
		@42 Closed_Door_Fac_Status_Code $2. 

;
		RUN;

DATA RX_REF.NCPDP_ERX;
	INFILE "&DIR./ncpdp/mas_erx.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
		IF NCPDP ='9999999' then delete;

INPUT
@8 ERX_Network_Id $3. 
@11 ERX_Service_Level_Codes $100. 
@111 Eff_From_Date anydtdte8. 
@119 Eff_Through_Date anydtdte8.
;

FORMAT Eff_From_Date Eff_Through_Date MMDDYY10.;
RUN;

DATA RX_REF.NCPDP_PROVIDER_RELATIONSHIP;
	INFILE "&DIR./ncpdp/mas_rr.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
		IF NCPDP ='9999999' then delete;

INPUT
@8 Relationship_ID $3. 
@11 Payment_Center_ID $6. 
@17 Remit_and_Reconciliation_ID $6. 
@23 Provider_Type $2. 
@25 Is_Primary $1. 
@26 Eff_From_Date anydtdte8.
@34 Eff_Through_Date anydtdte8. 

;

FORMAT Eff_From_Date Eff_Through_Date MMDDYY10.;
RUN;


DATA RX_REF.NCPDP_RELATIONSHIP_DEMO;
	INFILE "&DIR./ncpdp/mas_af.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
		IF NCPDP ='9999999' then delete;

INPUT

@1 Relationship_ID $3. 
@4 Relationship_Type $2. 
@6 Name $35. 
@41 Address_1 $55. 
@96 Address_2 $55. 
@151 City $30. 
@181 State_Code $2. 
@183 Zip_Code $9. 
@192 Phone_Number $15. 
@202 Extension $5. 
@207 FAX_Number $15. 
@217 Relationship_NPI $10. 
@227 Relationship_Federal_Tax_ID $15. 
@242 Contact_Name $30. 
@272 Contact_Title $30. 
@302 Email_Address $50. 
@352 Contractual_Contact_Name $30. 
@382 Contractual_Contact_Title $30. 
@412 Contractual_Contact_EMail $50. 
@462 Operational_Contact_Name $30. 
@492 Operational_Contact_Title $30. 
@522 Operational_Contact_EMail $50. 
@572 Technical_Contact_Name $30. 
@602 Technical_Contact_Title $30. 
@632 Technical_Contact_EMail $50. 
@682 Audit_Contact_Name $30. 
@712 Audit_Contact_Title $30. 
@742 Audit_Contact_EMail $50. 
@792 Parent_Organization_ID $6. 
@798 Eff_From_Date anydtdte8.  
@806 Delete_Date anydtdte8.  
;

Phone_Number = PUT(INPUT(Phone_Number,10.),phone.);
FAX_Number = PUT(INPUT(FAX_Number,10.),phone.);
DROP NCPDP;

FORMAT Eff_From_Date Delete_Date MMDDYY10.;
RUN;

DATA RX_REF.NCPDP_FWA;
	INFILE "&DIR./ncpdp/mas_fwa.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
    IF NCPDP ='9999999' then delete;
 
	INPUT
		@8  Medicaid_Part_D $1. 
		@9  FWA_Attestation $1. 
		@10 Version_Number $5.
		@15 Plan_Year $4.
		@19 Q1 $1.
		@20 Q2 $1.
		@89 Q3 $1.
		@90 Q4 $1.
		@21 Accreditation_Date anydtdte8.
		@29 Accreditation_Org $60.
		@91 Signature_of_Resp_Party $60.
		@151 Signature_Date anydtdte8.
		@159 Responsible_Party $60.
		@219 Participating_Pharmacy_PSAO_Name $60.
		@279 Address1 $55.
		@334 Address2 $55.
		@389 City $30.
		@419 State $2.
		@421 Zip_Code $9.
		@430 NPI $10.
		@440 Fax $10.
		@450 Email $50.
		
;
FORMAT Accreditation_Date Signature_Date MMDDYY10.;
RUN;

DATA RX_REF.NCPDP_PARENT_ORG;
	INFILE "&DIR./ncpdp/mas_pr.txt" truncover lrecl=25000 FIRSTOBS=2 END=LASTREC
;
    INPUT @1   NCPDP $7. @;
    IF NCPDP ='9999999' then delete;

INPUT
@1 Parent_Organization_ID $6. 
@7 Parent_Organization_Name $35. 
@42 Address_1 $55. 
@97 Address_2 $55. 
@152 City $30. 
@182 State $2. 
@184 Zip $9. 
@193 Phone_Number $15. 
@203 Extension $5. 
@208 FAX_Number $15. 
@218 NPI $10. 
@228 Federal_Tax_ID $15. 
@243 Contact_Name $30. 
@273 Contact_Title $30. 
@303 Email $50. 
@353 Delete_Date anydtdte8. 
;

Phone_Number = PUT(INPUT(Phone_Number,10.),phone.);
FAX_Number = PUT(INPUT(FAX_Number,10.),phone.);
DROP NCPDP;
RUN;


PROC SQL;
CREATE TABLE PHARMACY_PARENT_ORG AS
SELECT M.NCPDP
,M.NPI
,M.Pharmacy_Name
,PR.Relationship_ID
,RD.Parent_Organization_ID
,PO.*


FROM RX_REF.NCPDP_MASTER M
INNER JOIN RX_REF.NCPDP_PROVIDER_RELATIONSHIP PR
ON M.NCPDP = PR.NCPDP

INNER JOIN RX_REF.NCPDP_RELATIONSHIP_DEMO RD
ON PR.Relationship_ID = RD.Relationship_ID

INNER JOIN RX_REF.NCPDP_PARENT_ORG PO
ON RD.Parent_Organization_ID = PO.Parent_Organization_ID
;
QUIT;

LIBNAME RX_DATA NETEZZA server="npsexp01" database="ADH_SBX_RX_DATA" Authdomain="AD_Dom_Auth";

PROC SQL;
CONNECT TO NETEZZA (server="npsexp01" database="ADH_SBX_RX_DATA" Authdomain="AD_Dom_Auth");
execute(TRUNCATE NCPDP_MASTER) by NETEZZA;
QUIT;

PROC SQL;
INSERT INTO RX_DATA.NCPDP_MASTER(NULLCHAR=NO)
SELECT * FROM RX_REF.NCPDP_MASTER;
QUIT;

proc sql;
connect to sqlsvr (dsn="RXNET_PA" authdomain="BSC_Dom_Auth");
execute(DELETE FROM NCPDP) by sqlsvr;
QUIT;

PROC APPEND BASE=RXNETP.NCPDP(NULLCHAR=NO) DATA=RX_REF.NCPDP_MASTER;
RUN;

proc sql;
connect to sqlsvr (dsn="RXNET_PA" authdomain="BSC_Dom_Auth");
execute(DELETE FROM NCPDP_STATE_LICENSE) by sqlsvr;
QUIT;

PROC APPEND BASE=RXNETP.NCPDP_STATE_LICENSE(NULLCHAR=NO) DATA=RX_REF.NCPDP_STATE_LICENSE;
RUN;

FileName MyMail email; run;

*******************************************************;
*     SEND e-mail to 'matt.dieckman@blueshieldca.com' *;
*******************************************************;
DATA _null_ ;
  file mymail;
  PUT "!EM_TO! 'rxnetwork@blueshieldca.com' ";
  PUT "!EM_CC! 'frances.lee@blueshieldca.com' 'john.le@blueshieldca.com'";
  PUT "!EM_SUBJECT! NCPDP Monthly Update"; 
  put "The NCPDP Vendor file has been downloaded and updated.";
  put " ";
  put "Thanks";
  put " ";
  PUT "This message (including any attachments) contains business proprietary/confidential information intended for a specific individual and purpose, and is protected by law.";
  PUT "If you are not the intended recipient, you should delete this message. Any disclosure, copying, or distribution of this message, or the taking of any action based on it, without the express permission of the originator, is strictly prohibited.";
run;
