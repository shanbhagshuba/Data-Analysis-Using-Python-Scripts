import pyhdb
import pandas as pd
import os
import numpy as np
from datetime import datetime
from calendar import monthrange
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import time

hap_conn = pyhdb.connect(host = "vhdtahapdb01.us164.corpintra.net",
    port = 30215,
    user = "KBHOSAR",
    password = "Jhampyajhabo9891@")

had_conn = pyhdb.connect(host = "vhdtahaddb01.us164.corpintra.net",
    port = 30615,
    user = "KBHOSAR",
    password = "HanaDaimler773!")

vmrs_desc_cost_file = r'''PM_Service_VMRS_Final.xlsx'''
vmrs_desc_cost_filepath = os.path.join( os.getcwd(), vmrs_desc_cost_file )
vmrs_desc_cost = pd.read_excel(vmrs_desc_cost_filepath)
vmrs_desc_cost.rename(columns={'Vmrs 33': 'VMRS_33'}, inplace=True)
vmrs_list = list(vmrs_desc_cost['VMRS_33'].unique())
vmrs_list =  ', '.join("'{0}'".format(w) for w in vmrs_list) # To get a single string with all VMRS comma separated
vmrs_list = ','.join('{}'.format(word) for word in vmrs_list.split(','))

claims_sql ="""
SELECT DISTINCT "VIN","CLAIM_CD", "CLAIM_PAID_DT", "IN_SERVICE_DT", "VMRS_33", SUM("TOTAL_CLAIM_PAID_AMT") AS "TOTAL_CLAIM_PAID_AMT_SUM", SUM("LABOR_AMT_ADJ") AS "LABOR_AMT_ADJ_SUM", SUM("ITEM_AMT_PAID") AS "ITEM_AMT_PAID_SUM"
FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CLAIMS_DEALER_CHASSIS"('PLACEHOLDER' = ('$$parPaid_YearFrom$$', '2018'), 'PLACEHOLDER' = ('$$parPaid_YearTo$$', '2022')) 
WHERE ("CLAIM_TYPE_CD" = 'MAINTENANCE_CONTRACT' AND "VMRS_33" IN ("""+ vmrs_list +"""))
GROUP BY "VIN", "CLAIM_CD", "CLAIM_PAID_DT", "IN_SERVICE_DT", "VMRS_33"
"""

#cvrg_sql = """
#SELECT DISTINCT "VIN", "IN_SERVICE_DT"
#FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CVRG_EXT_VIN" 
#WHERE ("COVERAGE_PACKAGE_TYPE" = 'PM')
#"""

claims_data = pd.read_sql(claims_sql, hap_conn) 
pm_claims = claims_data.copy()

vins_list = list(pm_claims['VIN'].unique())
vins_list =  ', '.join("'{0}'".format(w) for w in vins_list)
vins_list = ','.join('{}'.format(word) for word in vins_list.split(','))

prod_sql = """
SELECT DISTINCT "PRODUCT_VIN" as "VIN", "VEH_TSO_SPLIT_NO" as "TSO_SPLIT_NO", "VEH_MAKE_CD", "ENG_BASE_MDL_CD","VEH_ENG_OP_RPT_MDL", "VEH_BASE_MDL_NO", "IN_SERVICE_DT", "VOCATIONAL_CD", "VOCATIONAL_DESCRIPTION", "CURR_CUST_NAME"
FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_PRODUCT_VIN" 
WHERE ("PRODUCT_VIN" IN ("""+ vins_list +"""))
"""

prods = pd.read_sql(prod_sql, hap_conn)

tso_list = list(prods['TSO_SPLIT_NO'].unique())
tso_list =  ', '.join("'{0}'".format(w) for w in tso_list)
tso_list = ','.join('{}'.format(word) for word in tso_list.split(','))

db_codes_sql = """
SELECT DISTINCT "TSO_SPLIT_NO", "VEH_420_DB_CD", "VEH_420_DB_DESC", "VEH_342_DB_CD", "VEH_342_DB_DESC","VEH_014_DB_CD", "VEH_480_DB_CD","VEH_480_DB_DESC"
FROM "_SYS_BIC"."DTNA_PRD.ALL.Views/CV_DBCODES_SPLIT"
WHERE ("TSO_SPLIT_NO" IN ("""+ tso_list +""")) 
"""
db_codes = pd.read_sql(db_codes_sql, had_conn)
pm_data = pm_claims.merge(prods,on="VIN", how = 'left')                               
pm_data = pm_data.merge(db_codes, on="TSO_SPLIT_NO", how = 'left')

 
def create_item_model(differential, engine, service, air_filter, air_dryer, transmission, cummins_engine): #add air filter and air dryer to list of parameters     
     if service=='SPM-080-000' or service =='SPM-092-000':
         if air_filter== '014-1B5' or air_filter=='014-1CP' or air_filter=='014-1CR':
             return "High Capacity"
         elif air_filter== '014-107' or air_filter=='014-108' or air_filter =='014-109':
             return "1-stage safety"
         else:
             return "1-stage"
     elif service=='SPM-076-000':
         if "WABCO" in air_dryer:
             return "Wabco"
         elif "BW" in air_dryer:
             return "Bendix"
         else:
             return "No Air Dryer"
     elif service=='SPM-033-000':
         if "TANDEM" in differential:
             return "Tandem"
         elif "SINGLE" in differential:
             return "Single"
         else:
             return "NA"
     elif service=='SPM-B00-000' or service=='SPM-038-000' or service=='SPM-081-000' or service=='SPM-081-001'or service=='SPM-081-002' or service=='SPM-081-003' or service=='SPM-081-004':
         if engine is None == True  or engine == 'None':             
             return (cummins_engine)
         else:             
             return str(engine)
     elif service=='STA-B00-000':
         if "DD5" in engine:
             return "DD5"
         elif "DD15" in engine:
             return "DD15"
         return str(engine)
     elif service == 'SPM-069-000':
         return 'DT12'
     elif service == 'SPM-088-000':
         return 'EATON'
     elif service == 'SPM-083-000':
         return str(engine)
     elif service =='SPM-055-000' or service =="SPM-056-000":
         if "ALLISON 3000" in transmission:
             return "ALLISON 3000 FULL"
         elif "ALLISON 4500" in transmission:
             return "ALLISON 4000 FULL"
         else:
             return "Wrong Allison Model"
     elif service == 'SPM-057-000':
         if "ALLISON 2500"  in transmission:
             return "ALLISON 2000 FILTER"
         elif "ALLISON 2100" in transmission:
             return "ALLISON 2000 FILTER"
         else:
             return "ALLISON 1000 FILTER"
         
     elif service == "SPM-058-000" and transmission=="ALLISON 3000":
             return "ALLISON 3000 FILTER"
    
     #elif service == "SPM-058-000":
      #   if "ALLISON 3000" in transmission:
       #      return "ALLISON 3000 FILTER"
         
     elif transmission == "ALLISON 4500":
             return "ALLISON 4000 FILTER"          
     else:
         return 'ALL'
         
pm_data['ENG_BASE_MDL_CD'] = pm_data['ENG_BASE_MDL_CD'].replace(['DD15GHG17AT','D15E10','DD15GHG14TC', 'D13E10','DD13GHG17', 'DD13GHG14', 'DD8GHG17', 'DD16GHG17'],['DD15','DD15','DD15','DD13', 'DD13', 'DD13', 'DD8', 'DD16'])
pm_data['VEH_480_DB_DESC'] = pm_data['VEH_480_DB_DESC'].fillna('No Air Dryer')
pm_data['ITEM_MODEL'] = pm_data.apply(lambda x: create_item_model(x['VEH_420_DB_DESC'], str(x['ENG_BASE_MDL_CD']), x['VMRS_33'], x['VEH_014_DB_CD'], x['VEH_480_DB_DESC'], x['VEH_342_DB_DESC'], x['VEH_ENG_OP_RPT_MDL']),axis=1)

pm_data= pd.merge(pm_data, vmrs_desc_cost, how = 'left',left_on=['VMRS_33','ITEM_MODEL'], right_on=['VMRS_33','MODEL'])  #LEFT JOIN WITH VMRS_COST TABLE
pm_data.drop('MODEL', 1, inplace= True)
pm_data = pm_data.loc[:, ~pm_data.columns.str.contains('^Unnamed')]

pm_data  = pm_data.drop_duplicates()
pm_data['ITEM_MODEL'] = pm_data['ITEM_MODEL'].astype(str)
pm_data['SERVICE TYPE'] = pm_data['SERVICE TYPE'].astype(str)
pm_data.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/Preventive Maint/PM_Costs1.csv') 


