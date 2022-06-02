# -*- coding: utf-8 -*-
"""
Created on Tue Jul 27 10:05:56 2021

@author: SHANBHS
"""

import pandas as pd
import numpy as np
from datetime import datetime
from calendar import monthrange
from dateutil.relativedelta import relativedelta
import time
from sap_hana_connect import establish_connection
from dateutil import relativedelta
from Engine_Dashboard_Automated_Functions import get_final_files, rename_columns,write_to_excel, get_secondary_data

had = establish_connection('had')
hap = establish_connection('hap')

claim_start_date = """'2021-04-01'"""
claim_end_date = """'2022-04-30'"""
start_date = '04-01-2021' # start date of time frame mm-dd-yyyy
end_date = '04-30-2022'


st_date= datetime.strptime(start_date, '%m-%d-%Y').date() # changing the format from string to date
ed_date= datetime.strptime(end_date, '%m-%d-%Y').date()

sql_string = """
SELECT DISTINCT "VIN", "CURR_CUST_NAME", "REV_REC_BEGN_DT", "REV_REC_END_DT", "IN_SERVICE_DT", "ENG_BASE_MDL_CD","COVERAGE_PACKAGE","EMISSION_SUB_GRP","COVERAGE_PACKAGE_NAME"
FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CVRG_EXT_DDC" 
WHERE ("EXT_STATUS" = 'REGISTERED'  AND "PROD_TYPE_CD" = 'ENGINE' AND "PRODUCT_SUB_TYPE_CD" = 'ENGINE' AND "COVERAGE_PACKAGE" NOT IN ('EN', 'PM') AND "PRICEBOOK_CD" IN ('DDC', 'DDCSB') 
AND "COVERAGE_OPTION_CD" NOT IN ('EXT DETROIT TRANSMISSION 72/750000/WAK-1F1', 
'EXT DETROIT TRANSMISSION 84/850000/WAK-019', 
'EXT DETROIT TRANSMISSION 84/750000/WAK-023', 
'EXT DDC TRANSMISSION CUSTOM COMPONENT 48/450000/WBD-002', 
'EXT DDC TRANSMISSION CUSTOM COMPONENT 48/450000/WBD-023', 
'EXT DDC TRANSMISSION CUSTOM COMPONENT 60/500000/WBD-024', 
'EXT DETROIT CLUTCH COVERAGE ST 36/400000/WBD-022', 
'EXT DETROIT CLUTCH COVERAGE ST 48/450000/WBD-023', 
'EXT DETROIT CLUTCH COVERAGE ST 60/500000/WBD-024', 
'EXT DETROIT CLUTCH 48/450000/WBD-041', 
'EXT DETROIT CLUTCH COVERAGE ST 60/500000/WBD-044'))"""

start_time = time.time()
all_engine_contracts_data = pd.read_sql(sql_string, hap)
end_time = (time.time()-start_time)/60
print("Contracts loaded in ", end_time )

sql_string_claims = """
SELECT DISTINCT "VIN","CLAIM_CD", "ENG_BASE_MDL_CD", "IN_SERVICE_DT", "CLAIM_PAID_DT", "CURR_CUST_NAME","DEALER_NAME", "DEALER_TYPE_CODE", "EMISSION_SUB_GRP","FAILED_PART_SEAG_CODE","FAILED_PART_SEAG_DESC","VEH_BUILD_DT", "VIN_ODOMETER", "FAILURE_DT", SUM("TOTAL_WTY_COST") AS "TOTAL_WTY_COST_SUM"
FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CLAIMS_DEALER_DDC"('PLACEHOLDER' = ('$$parPaid_YearFrom$$', '2016'), 'PLACEHOLDER' = ('$$parPaid_YearTo$$', '2022')) 
WHERE ("CLAIM_TYPE_CD" = 'EXTENDED' AND "PRODUCT_TYPE_CD" = 'ENGINE'AND  "CLAIM_STATUS_CD"in ('PAID_IN_FULL','MODIFIED') AND "EXTENDED_REPLACEMENT_INDC" ='N' 
AND "CLAIM_PAID_DT" >= """+ claim_start_date +""" AND "CLAIM_PAID_DT" <= """+ claim_end_date +""")
GROUP BY "VIN","CLAIM_CD", "ENG_BASE_MDL_CD", "IN_SERVICE_DT", "CLAIM_PAID_DT", "CURR_CUST_NAME","DEALER_NAME", "DEALER_TYPE_CODE", "EMISSION_SUB_GRP","FAILED_PART_SEAG_CODE","FAILED_PART_SEAG_DESC", "VEH_BUILD_DT", "VIN_ODOMETER", "FAILURE_DT"
"""
cl_start_time = time.time()
all_engine_claims = pd.read_sql(sql_string_claims, hap)
cl_end_time = (time.time()-cl_start_time)/60
print("Claims loaded in  ", cl_end_time )

to_change = '01-01-1900'  # next few lines are getting rid of contracts that apparantely were bought before the company was founded. ¯\_(ツ)_/¯
myd  = datetime.strptime(to_change, '%m-%d-%Y').date()
all_engine_contracts_data['REV_REC_BEGN_DT'] = all_engine_contracts_data['REV_REC_BEGN_DT'].replace(myd, None)
all_engine_contracts_data['REV_REC_END_DT'] = all_engine_contracts_data['REV_REC_END_DT'].replace(myd, None)
all_engine_contracts_data = all_engine_contracts_data[all_engine_contracts_data['IN_SERVICE_DT'].notna()]  # get rid of contracts without In Service date (Coz nobody got time for dat!)
all_engine_contracts_data['ISY'] = pd.DatetimeIndex(all_engine_contracts_data['IN_SERVICE_DT']).year
data_for_active_units = all_engine_contracts_data.copy() #so that I dont blow up my contracts data and have to pull it again from HANA
data_for_active_units= data_for_active_units.drop_duplicates() 
all_engine_claims['ISY'] = pd.DatetimeIndex(all_engine_claims['IN_SERVICE_DT']).year
all_engine_claims = all_engine_claims.loc[all_engine_claims['CLAIM_PAID_DT'].isna() == False]
all_engine_claims["DEALER_TYPE"] = np.where((all_engine_claims['DEALER_TYPE_CODE'] == "DWC") | (all_engine_claims['DEALER_TYPE_CODE'] == "DSDWC"), "DWC", "Dealer")
all_engine_claims.drop('DEALER_TYPE_CODE', inplace = True, axis = 1)
all_engine_claims = all_engine_claims.drop_duplicates() 
eng_claims = all_engine_claims.copy()


ghg17_eng_claims = all_engine_claims.copy()
ghg17_eng_claims = ghg17_eng_claims.loc[ghg17_eng_claims['EMISSION_SUB_GRP'] =='GHG17']
ghg17_eng_claims = ghg17_eng_claims.loc[ghg17_eng_claims['IN_SERVICE_DT'].isnull() == False]
ghg17_eng_claims = ghg17_eng_claims.loc[ghg17_eng_claims['FAILURE_DT'].isnull() == False]
ghg17_contracts = data_for_active_units.copy()
ghg17_contracts = ghg17_contracts.loc[ghg17_contracts['EMISSION_SUB_GRP'] =='GHG17']

hpfp_contracts = ghg17_contracts.copy()
inj_contracts = ghg17_contracts.copy()
nox_contracts = ghg17_contracts.copy()
belt_tensioner_contracts = ghg17_contracts.copy()
air_compressor_contracts = ghg17_contracts.copy()


inj_claims = ghg17_eng_claims.copy()
inj_claims = inj_claims.loc[inj_claims['FAILED_PART_SEAG_CODE'] == '07091']
nox_claims = ghg17_eng_claims.copy()
nox_claims = nox_claims.loc[(nox_claims['FAILED_PART_SEAG_CODE'] == '14501') | (nox_claims['FAILED_PART_SEAG_CODE'] == '14502')]
belt_tensioner_claims = ghg17_eng_claims.copy()
belt_tensioner_claims = belt_tensioner_claims.loc[belt_tensioner_claims['FAILED_PART_SEAG_CODE'] == '20266']
air_compressor_claims = ghg17_eng_claims.copy()
air_compressor_claims = air_compressor_claims.loc[air_compressor_claims['FAILED_PART_SEAG_CODE'] == '13001']
hpfp_claims = ghg17_eng_claims.copy()
hpfp_claims  = hpfp_claims.loc[hpfp_claims['FAILED_PART_SEAG_CODE']=='07206']

# =============================================================================
# All engine dashboard
# =============================================================================
all_eng_total_wty,all_eng_cpc_cpu, all_eng_top_customers,all_eng_top_dealers, all_eng_seag_info = get_final_files(st_date, ed_date, data_for_active_units, eng_claims, "All_engine")

all_eng_top_dealers.reset_index(inplace = True)
all_eng_total_wty = rename_columns('total_wty', all_eng_total_wty)
all_eng_cpc_cpu = rename_columns('cpc_cpu', all_eng_cpc_cpu)
all_eng_top_customers = rename_columns('top_customers', all_eng_top_customers)
all_eng_top_dealers = rename_columns('top_dealers', all_eng_top_dealers)


all_eng_cpc_cpu['CPC'] = all_eng_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
all_eng_cpc_cpu['CPU'] = all_eng_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)

all_eng  = pd.concat([all_eng_total_wty, all_eng_cpc_cpu, all_eng_top_customers,all_eng_top_dealers,all_eng_seag_info], axis =1)
all_eng.to_csv(r'\\ftmsv01\ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\All_engine.csv', index = False)
 
# =============================================================================
# GHG17 Dashboard
# =============================================================================
ghg17_eng_total_wty,ghg17_eng_cpc_cpu, ghg17_eng_top_customers,ghg17_eng_top_dealers, ghg17_eng_seag_info = get_final_files(st_date, ed_date,ghg17_contracts, ghg17_eng_claims,  "GHG17_engine")
ghg17_eng_total_wty = rename_columns('total_wty', ghg17_eng_total_wty)
ghg17_eng_cpc_cpu = rename_columns('cpc_cpu', ghg17_eng_cpc_cpu)
ghg17_eng_top_customers = rename_columns('top_customers', ghg17_eng_top_customers)
ghg17_eng_top_dealers = rename_columns('top_dealers', ghg17_eng_top_dealers)
ghg17_eng_cpc_cpu['CPC'] = ghg17_eng_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ghg17_eng_cpc_cpu['CPU'] = ghg17_eng_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)

ghg17_eng = pd.concat([ghg17_eng_total_wty, ghg17_eng_cpc_cpu, ghg17_eng_top_customers,ghg17_eng_top_dealers,ghg17_eng_seag_info], axis = 1)
ghg17_eng.to_csv(r'\\ftmsv01\ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\GHG17_engine.csv', index = False)

# =============================================================================
# HPFP
# =============================================================================
ghg17_hpfp_total_wty,ghg17_hpfp_cpc_cpu, ghg17_hpfp_top_customers,ghg17_hpfp_top_dealers, ghg17_hpfp_seag_info = get_final_files(st_date, ed_date, hpfp_contracts, hpfp_claims, "hpfp")

ghg17_hpfp_total_wty = rename_columns('total_wty', ghg17_hpfp_total_wty)
ghg17_hpfp_cpc_cpu = rename_columns('cpc_cpu', ghg17_hpfp_cpc_cpu)
ghg17_hpfp_top_customers = rename_columns('top_customers', ghg17_hpfp_top_customers)
ghg17_hpfp_top_dealers = rename_columns('top_dealers', ghg17_hpfp_top_dealers).reset_index()
ghg17_hpfp_cpc_cpu['CPC'] = ghg17_hpfp_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ghg17_hpfp_cpc_cpu['CPU'] = ghg17_hpfp_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
ghg17_hpfp_paid_amt_bld_yr, ghg17_hpfp_total_claims_fail_miles_isy ,ghg17_hpfp_paid_amt_mis = get_secondary_data(hpfp_claims)


ghg17_hpfp = pd.concat([ghg17_hpfp_total_wty, ghg17_hpfp_cpc_cpu, ghg17_hpfp_top_customers,ghg17_hpfp_top_dealers,ghg17_hpfp_seag_info, ghg17_hpfp_paid_amt_bld_yr, ghg17_hpfp_total_claims_fail_miles_isy ,ghg17_hpfp_paid_amt_mis], axis =1)
ghg17_hpfp.to_csv(r'\\ftmsv01\ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\GHG17_HPFP.csv', index = False)
#=============================================================================
# Injector
# =============================================================================
ghg17_inj_total_wty,ghg17_inj_cpc_cpu, ghg17_inj_top_customers,ghg17_inj_top_dealers, ghg17_inj_seag_info = get_final_files(st_date, ed_date, inj_contracts, inj_claims, "inj")

ghg17_inj_total_wty = rename_columns('total_wty', ghg17_inj_total_wty)
ghg17_inj_cpc_cpu = rename_columns('cpc_cpu', ghg17_inj_cpc_cpu)
ghg17_inj_top_customers = rename_columns('top_customers', ghg17_inj_top_customers)
ghg17_inj_top_dealers = rename_columns('top_dealers', ghg17_inj_top_dealers).reset_index()
ghg17_inj_cpc_cpu['CPC'] = ghg17_inj_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ghg17_inj_cpc_cpu['CPU'] = ghg17_inj_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
ghg17_inj_paid_amt_bld_yr, ghg17_inj_total_claims_fail_miles_isy ,ghg17_inj_paid_amt_mis = get_secondary_data(inj_claims)
ghg17_inj = pd.concat([ghg17_inj_total_wty, ghg17_inj_cpc_cpu, ghg17_inj_top_customers,ghg17_inj_top_dealers,ghg17_inj_seag_info,ghg17_inj_paid_amt_bld_yr, ghg17_inj_total_claims_fail_miles_isy ,ghg17_inj_paid_amt_mis ], axis =1)
ghg17_inj.to_csv(r'\\ftmsv01\ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\GHG17_INJ.csv', index = False)

# =============================================================================
# NOX sensors
# =============================================================================
ghg17_nox_total_wty,ghg17_nox_cpc_cpu, ghg17_nox_top_customers,ghg17_nox_top_dealers, ghg17_nox_seag_info = get_final_files(st_date, ed_date, nox_contracts, nox_claims, "nox")

ghg17_nox_total_wty = rename_columns('total_wty', ghg17_nox_total_wty)
ghg17_nox_cpc_cpu = rename_columns('cpc_cpu', ghg17_nox_cpc_cpu)
ghg17_nox_top_customers = rename_columns('top_customers', ghg17_nox_top_customers)
ghg17_nox_top_dealers = rename_columns('top_dealers', ghg17_nox_top_dealers).reset_index()
ghg17_nox_cpc_cpu['CPC'] = ghg17_nox_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ghg17_nox_cpc_cpu['CPU'] = ghg17_nox_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
ghg17_nox_paid_amt_bld_yr, ghg17_nox_total_claims_fail_miles_isy ,ghg17_nox_paid_amt_mis = get_secondary_data(nox_claims)
ghg17_nox = pd.concat([ghg17_nox_total_wty,ghg17_nox_cpc_cpu, ghg17_nox_top_customers,ghg17_nox_top_dealers, ghg17_nox_seag_info,ghg17_nox_paid_amt_bld_yr, ghg17_nox_total_claims_fail_miles_isy ,ghg17_nox_paid_amt_mis], axis =1)
ghg17_nox.to_csv(r'\\ftmsv01/ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\GHG17_NOX.csv', index = False)

# =============================================================================
# Belt Tensioner
# =============================================================================
ghg17_belt_total_wty,ghg17_belt_cpc_cpu, ghg17_belt_top_customers,ghg17_belt_top_dealers, ghg17_belt_seag_info = get_final_files(st_date, ed_date, belt_tensioner_contracts, belt_tensioner_claims, "belt_tensioner")

ghg17_belt_total_wty = rename_columns('total_wty', ghg17_belt_total_wty)
ghg17_belt_cpc_cpu = rename_columns('cpc_cpu', ghg17_belt_cpc_cpu)
ghg17_belt_top_customers = rename_columns('top_customers', ghg17_belt_top_customers)
ghg17_belt_top_dealers = rename_columns('top_dealers', ghg17_belt_top_dealers).reset_index()
ghg17_belt_cpc_cpu['CPC'] = ghg17_belt_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ghg17_belt_cpc_cpu['CPU'] = ghg17_belt_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
ghg17_belt_paid_amt_bld_yr, ghg17_belt_total_claims_fail_miles_isy ,ghg17_belt_paid_amt_mis = get_secondary_data(belt_tensioner_claims)
ghg17_belt = pd.concat([ghg17_belt_total_wty,ghg17_belt_cpc_cpu, ghg17_belt_top_customers,ghg17_belt_top_dealers, ghg17_belt_seag_info,ghg17_belt_paid_amt_bld_yr, ghg17_belt_total_claims_fail_miles_isy ,ghg17_belt_paid_amt_mis], axis =1)
ghg17_belt.to_csv(r'\\ftmsv01\ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\ghg17_belt_tensioner.csv', index = False)

# =============================================================================
# Air Compressor 
# =============================================================================
ghg17_air_comp_total_wty,ghg17_air_comp_cpc_cpu, ghg17_air_comp_top_customers,ghg17_air_comp_top_dealers, ghg17_air_comp_seag_info = get_final_files(st_date, ed_date, air_compressor_contracts, air_compressor_claims, "air_compressor")

ghg17_air_comp_total_wty = rename_columns('total_wty', ghg17_air_comp_total_wty)
ghg17_air_comp_cpc_cpu = rename_columns('cpc_cpu', ghg17_air_comp_cpc_cpu)
ghg17_air_comp_top_customers = rename_columns('top_customers', ghg17_air_comp_top_customers)
ghg17_air_comp_top_dealers = rename_columns('top_dealers', ghg17_air_comp_top_dealers).reset_index()
ghg17_air_comp_cpc_cpu['CPC'] = ghg17_air_comp_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ghg17_air_comp_cpc_cpu['CPU'] = ghg17_air_comp_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
ghg17_air_comp_paid_amt_bld_yr, ghg17_air_comp_total_claims_fail_miles_isy ,ghg17_air_comp_paid_amt_mis = get_secondary_data(hpfp_claims)
ghg17_air_comp = pd.concat([ghg17_belt_total_wty,ghg17_air_comp_cpc_cpu, ghg17_air_comp_top_customers,ghg17_air_comp_top_dealers, ghg17_air_comp_seag_info,ghg17_air_comp_paid_amt_bld_yr, ghg17_air_comp_total_claims_fail_miles_isy ,ghg17_air_comp_paid_amt_mis], axis =1)
ghg17_air_comp.to_csv(r'\\ftmsv01\ew_services\2000_Cost Management\2500_General Information\2502_Processes & Documentation\Tableau Documentation\All_dashboards_combined\ghg17_air_comp.csv', index = False)



