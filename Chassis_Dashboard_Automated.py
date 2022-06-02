# -*- coding: utf-8 -*-
"""
Created on Mon Oct 25 15:02:30 2021

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
from Chassis_Dashboard_Automated_Functions import get_final_files, rename_columns,write_to_excel, get_secondary_data,veh_model

had = establish_connection('had')
hap = establish_connection('hap')

claim_start_date = """'2021-04-01'"""
claim_end_date = """'2022-04-30'"""
start_date = '04-01-2021' # start date of time frame mm-dd-yyyy
end_date = '04-30-2022'

st_date= datetime.strptime(start_date, '%m-%d-%Y').date() # changing the format from string to date
ed_date= datetime.strptime(end_date, '%m-%d-%Y').date()


chassis_string = """
SELECT DISTINCT "VIN", "REV_REC_BEGN_DT", "REV_REC_END_DT", "IN_SERVICE_DT","COVERAGE_PACKAGE","CURR_CUST_NAME","VEH_BASE_MDL_NO", "WAR_LEVEL_DB_CD"
FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CVRG_EXT_VIN" 
WHERE (  "REV_REC_END_DT" >= """+claim_start_date+""" AND "REV_REC_BEGN_DT" <= """+claim_end_date+""" AND "EXT_STATUS" = 'REGISTERED'
 AND "COVERAGE_PACKAGE" IN ('STANDALONE', 'TC1', 'TC2', 'TC3', 'TC4') AND "COVERAGE_CD" <> '00086 E'
 AND "PROD_TYPE_CD" = 'CHASSIS'
AND "COVERAGE_PACKAGE_NAME" IN ('STANDALONE', 'TC1 - TRUCK CVRG PKG 1', 'TC2 - TRUCK CVRG PKG 2', 'TC3 - TRUCK CVRG PKG 3', 'TC4 - TRUCK CVRG PKG 4')
AND "COVERAGE_CD" NOT IN ('00086 E','00272 E','00226 E','00475 E','00115 E','00141 E','00136 E','00154 E','00273 E','00068 E','00224 E','00326 E','00339 E') AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%FILE DIRECT%') AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%EXT MB%') 
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%EXT KROGER%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%EXT ENGINE%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%EXT CUMMINS%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%EXT TRANSMISSION%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%DVCM%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%TBB%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%CPP%')
AND  UPPER("COVERAGE_NAME") NOT LIKE  UPPER('%EXT ROADWAY%'))
"""
# Move the returned SAP HANA data into a pandas dataframe
start_time = time.time()
contracts_data = pd.read_sql(chassis_string, hap)
end_time = (time.time()-start_time)/60
print("Contracts loaded in ", end_time )

chassis_string_claims = """
SELECT DISTINCT "VIN", "CLAIM_CD", "CLAIM_PAID_DT","IN_SERVICE_DT","CURR_CUST_NAME", "VEH_BASE_MDL_NO", "VMRS_33",
"WAR_LEVEL_DB_CD","DEALER_NAME","REPAIR_DEALER_CD", "DEALER_TYPE_CODE","COMPONENT_DESC","VEH_BUILD_DT","VIN_ODOMETER","FAILURE_DT", SUM("TOTAL_WTY_COST") AS "TOTAL_WTY_COST_SUM"
FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CLAIMS_DEALER_CHASSIS"('PLACEHOLDER' = ('$$parPaid_YearFrom$$', '2019'), 'PLACEHOLDER' = ('$$parPaid_YearTo$$', '2022')) 
WHERE ("CLAIM_TYPE_CD" = 'EXTENDED' AND UPPER("VMRS_33") NOT LIKE  UPPER('%F9%') AND  UPPER("PRIMARY_FAILED_PART_DESC") NOT LIKE  UPPER('%TOWING%')
AND "CLAIM_PAID_DT" >= """+ claim_start_date +""" AND "CLAIM_PAID_DT" <= """+ claim_end_date +""")
GROUP BY "VIN", "CLAIM_CD", "CLAIM_PAID_DT","IN_SERVICE_DT", "CURR_CUST_NAME","VEH_BASE_MDL_NO","VMRS_33", 
"WAR_LEVEL_DB_CD","DEALER_NAME","REPAIR_DEALER_CD","DEALER_TYPE_CODE","COMPONENT_DESC","VEH_BUILD_DT", "VIN_ODOMETER","FAILURE_DT"
"""
cl_start_time = time.time()
claims = pd.read_sql(chassis_string_claims, hap)
cl_end_time = (time.time()-cl_start_time)/60
print("Claims loaded in  ", cl_end_time )

code_start_time = time.time()
to_change = '01-01-1900'  # next few lines are getting rid of contracts that apparantely were bought before the company was founded. ¯\_(ツ)_/¯
myd  = datetime.strptime(to_change, '%m-%d-%Y').date()
contracts_data['REV_REC_BEGN_DT'] = contracts_data['REV_REC_BEGN_DT'].replace(myd, None)
contracts_data['REV_REC_END_DT'] = contracts_data['REV_REC_END_DT'].replace(myd, None)
contracts_data = contracts_data[contracts_data['IN_SERVICE_DT'].notna()]  # get rid of contracts without In Service date (Coz nobody got time for dat!)
contracts_data['ISY'] = pd.DatetimeIndex(contracts_data['IN_SERVICE_DT']).year
data_for_active_units = contracts_data.copy() #so that I dont blow up my contracts data and have to pull it again from HANA
data_for_active_units= data_for_active_units.drop_duplicates() 
claims['ISY'] = pd.DatetimeIndex(claims['IN_SERVICE_DT']).year
chassis_claims = claims.copy()
data_for_active_units["VEH_MODEL"] = pd.np.where(data_for_active_units.WAR_LEVEL_DB_CD.str.contains("995-006"), "MD",
                                     pd.np.where(data_for_active_units.WAR_LEVEL_DB_CD.str.contains("995-091"), "MD",
                                     pd.np.where(data_for_active_units.WAR_LEVEL_DB_CD.str.contains("995-1AT"), "MD",
                                     pd.np.where(data_for_active_units.WAR_LEVEL_DB_CD.str.contains("995-011"), "FCC",
                                     pd.np.where(data_for_active_units.WAR_LEVEL_DB_CD.str.contains("995-037"), "FCC",
                                     pd.np.where(data_for_active_units.VEH_BASE_MDL_NO.str.contains("PT126"), "P4",
                                     pd.np.where(data_for_active_units.VEH_BASE_MDL_NO.str.contains("PE116"), "P4",
                                     pd.np.where(data_for_active_units.VEH_BASE_MDL_NO.str.contains("CA125"), "P3",
                                     pd.np.where(data_for_active_units.VEH_BASE_MDL_NO.str.contains("CA113"), "P3","HD"            
                                                 )))))))))

chassis_claims["VEH_MODEL"] = pd.np.where(chassis_claims.WAR_LEVEL_DB_CD.str.contains("995-006"), "MD",
                                     pd.np.where(chassis_claims.WAR_LEVEL_DB_CD.str.contains("995-091"), "MD",
                                     pd.np.where(chassis_claims.WAR_LEVEL_DB_CD.str.contains("995-1AT"), "MD",
                                     pd.np.where(chassis_claims.WAR_LEVEL_DB_CD.str.contains("995-011"), "FCC",
                                     pd.np.where(chassis_claims.WAR_LEVEL_DB_CD.str.contains("995-037"), "FCC",
                                     pd.np.where(chassis_claims.VEH_BASE_MDL_NO.str.contains("PT126"), "P4",
                                     pd.np.where(chassis_claims.VEH_BASE_MDL_NO.str.contains("PE116"), "P4",
                                     pd.np.where(chassis_claims.VEH_BASE_MDL_NO.str.contains("CA125"), "P3",
                                     pd.np.where(chassis_claims.VEH_BASE_MDL_NO.str.contains("CA113"), "P3","HD"            
                                                 )))))))))

chassis_claims["DEALER_NAME"] = chassis_claims.apply(lambda x: x["REPAIR_DEALER_CD"]+"_"+x["DEALER_NAME"], axis= 1 )
final_df = pd.DataFrame()

p4_contracts = data_for_active_units.copy()
p4_contracts = p4_contracts.loc[(p4_contracts["VEH_BASE_MDL_NO"]=='PT126SLP')|(p4_contracts["VEH_BASE_MDL_NO"]=='PT126DC')|(p4_contracts["VEH_BASE_MDL_NO"]=='PE116SLP')|(p4_contracts["VEH_BASE_MDL_NO"]== 'PE116DC')]
p4_claims = chassis_claims.loc[chassis_claims["VEH_MODEL"]== "P4"]
p4_claims = p4_claims.loc[p4_claims['IN_SERVICE_DT'].isnull() == False]
p4_claims = p4_claims.loc[p4_claims['FAILURE_DT'].isnull() == False]

seal_oil_contracts = p4_contracts.copy()
seal_oil_claims = p4_claims.copy()
seal_oil_claims = p4_claims.loc[p4_claims['VMRS_33']=='018-002-011']

mirror_heated_convex_contracts = p4_contracts.copy()
mirror_heated_convex_claims = p4_claims.copy()
mirror_heated_convex_claims = p4_claims.loc[p4_claims['VMRS_33']=='002-010-037']

core_tank_radiator_contracts = p4_contracts.copy()
core_tank_radiator_claims = p4_claims.copy()
core_tank_radiator_claims = p4_claims.loc[p4_claims['VMRS_33']=='042-002-001']

wiring_harness_contracts = p4_contracts.copy()
wiring_harness_claims = p4_claims.copy()
wiring_harness_claims = p4_claims.loc[p4_claims['VMRS_33']=='034-004-217']

heater_aux_contracts = p4_contracts.copy()
heater_aux_claims = p4_claims.copy()
heater_aux_claims = p4_claims.loc[p4_claims['VMRS_33']=='001-003-034']

ac_compressor_contracts = p4_contracts.copy()
ac_compressor_claims = p4_claims.copy()
ac_compressor_claims = p4_claims.loc[p4_claims['VMRS_33']=='001-001-002']

all_chassis_total_wty,all_chassis_cpc_cpu, all_chassis_top_customers,all_chassis_top_dealers, all_chassis_seag_info = get_final_files(st_date, ed_date, data_for_active_units, chassis_claims, "All_chassis")
  
all_chassis_top_dealers.reset_index(inplace = True)
all_chassis_total_wty = rename_columns('total_wty', all_chassis_total_wty)
all_chassis_cpc_cpu = rename_columns('cpc_cpu', all_chassis_cpc_cpu)
all_chassis_top_customers = rename_columns('top_customers', all_chassis_top_customers)
all_chassis_top_dealers = rename_columns('top_dealers', all_chassis_top_dealers)

all_chassis_cpc_cpu['CPC'] = all_chassis_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
all_chassis_cpc_cpu['CPU'] = all_chassis_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
all_chassis  = pd.concat([all_chassis_total_wty, all_chassis_cpc_cpu, all_chassis_top_customers,all_chassis_top_dealers,all_chassis_seag_info], axis =1)
all_chassis.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/All_chassis.csv', index = False)
 
p4_chassis_total_wty,p4_chassis_cpc_cpu, p4_chassis_top_customers,p4_chassis_top_dealers, p4_chassis_seag_info = get_final_files(st_date, ed_date, p4_contracts, p4_claims, "P4_chassis")
  
p4_chassis_top_dealers.reset_index(inplace = True)
p4_chassis_total_wty = rename_columns('total_wty', p4_chassis_total_wty)
p4_chassis_cpc_cpu = rename_columns('cpc_cpu', p4_chassis_cpc_cpu)
p4_chassis_top_customers = rename_columns('top_customers', p4_chassis_top_customers)
p4_chassis_top_dealers = rename_columns('top_dealers', p4_chassis_top_dealers)

p4_chassis_cpc_cpu['CPC'] = p4_chassis_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
p4_chassis_cpc_cpu['CPU'] = p4_chassis_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
p4_chassis  = pd.concat([p4_chassis_total_wty, p4_chassis_cpc_cpu, p4_chassis_top_customers,p4_chassis_top_dealers,p4_chassis_seag_info], axis =1)
p4_chassis.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/p4_chassis.csv', index = False)


seal_oil_total_wty,seal_oil_cpc_cpu, seal_oil_top_customers,seal_oil_top_dealers, seal_oil_seag_info = get_final_files(st_date, ed_date, seal_oil_contracts, seal_oil_claims, "seal_oil")
seal_oil_top_dealers.reset_index(inplace = True)
seal_oil_total_wty = rename_columns('total_wty', seal_oil_total_wty)
seal_oil_cpc_cpu = rename_columns('cpc_cpu', seal_oil_cpc_cpu)
seal_oil_top_customers = rename_columns('top_customers', seal_oil_top_customers)
seal_oil_top_dealers = rename_columns('top_dealers', seal_oil_top_dealers)

seal_oil_paid_amt_bld_yr, seal_oil_total_claims_fail_miles_isy ,seal_oil_paid_amt_mis = get_secondary_data(seal_oil_claims)
seal_oil_paid_amt_bld_yr = rename_columns('bld_yr', seal_oil_paid_amt_bld_yr)
seal_oil_total_claims_fail_miles_isy = rename_columns('fail_miles', seal_oil_total_claims_fail_miles_isy)
seal_oil_paid_amt_mis = rename_columns('mis', seal_oil_paid_amt_mis)

seal_oil_cpc_cpu['CPC'] = seal_oil_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
seal_oil_cpc_cpu['CPU'] = seal_oil_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
seal_oil  = pd.concat([seal_oil_total_wty, seal_oil_cpc_cpu, seal_oil_top_customers,seal_oil_top_dealers,seal_oil_seag_info, seal_oil_paid_amt_bld_yr,seal_oil_total_claims_fail_miles_isy,seal_oil_paid_amt_mis], axis =1)
seal_oil.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/seal_oil.csv', index = False)


mirror_heated_convex_total_wty,mirror_heated_convex_cpc_cpu, mirror_heated_convex_top_customers,mirror_heated_convex_top_dealers, mirror_heated_convex_seag_info = get_final_files(st_date, ed_date, mirror_heated_convex_contracts, mirror_heated_convex_claims, "mirror_heated_convex")
mirror_heated_convex_top_dealers.reset_index(inplace = True)
mirror_heated_convex_total_wty = rename_columns('total_wty', mirror_heated_convex_total_wty)
mirror_heated_convex_cpc_cpu = rename_columns('cpc_cpu', mirror_heated_convex_cpc_cpu)
mirror_heated_convex_top_customers = rename_columns('top_customers', mirror_heated_convex_top_customers)
mirror_heated_convex_top_dealers = rename_columns('top_dealers', mirror_heated_convex_top_dealers)

mirror_heated_convex_paid_amt_bld_yr, mirror_heated_convex_total_claims_fail_miles_isy ,mirror_heated_convex_paid_amt_mis = get_secondary_data(mirror_heated_convex_claims)
mirror_heated_convex_paid_amt_bld_yr = rename_columns('bld_yr', mirror_heated_convex_paid_amt_bld_yr)
mirror_heated_convex_total_claims_fail_miles_isy = rename_columns('fail_miles', mirror_heated_convex_total_claims_fail_miles_isy)
mirror_heated_convex_paid_amt_mis = rename_columns('mis', mirror_heated_convex_paid_amt_mis)

mirror_heated_convex_cpc_cpu['CPC'] = mirror_heated_convex_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
mirror_heated_convex_cpc_cpu['CPU'] = mirror_heated_convex_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
mirror_heated_convex  = pd.concat([mirror_heated_convex_total_wty, mirror_heated_convex_cpc_cpu, mirror_heated_convex_top_customers,mirror_heated_convex_top_dealers,mirror_heated_convex_seag_info, mirror_heated_convex_paid_amt_bld_yr,mirror_heated_convex_total_claims_fail_miles_isy,mirror_heated_convex_paid_amt_mis], axis =1)
mirror_heated_convex.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/mirror_heated_convex.csv', index = False)


core_tank_radiator_total_wty,core_tank_radiator_cpc_cpu, core_tank_radiator_top_customers,core_tank_radiator_top_dealers, core_tank_radiator_seag_info = get_final_files(st_date, ed_date, core_tank_radiator_contracts, core_tank_radiator_claims, "core_tank_radiator")
core_tank_radiator_top_dealers.reset_index(inplace = True)
core_tank_radiator_total_wty = rename_columns('total_wty', core_tank_radiator_total_wty)
core_tank_radiator_cpc_cpu = rename_columns('cpc_cpu', core_tank_radiator_cpc_cpu)
core_tank_radiator_top_customers = rename_columns('top_customers', core_tank_radiator_top_customers)
core_tank_radiator_top_dealers = rename_columns('top_dealers', core_tank_radiator_top_dealers)

core_tank_radiator_paid_amt_bld_yr, core_tank_radiator_total_claims_fail_miles_isy ,core_tank_radiator_paid_amt_mis = get_secondary_data(core_tank_radiator_claims)
core_tank_radiator_paid_amt_bld_yr = rename_columns('bld_yr', core_tank_radiator_paid_amt_bld_yr)
core_tank_radiator_total_claims_fail_miles_isy = rename_columns('fail_miles', core_tank_radiator_total_claims_fail_miles_isy)
core_tank_radiator_paid_amt_mis = rename_columns('mis', core_tank_radiator_paid_amt_mis)

core_tank_radiator_cpc_cpu['CPC'] = core_tank_radiator_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
core_tank_radiator_cpc_cpu['CPU'] = core_tank_radiator_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
core_tank_radiator  = pd.concat([core_tank_radiator_total_wty, core_tank_radiator_cpc_cpu, core_tank_radiator_top_customers,core_tank_radiator_top_dealers,core_tank_radiator_seag_info, core_tank_radiator_paid_amt_bld_yr,core_tank_radiator_total_claims_fail_miles_isy,core_tank_radiator_paid_amt_mis], axis =1)
core_tank_radiator.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/core_tank_radiator.csv', index = False)


wiring_harness_total_wty,wiring_harness_cpc_cpu, wiring_harness_top_customers,wiring_harness_top_dealers, wiring_harness_seag_info = get_final_files(st_date, ed_date, wiring_harness_contracts, wiring_harness_claims, "wiring_harness")
wiring_harness_top_dealers.reset_index(inplace = True)
wiring_harness_total_wty = rename_columns('total_wty', wiring_harness_total_wty)
wiring_harness_cpc_cpu = rename_columns('cpc_cpu', wiring_harness_cpc_cpu)
wiring_harness_top_customers = rename_columns('top_customers', wiring_harness_top_customers)
wiring_harness_top_dealers = rename_columns('top_dealers', wiring_harness_top_dealers)

wiring_harness_paid_amt_bld_yr, wiring_harness_total_claims_fail_miles_isy ,wiring_harness_paid_amt_mis = get_secondary_data(wiring_harness_claims)
wiring_harness_paid_amt_bld_yr = rename_columns('bld_yr', wiring_harness_paid_amt_bld_yr)
wiring_harness_total_claims_fail_miles_isy = rename_columns('fail_miles', wiring_harness_total_claims_fail_miles_isy)
wiring_harness_paid_amt_mis = rename_columns('mis', wiring_harness_paid_amt_mis)

wiring_harness_cpc_cpu['CPC'] = wiring_harness_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
wiring_harness_cpc_cpu['CPU'] = wiring_harness_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
wiring_harness  = pd.concat([wiring_harness_total_wty, wiring_harness_cpc_cpu, wiring_harness_top_customers,wiring_harness_top_dealers,wiring_harness_seag_info, wiring_harness_paid_amt_bld_yr,wiring_harness_total_claims_fail_miles_isy,wiring_harness_paid_amt_mis], axis =1)
wiring_harness.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/wiring_harness.csv', index = False)

 
heater_aux_total_wty,heater_aux_cpc_cpu, heater_aux_top_customers,heater_aux_top_dealers, heater_aux_seag_info = get_final_files(st_date, ed_date, heater_aux_contracts, heater_aux_claims, "heater_aux")
heater_aux_top_dealers.reset_index(inplace = True)
heater_aux_total_wty = rename_columns('total_wty', heater_aux_total_wty)
heater_aux_cpc_cpu = rename_columns('cpc_cpu', heater_aux_cpc_cpu)
heater_aux_top_customers = rename_columns('top_customers', heater_aux_top_customers)
heater_aux_top_dealers = rename_columns('top_dealers', heater_aux_top_dealers)

heater_aux_paid_amt_bld_yr, heater_aux_total_claims_fail_miles_isy ,heater_aux_paid_amt_mis = get_secondary_data(heater_aux_claims)
heater_aux_paid_amt_bld_yr = rename_columns('bld_yr', heater_aux_paid_amt_bld_yr)
heater_aux_total_claims_fail_miles_isy = rename_columns('fail_miles', heater_aux_total_claims_fail_miles_isy)
heater_aux_paid_amt_mis = rename_columns('mis', heater_aux_paid_amt_mis)

heater_aux_cpc_cpu['CPC'] = heater_aux_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
heater_aux_cpc_cpu['CPU'] = heater_aux_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
heater_aux  = pd.concat([heater_aux_total_wty, heater_aux_cpc_cpu, heater_aux_top_customers,heater_aux_top_dealers,heater_aux_seag_info, heater_aux_paid_amt_bld_yr,heater_aux_total_claims_fail_miles_isy,heater_aux_paid_amt_mis], axis =1)
heater_aux.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/heater_aux.csv', index = False)

ac_compressor_total_wty,ac_compressor_cpc_cpu, ac_compressor_top_customers,ac_compressor_top_dealers, ac_compressor_seag_info = get_final_files(st_date, ed_date, ac_compressor_contracts, ac_compressor_claims, "ac_compressor")
ac_compressor_top_dealers.reset_index(inplace = True)
ac_compressor_total_wty = rename_columns('total_wty', ac_compressor_total_wty)
ac_compressor_cpc_cpu = rename_columns('cpc_cpu', ac_compressor_cpc_cpu)
ac_compressor_top_customers = rename_columns('top_customers', ac_compressor_top_customers)
ac_compressor_top_dealers = rename_columns('top_dealers', ac_compressor_top_dealers)

ac_compressor_paid_amt_bld_yr, ac_compressor_total_claims_fail_miles_isy ,ac_compressor_paid_amt_mis = get_secondary_data(ac_compressor_claims)
ac_compressor_paid_amt_bld_yr = rename_columns('bld_yr', ac_compressor_paid_amt_bld_yr)
ac_compressor_total_claims_fail_miles_isy = rename_columns('fail_miles', ac_compressor_total_claims_fail_miles_isy)
ac_compressor_paid_amt_mis = rename_columns('mis', ac_compressor_paid_amt_mis)

ac_compressor_cpc_cpu['CPC'] = ac_compressor_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Total_Claims_cpc_cpu'] if x['Total_Claims_cpc_cpu'] > 0 else 0, axis =1)
ac_compressor_cpc_cpu['CPU'] = ac_compressor_cpc_cpu.apply(lambda x: x['Total_Cost_cpc_cpu']/x['Active_Units'] if x['Active_Units'] >0 else 0, axis =1)
ac_compressor  = pd.concat([ac_compressor_total_wty, ac_compressor_cpc_cpu, ac_compressor_top_customers,ac_compressor_top_dealers,ac_compressor_seag_info, ac_compressor_paid_amt_bld_yr,ac_compressor_total_claims_fail_miles_isy,ac_compressor_paid_amt_mis], axis =1)
ac_compressor.to_csv(r'Z:/2000_Cost Management/2500_General Information/2502_Processes & Documentation/Tableau Documentation/All_dashboards_combined/ac_compressor.csv', index = False)
