# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 11:55:39 2021

@author: SHANBHS
"""
import pandas as pd
from datetime import datetime
import calendar
import time
import math
import numpy as np
from Rate_Planning_Accuracy_Metricsv2 import change_engine_name, get_coverage_data, get_mapping_data, generate_component_rates,get_rate_plan_info, get_emission_grp, paired_mask, modify_voc_category
from Rate_Planning_Accuracy_Metricsv2 import get_failure_rate_data, get_cpc_data, get_pareto_data, get_vmrs_data, get_mapping2_data, get_actuals, get_MIS, get_unit_maturity

# =============================================================================
# user input
# =============================================================================
cutoff_date = '2021-03-31'
rate_plan_ids = ["115"]
plan_id = "115"
# =============================================================================
# END USER INPUT
# =============================================================================
final_start_time = time.time()
cutoff_date = datetime.strptime(cutoff_date, '%Y-%m-%d').date()
paid_date_cutoff = datetime(cutoff_date.year, cutoff_date.month + 1, calendar.monthrange(cutoff_date.year, cutoff_date.month+1)[-1]).date()
repair_date_cutoff = cutoff_date
report_date = datetime.today().date().replace(day=1)

rp_info_dicts = get_rate_plan_info(rate_plan_ids)   
map1 = get_mapping_data(rp_info_dicts)
map2 = get_mapping2_data(rp_info_dicts)
failure_rate_info = get_failure_rate_data(rp_info_dicts)
cpc_info = get_cpc_data(rp_info_dicts)
pareto_data = get_pareto_data(rp_info_dicts)
vmrs_grp_data = get_vmrs_data(rp_info_dicts)

waspp_raw_data, vmrs_raw_data, coverage_raw_data = get_coverage_data()
actuals_raw_data = get_actuals(str(repair_date_cutoff), str(paid_date_cutoff))
end_time = (time.time()-final_start_time)/60
print("All data data loaded in  ", end_time )
                                                 

pareto = pareto_data.copy()
failure_rate_data = failure_rate_info.copy()
cpc_data = cpc_info.copy()
pareto = pareto.reset_index(drop= True)
map1_data = map1.copy()
map2_data = map2.copy()
map1_data = map1_data.reset_index(drop = True)
map2_data = map2_data.reset_index(drop = True)


cvrg_data = coverage_raw_data.copy()
vmrs_data = vmrs_raw_data.copy()
waspp_data = waspp_raw_data.copy()
actuals_data = actuals_raw_data.copy()


pareto['PRODUCT_TYP'] = pareto.apply(lambda x: change_engine_name(x['PRODUCT_TYP'], x['EMISSION_SRVC_YEAR']), axis = 1) #match the engine names in both locations
map1_data['TRK_CLASS'] = map1_data.apply(lambda x: change_engine_name(x['TRK_CLASS'], x['COST_IN_SRVC_YEAR']), axis = 1)
map2_data['PRODUCT_TYP'] = map2_data.apply(lambda x: change_engine_name(x['PRODUCT_TYP'], x['IN_SRVC_YEAR']), axis = 1)
failure_rate_data['PRODUCT_TYP'] = failure_rate_data.apply(lambda x: change_engine_name(x['PRODUCT_TYP'], x['EMISSION_SRVC_YEAR']), axis = 1) #match the engine names in both locations
cpc_data['PRODUCT_TYP'] = cpc_data.apply(lambda x: change_engine_name(x['PRODUCT_TYP'], x['EMISSION_SRVC_YEAR']), axis = 1) #match the engine names in both locations
map1_data['EMISSION_GRP'] = map1_data.apply(lambda x: get_emission_grp(x['COST_IN_SRVC_YEAR']), axis = 1)
waspp_data = waspp_data.loc[waspp_data['PRODUCT_CLASS'].notna()]

waspp_data['EMISSION_GRP'] = waspp_data.apply(lambda x: get_emission_grp(x['PRODUCT_CLASS']), axis =1)


cvrg_data['NEW_VOC_CATEGORY'] = cvrg_data.apply(lambda x: modify_voc_category(x['VOC_CATEGORY']), axis =1)
cvrg_data = cvrg_data.loc[paired_mask(cvrg_data, map1_data, 'ENG_BASE_MDL_CD', 'ENG_TORQUE_GRP', 'NEW_VOC_CATEGORY', 'EMISSION_SUB_GRP', 'COVERAGE_DEDUCTION_AMT',
                                     'TRK_CLASS', 'TORQUE', 'VOC_CATEGORY', 'EMISSION_GRP', 'DEDUCT_AMT')]

waspp_data['TORQUE'] = waspp_data.apply(lambda x: "HIGH" if x['TORQUE']== 'H' else "LOW", axis = 1)

waspp_data = waspp_data.loc[paired_mask(waspp_data, map1_data, 'PRODUCT_CLASS', 'TORQUE', 'VOC_CATEGORY', 'EMISSION_GRP', 'COVERAGE_DEDUCTION_AMT',
                                     'TRK_CLASS', 'TORQUE', 'VOC_CATEGORY', 'EMISSION_GRP', 'DEDUCT_AMT')]

# =============================================================================
# 
# =============================================================================
cvrg_data['MIS'] = cvrg_data.apply(lambda x: get_MIS(report_date, x['IN_SERVICE_DT']),axis = 1)
cvrg_data['MATURITY_INDICATOR'] = cvrg_data.apply(lambda x: get_unit_maturity(x['COVERAGE_MONTHS'], x['MIS']), axis = 1) 
cvrg_data['CONCAT_ID'] = cvrg_data.apply(lambda x: x['ENG_BASE_MDL_CD'] + "_" + x['COVERAGE_PACKAGE']+ "_" +str(x['COVERAGE_MONTHS']) + "_" + x['COVERAGE_DISTANCE1'], axis =1)

vmrs_grp_data['MANUALLY_PLANNED'] = vmrs_grp_data.apply(lambda x: 0  if "EW" in x['VMRS_GROUP'] else 1, axis =1)
pareto = pareto.merge(vmrs_grp_data, left_on  = "VMRS_33_CD", right_on = "VMRS_33_CD")
pareto.drop_duplicates(inplace = True)
total = pareto.groupby(["EMISSION_SRVC_YEAR","VMRS_GROUP", "PRODUCT_TYP"]).SUM_EXT_CLAIM_COST_USD.sum().reset_index()  # get totals for EW1-4 for each emission year
pareto = pd.merge(pareto, total, how='left', left_on = ['EMISSION_SRVC_YEAR', 'VMRS_GROUP', 'PRODUCT_TYP'], right_on = ['EMISSION_SRVC_YEAR', 'VMRS_GROUP', 'PRODUCT_TYP'])
pareto = pareto.rename(columns = {'SUM_EXT_CLAIM_COST_USD_x':'SUM_EXT_CLAIM_COST_USD', 'SUM_EXT_CLAIM_COST_USD_y':'TOTAL_EXT_COST_USD'})
pareto["RATIO"] = pareto.apply(lambda x: x['SUM_EXT_CLAIM_COST_USD']/ x['TOTAL_EXT_COST_USD'], axis= 1)

pareto["RATIO"] = pareto.apply(lambda x: x["RATIO"] if x["EMISSION_SRVC_YEAR"]==rp_info_dicts[plan_id][2] else None, axis = 1)
pareto.RATIO = pareto.groupby('VMRS_33_CD').RATIO.apply(lambda x : x.ffill().bfill())

## assign packages to each VMRS code, when included
cvrg_cd_pkg_lookup = waspp_data[['COVERAGE_CD', 'COVERAGE_PACKAGE']]
cvrg_cd_pkg_lookup = cvrg_cd_pkg_lookup.drop_duplicates()
cvrg_cd_pkg_lookup = cvrg_cd_pkg_lookup.rename(columns = {'COVERAGE_CD': 'CVRG_CD'})
vmrs_data = vmrs_data.merge(cvrg_cd_pkg_lookup, how = "left", on = "CVRG_CD")
vmrs_data = vmrs_data.loc[vmrs_data['IS_COVERED'] == 'INCLUDE']
vmrs_data.drop('CVRG_CD', inplace= True, axis = 1)
vmrs_data.drop_duplicates(inplace= True)
vmrs_data.drop('IS_COVERED', inplace = True, axis = 1)

cvrg_data["STATUS"] = cvrg_data.apply(lambda x: "EXPIRED" if x['COVERAGE_END_DT'] <=cutoff_date else 'ACTIVE', axis = 1)
#change mis to cvrg months if unit is expired TODO
cvrg_data = cvrg_data.merge(waspp_data[['VIN', 'CURR_RATE', 'REM_COST_CCC_SUM']] , how = 'left', left_on=['VIN'], right_on = ['VIN'])
cvrg_data['CURR_RATE'] = cvrg_data.apply(lambda x: x['CURR_RATE'] if x['STATUS'] =='ACTIVE' else 1, axis = 1)
cvrg_data['REM_COST_CCC_SUM'] = cvrg_data.apply(lambda x: x['REM_COST_CCC_SUM'] if x['STATUS'] =='ACTIVE' else 0, axis = 1)
cvrg_data = cvrg_data.loc[cvrg_data['CURR_RATE'].isnull() == False]

cvrg_data['IN_SRVC_MONTH_YEAR'] = cvrg_data.apply(lambda x: datetime.strptime(str(x['IN_SERVICE_DT']), "%Y-%m-%d") , axis = 1) # get month _year from in service year
cvrg_data['IN_SRVC_YEAR'] = cvrg_data.apply(lambda x: str(x['IN_SRVC_MONTH_YEAR'].year) , axis = 1) #get_year
cvrg_data['IN_SRVC_MONTH_YEAR'] = cvrg_data.apply(lambda x: str(x['IN_SRVC_MONTH_YEAR'].month) + str(x['IN_SRVC_MONTH_YEAR'].year), axis =1)
cvrg_data= cvrg_data.merge(actuals_data[['VIN','VMRS_33', 'TOTAL_WTY_COST_SUM']] , how = 'left', left_on = ['VIN'], right_on =['VIN'])


cvrg_data["MIS"] = cvrg_data.apply(lambda x: x["MIS"] if x["STATUS"]=="ACTIVE" else x["COVERAGE_MONTHS"], axis = 1)

cvrg_data_null = cvrg_data.loc[cvrg_data['CURR_RATE'].isnull() == True] # why are these not in WASPP???  ¯\_(ツ)_/¯

cvrg_data_vin_count = cvrg_data.groupby(['ENG_BASE_MDL_CD','EMISSION_SUB_GRP','IN_SRVC_YEAR','IN_SRVC_MONTH_YEAR', "COVERAGE_MONTHS", 
                                         "COVERAGE_DISTANCE1","COVERAGE_PACKAGE", 'STATUS']).agg({'CURR_RATE':'sum','REM_COST_CCC_SUM':'sum', 'VIN':pd.Series.nunique,'MIS': np.mean, 'MATURITY_INDICATOR': np.mean}).reset_index().rename_axis(None, axis=1)

cvrg_summary_all = cvrg_data_vin_count.merge(vmrs_data,how = 'left', left_on=['COVERAGE_PACKAGE'], right_on= ['COVERAGE_PACKAGE'])

cvrg_data_total = cvrg_data.groupby(['ENG_BASE_MDL_CD','EMISSION_SUB_GRP','IN_SRVC_YEAR','IN_SRVC_MONTH_YEAR', "COVERAGE_MONTHS", "COVERAGE_DISTANCE1",
                                     "COVERAGE_PACKAGE", 'STATUS', 'VMRS_33']).agg({'TOTAL_WTY_COST_SUM': 'sum'}).reset_index().rename_axis(None, axis=1)

cvrg_data_summary = cvrg_summary_all.merge(cvrg_data_total[['ENG_BASE_MDL_CD','EMISSION_SUB_GRP', 'IN_SRVC_YEAR','IN_SRVC_MONTH_YEAR', 'COVERAGE_MONTHS','COVERAGE_DISTANCE1', 
                                                            'COVERAGE_PACKAGE','STATUS', 'VMRS_33','TOTAL_WTY_COST_SUM']], how = 'left', left_on = ['ENG_BASE_MDL_CD','EMISSION_SUB_GRP', 'IN_SRVC_YEAR','IN_SRVC_MONTH_YEAR', 'COVERAGE_MONTHS','COVERAGE_DISTANCE1', 'COVERAGE_PACKAGE','STATUS','VMRS_33'], right_on=['ENG_BASE_MDL_CD','EMISSION_SUB_GRP', 'IN_SRVC_YEAR','IN_SRVC_MONTH_YEAR', 'COVERAGE_MONTHS','COVERAGE_DISTANCE1', 'COVERAGE_PACKAGE','STATUS','VMRS_33'])

#cvrg_data_summary['CURR_RATE'] = cvrg_data_summary.apply(lambda x: 1 if x['CURR_RATE'] == x['VIN'] else x['CURR_RATE'], axis =1) # for all the expired units, the CURR_RATE = gets added. so resetting it to 1
cvrg_data_summary['REM_RATE_PCT'] = cvrg_data_summary.apply(lambda x: x['REM_COST_CCC_SUM']/ x['CURR_RATE'], axis = 1)
cvrg_data_summary['PAID_RATE_PCT'] = cvrg_data_summary.apply(lambda x: 1-x['REM_RATE_PCT'], axis = 1)
cvrg_data_summary['EMISSION_SRVC_YEAR'] = cvrg_data_summary.apply(lambda x: str(x['EMISSION_SUB_GRP']) +"-"+ str(x['IN_SRVC_YEAR']), axis = 1)
cvrg_summary = cvrg_data_summary.copy()

# =============================================================================
# Create a summary table by grouping contracts based on engine, ISY-M, time , mileage and package
# =============================================================================

cvrg_summary.rename(columns = {'COVERAGE_MONTHS':'USER_CVRG_MONTHS', 'COVERAGE_DISTANCE1':'USER_MILES'}, inplace = True)

engines = list(pareto['PRODUCT_TYP'].unique())
engines_in_pareto = cvrg_summary.loc[cvrg_summary['ENG_BASE_MDL_CD'].isin(engines)]
engines_in_pareto['USER_CVRG_MONTHS'] = engines_in_pareto['USER_CVRG_MONTHS'].astype(int)
engines_in_pareto['USER_MILES'] = engines_in_pareto['USER_MILES'].astype(int)


time_mileage_combinations = engines_in_pareto.groupby(['USER_CVRG_MONTHS', 'USER_MILES']).size().reset_index().rename(columns={0:'count'})
time_mileage_combinations_dict = time_mileage_combinations.to_dict('list')

component_rates_part = generate_component_rates(time_mileage_combinations_dict, pareto, cpc_data, map1_data, map2_data, failure_rate_data)
component_rates_part['STRATEGIC_RATE'].fillna(0, inplace = True)
component_rates_part['REAL_RATE'].fillna(0, inplace = True)

engines_with_rates = engines_in_pareto.merge(component_rates_part[['STRATEGIC_RATE','REAL_RATE', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP', 'VMRS_33_CD', 'USER_CVRG_MONTHS', 'USER_MILES']], how = 'left', left_on =['EMISSION_SRVC_YEAR', 'ENG_BASE_MDL_CD', 'VMRS_33', 'USER_CVRG_MONTHS', 'USER_MILES'], right_on = ['EMISSION_SRVC_YEAR', 'PRODUCT_TYP', 'VMRS_33_CD', 'USER_CVRG_MONTHS', 'USER_MILES'])
engines_with_rates['ID'] = engines_with_rates.apply(lambda x: x['ENG_BASE_MDL_CD'] + "_" +str(x['VMRS_33']) + "_" + str(x['USER_MILES'])+ "_" + str(x['USER_CVRG_MONTHS']), axis =1)
engines_with_rates.STRATEGIC_RATE=engines_with_rates.groupby('ID').STRATEGIC_RATE.apply(lambda x : x.ffill().bfill())
engines_with_rates.REAL_RATE=engines_with_rates.groupby('ID').REAL_RATE.apply(lambda x : x.ffill().bfill())

engines_with_rates['PAID_RATE_STRATEGIC'] = engines_with_rates.apply(lambda x: x['STRATEGIC_RATE'] * x['PAID_RATE_PCT'] * x['VIN'], axis =1)
engines_with_rates['REMAINING_RATE_STRATEGIC'] = engines_with_rates.apply(lambda x: x['STRATEGIC_RATE'] * x['REM_RATE_PCT'] * x['VIN'], axis =1)
engines_with_rates['PAID_RATE_REAL'] = engines_with_rates.apply(lambda x: x['REAL_RATE'] * x['PAID_RATE_PCT'] * x['VIN'], axis =1)
engines_with_rates['REMAINING_RATE_REAL'] = engines_with_rates.apply(lambda x: x['REAL_RATE'] * x['REM_RATE_PCT'] * x['VIN'], axis =1)

engines_with_rates.drop('PRODUCT_TYP', inplace=  True, axis = 1)
engines_with_rates.drop('VMRS_33_CD', inplace=  True, axis = 1)
engines_with_rates['TOTAL_WTY_COST_SUM'].fillna(0, inplace = True)


df_size = len(engines_with_rates)
rates_1 = engines_with_rates.iloc[0:math.floor(df_size/2), :]
rates_2 = engines_with_rates.iloc[math.floor(df_size/2): df_size+1, :]

rates_1.to_csv("1_engines_with_rates_115 "+str(datetime.today().date()) + ".csv")
rates_2.to_csv("2_engines_with_rates_115 "+str(datetime.today().date()) + ".csv")

code_end_time = (time.time()-final_start_time)/60
print("code ran in  ", code_end_time )




