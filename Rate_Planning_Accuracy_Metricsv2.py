# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 10:49:16 2021

@author: SHANBHS
"""
import pandas as pd
from datetime import datetime
import time
from sap_hana_connect import establish_connection
from dateutil import relativedelta

had = establish_connection('had')
hap = establish_connection('hap')
haq = establish_connection('haq')



# get details from rate plan ids needed for SQL queries
def get_rate_plan_info(rate_plan_ids):
    rp_info_dicts ={}
    for plan in rate_plan_ids:
        temp_list = []
        rp_info_sql = """
        SELECT DISTINCT "RATE_PLAN_CUTOFF_DT", "PRODUCT_TYPE", "EMISSION_GRP_SRVC_YEAR_BASE"
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_DEF"('PLACEHOLDER' = ('$$parRatePlanId$$', '"""+plan+"""'))"""
        info = pd.read_sql(rp_info_sql, hap)
        temp_list.append(datetime.strptime(info["RATE_PLAN_CUTOFF_DT"].unique().item(), '%m/%d/%Y').strftime("%Y-%m-%d"))
        temp_list.append(info["PRODUCT_TYPE"].unique().item())
        temp_list.append(info["EMISSION_GRP_SRVC_YEAR_BASE"].unique().item())
        rp_info_dicts[plan] = temp_list
    return rp_info_dicts  

def get_vmrs_data(rp_info_dicts):
    vmrs_grp_data = pd.DataFrame()    
    for plan_id in rp_info_dicts:
        vmrs_grp_sql = """
        SELECT DISTINCT "VMRS_33_CD","VMRS_GROUP"
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_VMRS_SEL_ALL"
        ('PLACEHOLDER' = ('$$parPortfolio$$', 'CHASSIS'), 
         'PLACEHOLDER' = ('$$parRatePlanId$$', '"""+plan_id+"""'))
        """
        vmrs = pd.read_sql(vmrs_grp_sql, hap)
        vmrs_grp_data = vmrs_grp_data.append(vmrs)
    return vmrs_grp_data

def get_failure_rate_data(rp_info_dicts):
    failure_rate_data = pd.DataFrame()
    for plan_id in rp_info_dicts:
        failure_rate_sql = """
        SELECT "RATE_PLAN_ID", "VMRS_33_CD", "CVRG_GROUP", "CVRG_MONTHS", "DOMCL_CNTRY_CD", "VOC_CATEGORY", 
        "ENGINE_TORQUE", "DEDUCT_AMT", "BASELINE_INDICATOR", "IN_SRVC_YEAR", "TECH_ADJ_1_COMMENTS", 
        "TECH_ADJ_2_COMMENTS", "CREATED_DATE", "CREATED_USER", "MODIFIED_DATE", "MODIFIED_USER", "PRD_TYP_CD", 
        "PRODUCT_TYP", "EMISSION_SRVC_YEAR", sum("TECH_ADJ_1") AS "TECH_ADJ_1", sum("REAL_FAILURE_RATE") AS "REAL_FAILURE_RATE", 
        sum("TECH_ADJ_2") AS "TECH_ADJ_2", sum("STRATEGIC_FAILURE_RATE") AS "STRATEGIC_FAILURE_RATE" 
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_FAILURE_RATE_INPUT"
        WHERE ("RATE_PLAN_ID" IN ("""+ plan_id +""")) GROUP BY "RATE_PLAN_ID", "VMRS_33_CD", "CVRG_GROUP", "CVRG_MONTHS", 
        "DOMCL_CNTRY_CD", "VOC_CATEGORY", "ENGINE_TORQUE", "DEDUCT_AMT", "BASELINE_INDICATOR", "IN_SRVC_YEAR", 
        "TECH_ADJ_1_COMMENTS", "TECH_ADJ_2_COMMENTS", "CREATED_DATE", "CREATED_USER", "MODIFIED_DATE", 
        "MODIFIED_USER", "PRD_TYP_CD", "PRODUCT_TYP", "EMISSION_SRVC_YEAR"
        """
        fail = pd.read_sql(failure_rate_sql, hap)
        failure_rate_data = failure_rate_data.append(fail)
    return failure_rate_data

def get_cpc_data(rp_info_dicts):
    cpc_data = pd.DataFrame()
    for plan_id in rp_info_dicts:
        cpc_sql = """
        SELECT "RATE_PLAN_ID", "RATE_PLAN_TERM", "PRD_TYPE", "VMRS_33_CD", "CREATED_DT", "CREATED_USER", 
        "MODIFIED_DT", "MODIFIED_USER", "CPC_COMMENT", "YEAR", "EMISSION_SRVC_YEAR", 
        sum("EW_CPC_REAL") AS "EW_CPC_REAL",sum("EW_CPC_STRATEGIC") AS "EW_CPC_STRATEGIC" 
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_USER_INPUT_REAL_CPC"
        WHERE ("RATE_PLAN_ID" IN ("""+ plan_id +""")) 
        GROUP BY "RATE_PLAN_ID", "RATE_PLAN_TERM", "PRD_TYPE", "VMRS_33_CD", "CREATED_DT", "CREATED_USER", 
        "MODIFIED_DT", "MODIFIED_USER", "CPC_COMMENT", "YEAR", "EMISSION_SRVC_YEAR"
        """
        cpc = pd.read_sql(cpc_sql, hap)
        cpc["PRODUCT_TYP"] = rp_info_dicts[plan_id][1]
        cpc_data = cpc_data.append(cpc)
    return cpc_data

def get_pareto_data(rp_info_dicts):
    pareto_data = pd.DataFrame()
    for plan_id in rp_info_dicts:
        pareto_sql = """SELECT "EMISSION_SRVC_YEAR", "VMRS_33_CD", "VMRS_33_ASSEMB_DESC", "RANK", "SELECTION_FLAG", 
        sum("SUM_EXT_CLAIM_COST_USD") AS "SUM_EXT_CLAIM_COST_USD", sum("CLAIM_COUNT") AS "CLAIM_COUNT" 
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_ENG_PARETO_SRVC_YR"
        ('PLACEHOLDER' = ('$$parEndDate$$', '"""+rp_info_dicts[plan_id][0]+"""'),
        'PLACEHOLDER' = ('$$parProductGroup$$', '"""+rp_info_dicts[plan_id][1]+"""'), 
        'PLACEHOLDER' = ('$$parEmissionServiceYear$$', '"""+rp_info_dicts[plan_id][2]+"""'), 
        'PLACEHOLDER' = ('$$parRatePlanId$$', '"""+plan_id+"""')) 
        GROUP BY "EMISSION_SRVC_YEAR", "VMRS_33_CD", "VMRS_33_ASSEMB_DESC", "RANK", "SELECTION_FLAG"
        """
        par = pd.read_sql(pareto_sql, hap)
        par["PRODUCT_TYP"] = rp_info_dicts[plan_id][1]
        pareto_data = pareto_data.append(par)
    return pareto_data
        
 
def get_mapping_data(rp_info_dicts):
    map1_data = pd.DataFrame()             
    for plan_id in rp_info_dicts:        
        map1_sql = """
        SELECT "REAL_COST", "STRATEGIC_COST", "RATE_PLAN_ID", "PORTFOLIO", "MONTHS", "DOMCL_CNTRY_CD", "TORQUE", 
        "DEDUCT_AMT", "VOC_LVL", "CVRG_CD", "PKSMT", "FCCC_MODEL", "COST_IN_SRVC_YEAR", "COST_BASIS_YEAR", 
        "MCF", "MOC", "VOC_CATEGORY", "REAL_IMPROVEMENT_RATIO", "FINAL_AUF_FACTOR", "MILEAGE", "UNADJUSTED_METHOD_RATE", 
        "NET_COST_FACTOR_CALC", "TRK_CLASS", "COST_COVERAGE", "CC_INFLATION_RATE", "VENDOR_RECOVERY_FACTOR_CALC","INFLATION_RATE" 
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_REAL_STRATEGIC_COST_TIME"('PLACEHOLDER' = ('$$parPortfolio$$', 'ENGINE'))
        WHERE ("RATE_PLAN_ID" IN ("""+ plan_id +"""))
        """    
        map1 = pd.read_sql(map1_sql, hap)
        map1["PRODUCT_TYP"] = rp_info_dicts[plan_id][1]
        map1_data = map1_data.append(map1) 
    return (map1_data)  

def get_mapping2_data(rp_info_dicts):    
    map2_data = pd.DataFrame()          
    for plan_id in rp_info_dicts:
        map2_sql="""SELECT "RATE_PLAN_ID", "PORTFOLIO", "MILES", "TORQUE", "DOMCL_CNTRY", "DEDUCT_AMT", "MOC", "FCCC_MODEL", 
        "VOC_LVL", "CVRG_CD", "PKSMT", "COST_BASIS_YEAR", "VOC_CATEGORY", "IN_SRVC_YEAR", "TRK_CLASS", "CVRG_GROUP", 
        sum("ACC_FACTOR") AS "ACC_FACTOR", sum("FINAL_AUF_FACTOR") AS "FINAL_AUF_FACTOR" 
        FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.RatePlanning2.Views/CV_RP_MAPPING_FILE_2_MILEAGE"('PLACEHOLDER' = ('$$parRATE_PLAN_ID$$', '"""+plan_id+"""')) 
        GROUP BY "RATE_PLAN_ID", "PORTFOLIO", "MILES", "TORQUE", "DOMCL_CNTRY", "DEDUCT_AMT", "MOC", "FCCC_MODEL", 
        "VOC_LVL", "CVRG_CD", "PKSMT", "COST_BASIS_YEAR", "VOC_CATEGORY", "IN_SRVC_YEAR", "TRK_CLASS", "CVRG_GROUP"
        """   
        map2 = pd.read_sql(map2_sql, hap)
        map2["PRODUCT_TYP"] = rp_info_dicts[plan_id][1]
        map2_data = map2_data.append(map2)                
    return (map2_data)      
    

def change_engine_name(engine, emission):
    engine_name = ''
    if "GHG17" in emission:
        if engine == "DD15 AT":
             engine_name = 'DD15GHG17AT'
        elif engine == "DD13":
            engine_name = "DD13GHG17"
    elif "GHG14" in emission:
        if engine == "DD13":
            engine_name = "DD13GHG14"
    else:
        engine_name = engine
    return engine_name    

#user_time_mileage = {'USER_CVRG_MONTHS':[40],'USER_MILES': [450000]}
def generate_component_rates(time_mileage_combinations_dict, pareto, cpc_data, map1_data, map2_data, failure_rate_data):
    user_time_mileage = time_mileage_combinations_dict.copy()
    ref_time_mileage = {'CVRG_MONTHS': [36, 48, 60, 72],
                          'MILES': [300000,400000,500000,600000]}
    temp_tm = pd.DataFrame.from_dict(ref_time_mileage)
    temp_user_tm = pd.DataFrame.from_dict(user_time_mileage)
    temp_tm['key']  = 1
    temp_user_tm['key']  = 1
    
    component_rates = pareto.copy()
    component_rates = component_rates[['EMISSION_SRVC_YEAR', 'VMRS_33_CD', 'VMRS_33_ASSEMB_DESC', 'VMRS_GROUP', 'PRODUCT_TYP', 'RATIO', 'MANUALLY_PLANNED']]
    component_rates['key'] = 1
    component_rates = pd.merge(component_rates, temp_tm,left_on='key', right_on='key')
    
    #merge with Failure rate data
    component_rates = component_rates.merge(failure_rate_data[['VMRS_33_CD', 'CVRG_MONTHS', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP','STRATEGIC_FAILURE_RATE','REAL_FAILURE_RATE']], how = 'left', left_on = ['VMRS_GROUP', 'CVRG_MONTHS', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP'], right_on = ['VMRS_33_CD', 'CVRG_MONTHS', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP'])
    component_rates = component_rates.merge(cpc_data[['VMRS_33_CD', 'EW_CPC_STRATEGIC','EW_CPC_REAL', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP']], how = 'left', left_on = ['VMRS_GROUP', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP'], right_on = ['VMRS_33_CD', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP'])
    component_rates.drop('VMRS_33_CD_y', inplace= True, axis = 1)
    component_rates.rename(columns = {'VMRS_33_CD_x':'VMRS_33_CD'}, inplace = True)
    component_rates["GROSS STR. RATE"] = component_rates.apply(lambda x: x['STRATEGIC_FAILURE_RATE']*x['EW_CPC_STRATEGIC'], axis = 1) # do I need this??
    
    #get rates for EW4 only
    map1_EW4 = map1_data.loc[map1_data["CVRG_CD"] == "EW4"]
    map2_EW4 = map2_data.loc[map2_data["CVRG_CD"] == "EW4"]
    map2_EW4['MILES'] = map2_EW4['MILES'].astype(int)
    map2_EW4['MOC'] = map2_EW4['MOC'].astype(int)
    component_rates = component_rates.merge(map1_EW4[['MONTHS', 'COST_IN_SRVC_YEAR', 'TRK_CLASS', 'MCF', 'STRATEGIC_COST','REAL_COST']],how = 'left', left_on = ['CVRG_MONTHS', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP'], right_on= ['MONTHS', 'COST_IN_SRVC_YEAR', 'TRK_CLASS'])
    component_rates.drop('MONTHS', inplace=True, axis = 1)
    component_rates.drop('COST_IN_SRVC_YEAR', inplace= True, axis = 1)
    component_rates.drop_duplicates(inplace= True)    
    component_rates = component_rates.merge(map2_EW4[['MILES', 'IN_SRVC_YEAR', 'PRODUCT_TYP','MOC', 'ACC_FACTOR']], how='left', left_on = ['MILES', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP','CVRG_MONTHS'], right_on  = ['MILES', 'IN_SRVC_YEAR', 'PRODUCT_TYP','MOC'])
    component_rates.drop('MOC', inplace=True, axis = 1)
    component_rates.drop('IN_SRVC_YEAR', inplace= True, axis = 1)
    component_rates.drop_duplicates(inplace= True)
    component_rates["EW4_REF_RATE_STRATEGIC"] = component_rates.apply(lambda x: x['STRATEGIC_COST']* x['MCF'] * x['ACC_FACTOR'], axis = 1)
    component_rates["EW4_REF_RATE_REAL"] = component_rates.apply(lambda x: x['REAL_COST']* x['MCF'] * x['ACC_FACTOR'], axis = 1)
    
    component_rates = pd.merge(component_rates, temp_user_tm,left_on='key', right_on='key')
    component_rates.drop('key', inplace= True, axis = 1)
    component_rates = component_rates.merge(map1_EW4[['MONTHS', 'COST_IN_SRVC_YEAR', 'TRK_CLASS', 'MCF', 'STRATEGIC_COST','REAL_COST']],how = 'left', left_on = ['USER_CVRG_MONTHS', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP'], right_on= ['MONTHS', 'COST_IN_SRVC_YEAR', 'TRK_CLASS'])
    component_rates.drop('MONTHS', inplace=True, axis = 1)
    component_rates.drop('COST_IN_SRVC_YEAR', inplace= True, axis = 1)
    component_rates.drop_duplicates(inplace= True)
    
    component_rates = component_rates.merge(map2_EW4[['MILES', 'IN_SRVC_YEAR', 'PRODUCT_TYP','MOC', 'ACC_FACTOR']], how='left', left_on = ['USER_MILES', 'EMISSION_SRVC_YEAR', 'PRODUCT_TYP','CVRG_MONTHS'], right_on  = ['MILES', 'IN_SRVC_YEAR', 'PRODUCT_TYP','MOC'])
    component_rates.drop('MOC', inplace=True, axis = 1)
    component_rates.drop('IN_SRVC_YEAR', inplace= True, axis = 1)
    component_rates.drop('MILES_y', inplace= True, axis = 1)
    component_rates.drop_duplicates(inplace= True)
    component_rates.rename(columns = {'MCF_y':'MCF_USER', 'STRATEGIC_COST_y': 'STRATEGIC_COST_USER', 'ACC_FACTOR_y': 'ACC_FACTOR_USER'}, inplace = True)
    component_rates.rename(columns = {'REAL_COST_y': 'REAL_COST_USER'}, inplace = True)
    component_rates.drop('MILES_x', inplace= True, axis = 1)
    component_rates.drop('TRK_CLASS_x', inplace= True, axis = 1)
    component_rates.drop('TRK_CLASS_y', inplace= True, axis = 1)
    component_rates.drop('MCF_x', inplace= True, axis = 1)
    component_rates.drop('STRATEGIC_COST_x', inplace= True, axis = 1)
    component_rates.drop('REAL_COST_x', inplace= True, axis = 1)
    component_rates.drop('ACC_FACTOR_x', inplace= True, axis = 1)
    component_rates["EW4_USER_RATE_STRATEGIC"] = component_rates.apply(lambda x: x['STRATEGIC_COST_USER']* x['MCF_USER'] * x['ACC_FACTOR_USER'], axis = 1)
    component_rates["EW4_USER_RATE_REAL"] = component_rates.apply(lambda x: x['REAL_COST_USER']* x['MCF_USER'] * x['ACC_FACTOR_USER'], axis = 1)
    component_rates["REF_MONTH"] = component_rates.apply(lambda x: get_ref_month(x['USER_CVRG_MONTHS']), axis = 1)
    
    component_rates_part = component_rates.loc[component_rates["CVRG_MONTHS"] == component_rates["REF_MONTH"]]
    component_rates_part["EW4_RATIO_STRATEGIC"] = component_rates_part.apply(lambda x : x['EW4_USER_RATE_STRATEGIC']/ x['EW4_REF_RATE_STRATEGIC'], axis = 1)
    component_rates_part["EW4_RATIO_REAL"] = component_rates_part.apply(lambda x : x['EW4_USER_RATE_REAL']/ x['EW4_REF_RATE_REAL'], axis = 1)
    component_rates_part = component_rates_part.merge(map1_EW4[[ 'COST_IN_SRVC_YEAR', 'TRK_CLASS', 'NET_COST_FACTOR_CALC']],how = 'left', left_on = ['EMISSION_SRVC_YEAR', 'PRODUCT_TYP'], right_on= ['COST_IN_SRVC_YEAR', 'TRK_CLASS'])
    component_rates_part.drop('COST_IN_SRVC_YEAR', inplace = True, axis =1)
    component_rates_part.drop_duplicates(inplace= True)
    component_rates_part = component_rates_part.loc[:, ~component_rates_part.columns.duplicated()]
    component_rates_part["STRATEGIC_RATE"] = component_rates_part.apply(lambda x : x['STRATEGIC_FAILURE_RATE']*x['EW_CPC_STRATEGIC']*x['RATIO']*x['EW4_RATIO_STRATEGIC']*x['NET_COST_FACTOR_CALC'], axis = 1)
    component_rates_part["REAL_RATE"] = component_rates_part.apply(lambda x : x['REAL_FAILURE_RATE']*x['EW_CPC_REAL']*x['RATIO']*x['EW4_RATIO_REAL']*x['NET_COST_FACTOR_CALC'], axis = 1)
#    component_rates_part.to_excel("component_rates_part_6088.xlsx")
    return component_rates_part

def get_ref_month(USER_CVRG_MONTHS):
    REF_MONTH = int()
    if USER_CVRG_MONTHS <= 36:
        REF_MONTH = 36
    elif USER_CVRG_MONTHS <= 48 and USER_CVRG_MONTHS >=37:
        REF_MONTH = 48
    elif USER_CVRG_MONTHS <= 60 and USER_CVRG_MONTHS >=49:
        REF_MONTH = 60
    else:
        REF_MONTH = 72
    return REF_MONTH    

def get_coverage_data():    
    waspp_sql = """
    SELECT DISTINCT "VIN", "PRODUCT_CLASS","EMISSION_YR", "COVERAGE_PACKAGE", "CVRG_MONTHS", "MILES", "TORQUE", "VOC_CATEGORY", "COVERAGE_DEDUCTION_AMT",
    "COVERAGE_CD", "COVERAGE_NAME", "IN_SERVICE_YEAR", "CURR_RATE",  SUM("REM_COST_CCC") AS "REM_COST_CCC_SUM"
    FROM "_SYS_BIC"."DTNA_PRD.Aftermarket.ASP.Apps.WASPP.Views/CV_WASPP_ENG_ONEROUS"('PLACEHOLDER' = ('$$parPortfolio_Curr_Period$$', '202102'), 
    'PLACEHOLDER' = ('$$parProposed_ForecastID$$', 'ENGINE_2020_Q3_FINAL_20200901_TO_20210228'), 
    'PLACEHOLDER' = ('$$parActive_ForecastID$$', 'ENGINE_2020_Q1_FINAL_20200301_TO_20200831'), 
    'PLACEHOLDER' = ('$$parOC_Profit_Flag$$', 'ALL'), 'PLACEHOLDER' = ('$$parPortfolio_Prior_Period$$', '202102'))
    WHERE ("COVERAGE_STATUS" = 'REGISTERED')
    GROUP BY "VIN", "PRODUCT_CLASS","EMISSION_YR", "COVERAGE_PACKAGE", "CVRG_MONTHS", "MILES","TORQUE", "VOC_CATEGORY", "COVERAGE_DEDUCTION_AMT", "COVERAGE_CD", "COVERAGE_NAME", 
    "IN_SERVICE_YEAR", "CURR_RATE"
    """
    start_time = time.time()
    waspp_raw_data = pd.read_sql(waspp_sql, had)
    end_time = (time.time()-start_time)/60
    print("waspp data loaded in  ", end_time ) 
    
    # =============================================================================
    # get VMRS list from WASPP data
    # =============================================================================
    cvrg_code_list = list(waspp_raw_data['COVERAGE_CD'].unique())
    cvrg_code_list =  ', '.join("'{0}'".format(w) for w in cvrg_code_list) # To get a single string with all VMRS comma separated
    cvrg_code_list = ','.join('{}'.format(word) for word in cvrg_code_list.split(','))
    
    vmrs_sql ="""
    SELECT DISTINCT "VMRS_33", "VMRS_33_NAME", "CVRG_CD", "IS_COVERED"
    FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CVRG_VMRS" 
    WHERE ("PRD_TYP_CD" = 'ENGINE' AND "CVRG_CD" IN ("""+ cvrg_code_list +"""))
    """
    start_time = time.time()
    vmrs_raw_data = pd.read_sql(vmrs_sql, hap)
    end_time = (time.time()-start_time)/60
    print("vmrs_raw_data data loaded in  ", end_time )      

    # =============================================================================
    # get contracts from CV_CVRG_EXT_VIN
    # =============================================================================
    coverage_sql = """
    SELECT DISTINCT "VIN","ENG_BASE_MDL_CD","EMISSION_SUB_GRP","IN_SERVICE_DT", "COVERAGE_MONTHS","COVERAGE_DISTANCE1", 
    "COVERAGE_PACKAGE", "COVERAGE_BEGIN_DT", "COVERAGE_END_DT","VOC_CATEGORY", "ENG_TORQUE_GRP", "COVERAGE_DEDUCTION_AMT"
    FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CVRG_EXT_VIN" 
    WHERE ("EXT_STATUS" = 'REGISTERED' AND "PROD_TYPE_CD" = 'ENGINE' AND "IN_SERVICE_DT" >= '2014-01-01' AND "COVERAGE_PACKAGE_TYPE" = 'ENGINE')
    """
    start_time = time.time()
    coverage_raw_data = pd.read_sql(coverage_sql, hap)
    end_time = (time.time()-start_time)/60
    print("coverage_raw_data data loaded in  ", end_time )
    return(waspp_raw_data, vmrs_raw_data, coverage_raw_data)

def get_actuals(repair_date_cutoff, paid_date_cutoff):
    actuals_raw_data_sql ="""SELECT DISTINCT "VIN", "VMRS_33","IN_SERVICE_DT", SUM("TOTAL_WTY_COST") AS "TOTAL_WTY_COST_SUM"
    FROM "_SYS_BIC"."DTNA_PRD.Wty.Views/CV_CLAIMS_DEALER_DDC"('PLACEHOLDER' = ('$$parPaid_YearFrom$$', '2014'), 'PLACEHOLDER' = ('$$parPaid_YearTo$$', '2021')) 
    WHERE ("CLAIM_TYPE_CD" = 'EXTENDED' AND "EXTENDED_REPLACEMENT_INDC" = 'N' AND "CLAIM_PAID_DT" <= '"""+paid_date_cutoff+"""' AND "REPAIR_DT" <= '"""+repair_date_cutoff+"""')
    GROUP BY "VIN", "VMRS_33","IN_SERVICE_DT"
    """
    start_time = time.time()
    actuals_raw_data= pd.read_sql(actuals_raw_data_sql, hap)
    end_time = (time.time()-start_time)/60
    print("Actuals data loaded in  ", end_time )
    return actuals_raw_data

def get_MIS(report_date, in_service_date):
    r = relativedelta.relativedelta(report_date, in_service_date)
    mis = r.months + (12*r.years)
    return mis

def get_unit_maturity(cvrg_months, MIS):
    maturity_indicator = 0
    if (cvrg_months <48 and MIS >=30) or (cvrg_months >= 48 and MIS >=36):
        maturity_indicator = 1
    return maturity_indicator 

#To keep coverages that macth the criteria in Mapping 1 for Engine, Torque, Vocational category, emission, Deductible   
    
def get_emission_grp(emission):
    emission_grp = 'None'
    if "GHG17" in emission:
        emission_grp = "GHG17"
    elif "GHG14" in emission:
        emission_grp = "GHG14"  
    elif "EPA10" in emission:
        emission_grp = "EPA10" 
    elif "OTHER_EPA" in emission:
        emission_grp = "OTHER_EPA"     
    elif "EPA98" in emission:
        emission_grp = "EPA98" 
    elif "EPA07" in emission:
        emission_grp = "EPA07"
    else:
        emission_grp = None
    return emission_grp   

def modify_voc_category(category):
    new_voc_category = "ON HIGHWAY"
    if category != "STANDARD":
        new_voc_category = "VOCATIONAL"
    return new_voc_category    
        

def pair_columns(cvrg_data, engine, torque, voc_category, emission, deductible):
   return cvrg_data[engine] + cvrg_data[torque] +cvrg_data[voc_category] + cvrg_data[emission] +cvrg_data[deductible]

def paired_mask(cvrg_data, map1_data, engine, torque, voc_category, emission, deductible, m_engine, m_torque, m_voc_category, m_emission, m_deductible):
   return pair_columns(cvrg_data, engine, torque, voc_category, emission, deductible).isin(pair_columns(map1_data, m_engine, m_torque, m_voc_category, m_emission, m_deductible))
    
    



