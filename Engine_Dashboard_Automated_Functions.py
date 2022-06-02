# -*- coding: utf-8 -*-
"""
Created on Tue Jul 27 10:42:45 2021

@author: SHANBHS
"""

import pandas as pd
from datetime import datetime
from calendar import monthrange
from dateutil.relativedelta import relativedelta


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month- d2.month

# get last date of each month (e.g: 08/31, 09/30, 10/31 etc)
def last_day_of_month(no_of_months):
    my_list = list()
    for x in no_of_months:
       date_value = datetime.today().date() + relativedelta(months=-x)
       my_list.append(date_value.replace(day = monthrange(date_value.year, date_value.month)[1]))
    return my_list

#get first day of the month
def first_day_of_month(no_of_months):
    my_list = list()
    for x in no_of_months:
       date_value = datetime.today().date() + relativedelta(months=-x)
       my_list.append(date_value.replace(day = 1))
    return my_list

def check_for_active_unit(rr_begin_date, rr_end_date, given_date):
    if rr_begin_date < given_date and rr_end_date > given_date:
        return 1
    else:
        return 0
    
def get_total(first_date,last_date, claim_paid_date, wty_dollars):
    if claim_paid_date >= first_date and claim_paid_date <= last_date:
        return wty_dollars
    else:
        return 0
    
def get_claims(first_date,last_date, claim_paid_date):
    if claim_paid_date >= first_date and claim_paid_date <= last_date:
        return 1
    else:
        return 0    

#get number of active units for each month
def get_active_units_cj(last_dates, active_data):
    dates = pd.DataFrame(last_dates)
    dates.rename(columns={0: 'Claim_paid_date'}, inplace=True)
    dates['key']=1
    active_data['key'] = 1
    result = pd.merge(dates, active_data, on ='key').drop("key", 1)
    result['Active_Units'] = result.apply(lambda x: check_for_active_unit(x['REV_REC_BEGN_DT'],x['REV_REC_END_DT'], x['Claim_paid_date']), axis=1)
    result.drop("REV_REC_BEGN_DT", inplace= True, axis = 1)
    result.drop("REV_REC_END_DT", inplace= True, axis = 1)
    result.drop("COVERAGE_PACKAGE", inplace= True, axis = 1)
    result = result.drop_duplicates()
    active_units = result.groupby(["Claim_paid_date"]).Active_Units.sum().reset_index().rename_axis(None, axis=1)
    return active_units

#get warranty cost fro each month
def get_wty_cost_cj(my_dates, claims, groupby_param):
    dates = pd.DataFrame(my_dates)    
    dates['key']=1
    claims['key'] = 1
    result_cl = pd.merge(dates, claims, on ='key').drop("key", 1)
    result_cl['Total_Cost'] = result_cl.apply(lambda x: get_total(x['first_dates'],x['last_dates'], x['CLAIM_PAID_DT'], x['TOTAL_WTY_COST_SUM']), axis=1)
    Total_claim_dollars = result_cl.groupby(groupby_param).Total_Cost.sum().reset_index().rename_axis(None, axis=1)
    Total_claim_dollars.rename(columns={'last_dates': 'Claim_paid_date'}, inplace=True)
    return Total_claim_dollars

def get_wty_claims(my_dates, claims, groupby_param):
    dates = pd.DataFrame(my_dates)    
    dates['key']=1
    claims['key'] = 1
    result_cl = pd.merge(dates, claims, on ='key').drop("key", 1)
    result_cl['Total_Claims'] = result_cl.apply(lambda x: get_claims(x['first_dates'],x['last_dates'], x['CLAIM_PAID_DT']), axis=1)
    Total_claims = result_cl.groupby(groupby_param).Total_Claims.sum().reset_index().rename_axis(None, axis=1)
    Total_claims.rename(columns={'last_dates': 'Claim_paid_date'}, inplace=True)
    return Total_claims


def check_for_active_customer(rr_begin_date, rr_end_date, ed_date, st_date):
    if rr_begin_date < ed_date and rr_end_date > st_date:
        return 1
    else:
        return 0

def get_customer_active_units(ed_date, st_date, active_data):
    active_data['my_ed_date'] = ed_date
    active_data['my_st_date'] = st_date
    result  = active_data
    result['Is_active'] = result.apply(lambda x: check_for_active_customer(x['REV_REC_BEGN_DT'],x['REV_REC_END_DT'], x['my_ed_date'], x['my_st_date']), axis=1)
    active_units = result.groupby(["VIN", 'CURR_CUST_NAME']).Is_active.sum().reset_index().rename_axis(None, axis=1)
    return active_units

    
def get_final_files(st_date, ed_date, data_for_active_units, claims,portfolio):
    no_of_months = diff_month(ed_date, st_date) +2
    last_dates= last_day_of_month(list(range(1,no_of_months)))
    first_dates = first_day_of_month(list(range(1,no_of_months)))  
    Claim_paid_date = pd.DataFrame({'first_dates':first_dates,'last_dates':last_dates})
    total_wty_cost = get_wty_cost_cj(Claim_paid_date, claims,['last_dates','ENG_BASE_MDL_CD'])
    total_wty_claims = get_wty_claims(Claim_paid_date, claims,['last_dates','ENG_BASE_MDL_CD'])
    total_wty = pd.merge(total_wty_cost, total_wty_claims, on= ['Claim_paid_date','ENG_BASE_MDL_CD'])
    cross_join_rs = get_active_units_cj(last_dates, data_for_active_units) #get active units
    cross_join_rs_claims = get_wty_cost_cj(Claim_paid_date, claims,['last_dates']) 
    claim_count = get_wty_claims(Claim_paid_date, claims, ['last_dates'])  # get claim count

    cpc_cpu_df = pd.DataFrame()
    cpc_cpu_df = pd.merge(cross_join_rs, cross_join_rs_claims) # combine them into one dataframe.
    cpc_cpu_df = pd.merge(cpc_cpu_df, claim_count)
#    final_df.to_excel("Engine Rolling_13_month " +str(datetime.today().date()) + ".xlsx") 
    if( (portfolio == "GHG17_engine" )| (portfolio == "All_engine")):
        last_three_months = last_dates[:3] 
        first_dates_three_months = first_dates[:3]
    else:
        last_three_months = last_dates
        first_dates_three_months = first_dates
    last_three_month_dates = pd.DataFrame({'first_dates': first_dates_three_months, 'last_dates': last_three_months})
    customer_info = get_wty_cost_cj(last_three_month_dates, claims, ['CURR_CUST_NAME'])
    customer_claim_count = get_wty_claims(last_three_month_dates, claims, ['CURR_CUST_NAME'])  # get claim count
    customer_info_active = get_customer_active_units(max(last_three_months), min(first_dates_three_months), data_for_active_units)
    customer_info_active = customer_info_active.loc[customer_info_active['Is_active'] ==1]
    customer_active = customer_info_active.groupby(['CURR_CUST_NAME']).Is_active.sum().reset_index().rename_axis(None, axis=1)
    top_customers = pd.merge(customer_info, customer_active, on ="CURR_CUST_NAME")
    top_customers = pd.merge(top_customers, customer_claim_count, on ="CURR_CUST_NAME")
    top_customers['Cost_per_unit_Cust'] = top_customers.apply(lambda x: x['Total_Cost']/ x['Is_active'] if x['Is_active']> 0 else 0, axis = 1)
    top_customers['Cost_per_claim_Cust'] = top_customers.apply(lambda x: x['Total_Cost']/ x['Total_Claims'] if x['Total_Claims']> 0 else 0, axis = 1)
    top15_customers = top_customers.loc[top_customers['Is_active']>=500]
    top15_customers = top15_customers.sort_values(by =['Total_Cost'],ascending  = False).head(20)
    dealer_info = get_wty_cost_cj(last_three_month_dates, claims, ['DEALER_NAME'])
    dealer_claim_count = get_wty_claims(last_three_month_dates, claims, ['DEALER_NAME'])
    top_dealers = pd.merge(dealer_info, dealer_claim_count, on ="DEALER_NAME")
    top_dealers['Cost_per_claim_dealer'] = top_dealers.apply(lambda x: x['Total_Cost']/ x['Total_Claims'] if x['Total_Claims']> 0 else 0, axis = 1)
    top_dealers = pd.merge(top_dealers, claims.loc[:,["DEALER_TYPE", "DEALER_NAME"]], how= 'left', left_on = 'DEALER_NAME', right_on = 'DEALER_NAME')
    top_dealers = top_dealers.drop_duplicates()
    if( (portfolio == "GHG17_engine") | (portfolio == "All_engine")):
        prior_last_three_months = last_dates[3:6]
        prior_first_three_months = first_dates[3:6]
        prior_three_month_dates = pd.DataFrame({'first_dates': prior_first_three_months, 'last_dates': prior_last_three_months})
        seag_info_prior = get_wty_cost_cj(prior_three_month_dates, claims, ['FAILED_PART_SEAG_DESC'])
        seag_info_prior.rename(columns={'Total_Cost': 'Total_Cost_Prior'}, inplace=True)
        seag_info_latest = get_wty_cost_cj(last_three_month_dates, claims, ['FAILED_PART_SEAG_DESC'])
        seag_info_latest.rename(columns={'Total_Cost': 'Total_Cost_Latest'}, inplace=True)    
        seag_info = pd.merge(seag_info_latest, seag_info_prior, on ='FAILED_PART_SEAG_DESC')
        seag_info["Percent_diff"] = seag_info.apply(lambda x:  (x['Total_Cost_Latest']- x['Total_Cost_Prior'])/x['Total_Cost_Prior'] if x['Total_Cost_Prior'] >0 else 0, axis = 1)
    else:
        seag_info= pd.DataFrame()
    return (total_wty,cpc_cpu_df, top_customers,top_dealers, seag_info )
    
#    top15_customers.to_excel("Engine Customers.xlsx")
    
def rename_columns(data_info, df):
    if(data_info =='total_wty'):
        df2 = df.rename(columns={'Claim_paid_date': 'Claim_paid_date_13mnth','Total_Cost':'Total_Cost_13mnth','Total_Claims':'Total_Claims_13mnth'})
        return df2
    elif(data_info== 'cpc_cpu'):
        df2 = df.rename(columns={'Claim_paid_date': 'Claim_paid_date_cpc_cpu','Total_Cost':'Total_Cost_cpc_cpu','Total_Claims':'Total_Claims_cpc_cpu'})
        return df2
    elif(data_info == 'top_customers'):        
        df2 = df.rename(columns={'Total_Cost':'Total_Cost_cust','Total_Claims':'Total_Claims_cust'}) 
        return df2
    elif(data_info== 'top_dealers'):
        df2= df.rename(columns={'Total_Cost':'Total_Cost_dlr','Total_Claims':'Total_Claims_dlr'})
        return df2
    else:        
        return df
    
    
def write_to_excel(worksheet_name, total_wty,cpc_cpu,top_customers,top_dealers,seag_info, writer):
    workbook=writer.book
    worksheet=workbook.add_worksheet(worksheet_name)
    writer.sheets[worksheet_name] = worksheet
    total_wty.to_excel(writer,sheet_name=worksheet_name,startrow=0 , startcol=0)   
    cpc_cpu.to_excel(writer,sheet_name=worksheet_name,startrow=0, startcol=len(total_wty.columns)+1) 
    top_customers.to_excel(writer,sheet_name=worksheet_name,startrow=0, startcol=len(total_wty.columns)+len(cpc_cpu.columns) +2)   
    top_dealers.to_excel(writer,sheet_name=worksheet_name,startrow=0, startcol=len(total_wty.columns)+len(cpc_cpu.columns)+ len(top_customers.columns)+ 3)   
    seag_info.to_excel(writer,sheet_name=worksheet_name,startrow=0, startcol=len(total_wty.columns)+len(cpc_cpu.columns)+ len(top_customers.columns)+len(top_dealers.columns)+4)       
    writer.save()    
    

def get_secondary_data(claims):
    claims['BLD_YR'] = pd.DatetimeIndex(claims['VEH_BUILD_DT']).year
    paid_amt_bld_yr = claims.groupby(['BLD_YR']).agg({'TOTAL_WTY_COST_SUM':'sum'}).reset_index().rename_axis(None, axis=1)
    total_claims_fail_miles_isy = claims.groupby(['VIN_ODOMETER', 'ISY']).agg({'CLAIM_CD':'nunique'}).reset_index().rename_axis(None, axis=1)
    claims['MIS'] =  claims.apply(lambda x: diff_month(x['FAILURE_DT'], x['IN_SERVICE_DT']), axis =1)
    paid_amt_mis = claims.groupby(['MIS']).agg({'TOTAL_WTY_COST_SUM':'sum'}).reset_index().rename_axis(None, axis=1)
    return (paid_amt_bld_yr, total_claims_fail_miles_isy ,paid_amt_mis)

