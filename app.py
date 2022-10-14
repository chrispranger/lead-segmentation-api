from flask import Flask, jsonify, request, render_template
import requests
import json
import urllib.request
import os
import ssl

ATP_impute = 417.0
Affluence_Index_impute = 188.0
EconomicCohortsCodeNumeric_impute = 29.0
Total_Income360_impute = 67589.0
Vantage_Score_Neighborhood_Risk_Score_impute = 692.0
total_liquid_assets_impute = 30990.0
netWorthGoldMin__c_impute = 100000.0
ESI_impute = 27.0
distance_to_center__c_impute = 22.0
created_hour_impute = 11.0
created_dow_impute = 2.0

EconomicCohortsCode_impute = 'F29'
first_campaign_type_impute = 'Other'
CreatedBy_Channel_impute = 'Website'
has_email_address_impute = 'N'
dental_condition_impute = 'No Response or Other'

completed_percentile_thresholds = [
    0.5618455689698492,
    0.45255955707037726,
    0.3791328809375031,
    0.3247136538868236,
    0.2801799989765551,
    0.2459017891993004,
    0.21512971380788146,
    0.19336098729658877,
    0.17387252726703237,
    0.1570808028296809,
    0.14138929439895515,
    0.12806552689227524,
    0.11534446520824325,
    0.10264464822503738,
    0.08995962328456503,
    0.07632904550224781,
    0.06164845917462097,
    0.04801035035200325,
    0.03340477708547122
    ]

collections_percentile_thresholds = [
    4691.003565296626,
    3013.658181150213,
    2229.8728434273876,
    1734.365564581659,
    1370.3905514934868,
    1115.0334416902908,
    929.5500486300754,
    768.8388440932843,
    641.3777338719883,
    531.7923993276363,
    442.6984110249428,
    365.0432264351628,
    297.5606573988183,
    236.9258610252626,
    178.94690428273873,
    122.65745374142925,
    64.34480976599149,
    3.0353671917654172,
    -77.5399394722977
 ]

'''### Old (With MatchFlags)
completed_percentile_thresholds = [
    0.5618980927990199,
    0.4527505348275388,
    0.37891228413281214,
    0.3248216064974017,
    0.27924630686135254,
    0.24597724248208946,
    0.21769077030458153,
    0.19412501465626977,
    0.1741130489431159,
    0.1566471968369824,
    0.14150574751285316,
    0.1269253792446465,
    0.11472776920386848,
    0.10168405407128796,
    0.0890719695622815,
    0.07623494364367314,
    0.06248650273880042,
    0.047760495939900456,
    0.03282364533706566
    ]

collections_percentile_thresholds = [
    4796.4080334680175,
    3049.8876793532327,
    2199.695964743468,
    1694.607888962551,
    1339.2458022433418,
    1090.4785262434295,
    900.3089418466466,
    743.4943691526707,
    626.6713456985647,
    520.3895610932104,
    446.7357183964067,
    370.82143395733414,
    309.1158224555455,
    250.0911140896453,
    193.7109401780866,
    135.7646882195283,
    78.30620055668582,
    18.365324039178862,
    -54.64081999794801
    ]
'''

# Imputing Missing Values
def impute_missing_values(posted_data):
    
    id = posted_data['data']['id']

    # Float Fields
    try:
        ATP = posted_data['data']['ATP']
        ATP = float(ATP)
    except:
        ATP = ATP_impute
    try:
        Affluence_Index = posted_data['data']['AffluenceIndex']
        Affluence_Index = float(Affluence_Index)
    except:
        Affluence_Index = Affluence_Index_impute
    try:
        # Get numeric from code
        EconomicCohortsCodeNumeric = posted_data['data']['EconomicCohorts']
        emp_str = ""
        for m in EconomicCohortsCodeNumeric:
            if m.isdigit():
                emp_str = emp_str + m
        EconomicCohortsCodeNumeric = float(emp_str)
    except:
        EconomicCohortsCodeNumeric = EconomicCohortsCodeNumeric_impute

    try:
        Total_Income360 = posted_data['data']['Income360']
        Total_Income360 = float(Total_Income360)
    except:
        Total_Income360 = Total_Income360_impute
    try:
        Vantage_Score_Neighborhood_Risk_Score = posted_data['data']['Vantage Score Neighborhood Risk Score']
        Vantage_Score_Neighborhood_Risk_Score = float(Vantage_Score_Neighborhood_Risk_Score)
    except:
        Vantage_Score_Neighborhood_Risk_Score = Vantage_Score_Neighborhood_Risk_Score_impute
    try:
        total_liquid_assets = posted_data['data']['Total_Liquid_Assets__c']
        total_liquid_assets = float(total_liquid_assets)
    except:
        total_liquid_assets = total_liquid_assets_impute
    try:
        netWorthGoldMin__c = posted_data['data']['netWorthGoldMin__c']
        netWorthGoldMin__c = float(netWorthGoldMin__c)
    except:
        netWorthGoldMin__c = netWorthGoldMin__c_impute
    try:
        ESI = posted_data['data']['Economic_Stability_Indicator__c']
        ESI = float(ESI)
    except:
        ESI = ESI_impute
    try:
        distance_to_center__c = posted_data['data']['distance_to_center__c']
        distance_to_center__c = float(distance_to_center__c)
    except:
        distance_to_center__c = distance_to_center__c_impute
    try:
        created_hour = posted_data['data']['Created_Hour__c']
        created_hour = float(created_hour)
    except:
        created_hour = created_hour_impute
    try:
        created_dow = posted_data['data']['Created_Day_Of_Week__c']
        created_dow = float(created_dow)
    except:
        created_dow = created_dow_impute

    # String Data
    try:
        EconomicCohortsCode = posted_data['data']['EconomicCohorts']
        if EconomicCohortsCode == None:
            EconomicCohortsCode = EconomicCohortsCode_impute
        else:
            EconomicCohortsCode = str(EconomicCohortsCode)
    except:
        EconomicCohortsCode = EconomicCohortsCode_impute

    try:
        first_campaign_type = posted_data['data']['First_Campaign_Type__c']
        if first_campaign_type == None:
            first_campaign_type = first_campaign_type_impute
        else:
            first_campaign_type = str(first_campaign_type)
    except:
        first_campaign_type = first_campaign_type_impute

    try:
        CreatedBy_Channel = posted_data['data']['Created_By_Channel__c']
        if CreatedBy_Channel == None:
            CreatedBy_Channel = CreatedBy_Channel_impute
        else:
            CreatedBy_Channel = str(CreatedBy_Channel)
    except:
        CreatedBy_Channel = CreatedBy_Channel_impute

    try:
        has_email_address = posted_data['data']['Has_Email_Address__c']
        if has_email_address == None:
            has_email_address = has_email_address_impute
        else:
            has_email_address = str(has_email_address)
    except:
        has_email_address = has_email_address_impute

    try:
        dental_condition = posted_data['data']['Dental_Condition__c']
        if dental_condition == None:
            dental_condition = dental_condition_impute
        else:
            dental_condition = str(dental_condition)
    except:
        dental_condition = dental_condition_impute

    payload = {
                "id": id,
                "ATP": ATP,
                "AffluenceIndex": Affluence_Index,
                "EconomicCohortsCodeNumeric": EconomicCohortsCodeNumeric,
                "EconomicCohortsCode": EconomicCohortsCode,
                "Income360": Total_Income360,
                "Vantage Score Neighborhood Risk Score": Vantage_Score_Neighborhood_Risk_Score,
                "total_liquid_assets": total_liquid_assets,
                "netWorthGoldMin__c": netWorthGoldMin__c,
                "ESI": ESI,
                "distance_to_center__c": distance_to_center__c,
                "first_campaign_type": first_campaign_type,
                "CreatedBy_Channel": CreatedBy_Channel,
                "has_email_address": has_email_address,
                "dental_condition": dental_condition,
                "created_hour": created_hour,
                "created_dow": created_dow
            }

    return payload


def pre_process_fields(payload):

    ec_lookup_dict = [{'Income': 'Lower',
                            'Age Group': 'Young',
                            'Cluster': 'A01',
                            'Cat1': ' Tough Start',
                            'Cat2': ' Young Single Parents'},
                            {'Income': 'Lower',
                            'Age Group': 'Young',
                            'Cluster': 'A02',
                            'Cat1': ' Tough Start',
                            'Cat2': ' Young Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Young',
                            'Cluster': 'A03',
                            'Cat1': ' Starting Small',
                            'Cat2': ' Small-Town Families'},
                            {'Income': 'Lower',
                            'Age Group': 'Young',
                            'Cluster': 'A04',
                            'Cat1': ' Starting Small',
                            'Cat2': ' Small-Town Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Young',
                            'Cluster': 'A05',
                            'Cat1': ' Living on Loans',
                            'Cat2': ' Young Urban Single Parents'},
                            {'Income': 'Lower',
                            'Age Group': 'Young',
                            'Cluster': 'A06',
                            'Cat1': ' Living on Loans',
                            'Cat2': ' Young Urban Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Working Years',
                            'Cluster': 'B07',
                            'Cat1': ' Mid-Life Strugglers',
                            'Cat2': ' Families'},
                            {'Income': 'Lower',
                            'Age Group': 'Working Years',
                            'Cluster': 'B08',
                            'Cat1': ' Mid-Life Strugglers',
                            'Cat2': ' Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Working Years',
                            'Cluster': 'B09',
                            'Cat1': ' Getting By',
                            'Cat2': ' Small-Town Families'},
                            {'Income': 'Lower',
                            'Age Group': 'Working Years',
                            'Cluster': 'B10',
                            'Cat1': ' Getting By',
                            'Cat2': ' Small-Town Singles and Couples'},
                            {'Income': 'Lower',
                            'Age Group': 'Working Years',
                            'Cluster': 'B11',
                            'Cat1': ' Credit Crunched',
                            'Cat2': ' City Families'},
                            {'Income': 'Lower',
                            'Age Group': 'Working Years',
                            'Cluster': 'B12',
                            'Cat1': ' Credit Crunched',
                            'Cat2': ' City Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'C13',
                            'Cat1': ' Retiring on Empty',
                            'Cat2': ' Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'C14',
                            'Cat1': ' Burdened by Debt',
                            'Cat2': '  Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'C15',
                            'Cat1': ' Sensible Spenders',
                            'Cat2': ' Families'},
                            {'Income': 'Lower',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'C16',
                            'Cat1': ' Sensible Spenders',
                            'Cat2': ' Small-Town Empty Nesters'},
                            {'Income': 'Lower',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'C17',
                            'Cat1': ' Sensible Spenders',
                            'Cat2': ' Urban Pre-Retirement Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Retired',
                            'Cluster': 'D18',
                            'Cat1': ' Relying On Aid',
                            'Cat2': '  Retired Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Retired',
                            'Cluster': 'D19',
                            'Cat1': ' Rough Retirement',
                            'Cat2': ' Small-Town and Rural Seniors'},
                            {'Income': 'Lower',
                            'Age Group': 'Retired',
                            'Cluster': 'D20',
                            'Cat1': ' Struggling Elders',
                            'Cat2': ' Singles'},
                            {'Income': 'Lower',
                            'Age Group': 'Retired',
                            'Cluster': 'D21',
                            'Cat1': ' Modest Means',
                            'Cat2': '  Urban Retirees'},
                            {'Income': 'Moderate',
                            'Age Group': 'Young',
                            'Cluster': 'E22',
                            'Cat1': ' Credit City',
                            'Cat2': ' Young Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Young',
                            'Cluster': 'E23',
                            'Cat1': ' Credit City',
                            'Cat2': ' Young Singles'},
                            {'Income': 'Moderate',
                            'Age Group': 'Young',
                            'Cluster': 'E24',
                            'Cat1': ' Midscale Mainstream',
                            'Cat2': ' Small-Town Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Young',
                            'Cluster': 'E25',
                            'Cat1': ' Midscale Mainstream',
                            'Cat2': ' Small-Town Singles and Couples'},
                            {'Income': 'Moderate',
                            'Age Group': 'Young',
                            'Cluster': 'E26',
                            'Cat1': ' Getting Ahead',
                            'Cat2': ' Young City Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Young',
                            'Cluster': 'E27',
                            'Cat1': ' Getting Ahead',
                            'Cat2': ' Young City Singles'},
                            {'Income': 'Moderate',
                            'Age Group': 'Working Years',
                            'Cluster': 'F28',
                            'Cat1': ' Living Simply',
                            'Cat2': ' Small-Town Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Working Years',
                            'Cluster': 'F29',
                            'Cat1': ' Living Simply',
                            'Cat2': ' Small-Town Singles and Couples'},
                            {'Income': 'Moderate',
                            'Age Group': 'Working Years',
                            'Cluster': 'F30',
                            'Cat1': ' Credit Rules',
                            'Cat2': ' Urban Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Working Years',
                            'Cluster': 'F31',
                            'Cat1': ' Credit Rules',
                            'Cat2': ' Urban Singles '},
                            {'Income': 'Moderate',
                            'Age Group': 'Working Years',
                            'Cluster': 'F32',
                            'Cat1': ' Suburban Stability',
                            'Cat2': ' Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Working Years',
                            'Cluster': 'F33',
                            'Cat1': ' Suburban Stability',
                            'Cat2': ' Singles and Couples'},
                            {'Income': 'Moderate',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'G34',
                            'Cat1': ' Committed to Credit',
                            'Cat2': '  Small-Town Couples'},
                            {'Income': 'Moderate',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'G35',
                            'Cat1': ' Striving for Balance',
                            'Cat2': '  Urban Pre-Retirement Singles'},
                            {'Income': 'Moderate',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'G36',
                            'Cat1': ' Conservative Consumers',
                            'Cat2': '  Small-Town Empty Nesters'},
                            {'Income': 'Moderate',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'G37',
                            'Cat1': ' Conservative Consumers',
                            'Cat2': '  Suburban Families'},
                            {'Income': 'Moderate',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'G38',
                            'Cat1': ' Solid Foundation',
                            'Cat2': '  Suburban Empty Nesters'},
                            {'Income': 'Moderate',
                            'Age Group': 'Retired',
                            'Cluster': 'H39',
                            'Cat1': ' Retired on Credit',
                            'Cat2': '  City Singles and Couples'},
                            {'Income': 'Moderate',
                            'Age Group': 'Retired',
                            'Cluster': 'H40',
                            'Cat1': ' Safety Net Seniors',
                            'Cat2': ' Small-Town Retired Couples'},
                            {'Income': 'Moderate',
                            'Age Group': 'Retired',
                            'Cluster': 'H41',
                            'Cat1': ' Nest Egg Elders',
                            'Cat2': ' Older Retirees'},
                            {'Income': 'Moderate',
                            'Age Group': 'Retired',
                            'Cluster': 'H42',
                            'Cat1': ' Comfortable Retirement',
                            'Cat2': ' Suburban Couples'},
                            {'Income': 'High',
                            'Age Group': 'Young',
                            'Cluster': 'I43',
                            'Cat1': ' Charge-It Champs',
                            'Cat2': ' Young Suburban Families'},
                            {'Income': 'High',
                            'Age Group': 'Young',
                            'Cluster': 'I44',
                            'Cat1': ' Charge-It Champs',
                            'Cat2': ' Young Suburban Singles'},
                            {'Income': 'High',
                            'Age Group': 'Young',
                            'Cluster': 'I45',
                            'Cat1': ' Confident Futures',
                            'Cat2': ' Young City Families'},
                            {'Income': 'High',
                            'Age Group': 'Young',
                            'Cluster': 'I46',
                            'Cat1': ' Confident Futures',
                            'Cat2': ' Young City Singles and Couples'},
                            {'Income': 'High',
                            'Age Group': 'Young',
                            'Cluster': 'I47',
                            'Cat1': ' Material World',
                            'Cat2': ' Urban Families'},
                            {'Income': 'High',
                            'Age Group': 'Young',
                            'Cluster': 'I48',
                            'Cat1': ' Material World',
                            'Cat2': ' Urban Singles'},
                            {'Income': 'High',
                            'Age Group': 'Working Years',
                            'Cluster': 'J49',
                            'Cat1': ' House of Cards',
                            'Cat2': ' Suburban Families'},
                            {'Income': 'High',
                            'Age Group': 'Working Years',
                            'Cluster': 'J50',
                            'Cat1': ' House of Cards',
                            'Cat2': ' Suburban Singles and Couples'},
                            {'Income': 'High',
                            'Age Group': 'Working Years',
                            'Cluster': 'J51',
                            'Cat1': ' Prudent Professionals',
                            'Cat2': ' Suburban Families'},
                            {'Income': 'High',
                            'Age Group': 'Working Years',
                            'Cluster': 'J52',
                            'Cat1': ' Prudent Professionals',
                            'Cat2': ' Suburban Singles and Couples'},
                            {'Income': 'High',
                            'Age Group': 'Working Years',
                            'Cluster': 'J53',
                            'Cat1': ' Suburban Success',
                            'Cat2': ' Upscale Families'},
                            {'Income': 'High',
                            'Age Group': 'Working Years',
                            'Cluster': 'J54',
                            'Cat1': ' Suburban Success',
                            'Cat2': ' Upscale Singles and Couples'},
                            {'Income': 'High',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'K55',
                            'Cat1': ' Living for Today',
                            'Cat2': ' Couples'},
                            {'Income': 'High',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'K56',
                            'Cat1': ' Planners and Savers',
                            'Cat2': ' Suburban Families'},
                            {'Income': 'High',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'K57',
                            'Cat1': ' Planners and Savers',
                            'Cat2': ' Suburban Couples'},
                            {'Income': 'High',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'K58',
                            'Cat1': ' Planners and Savers',
                            'Cat2': ' City Couples'},
                            {'Income': 'High',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'K59',
                            'Cat1': ' Country Club Climbers',
                            'Cat2': ' Suburban Empty Nesters'},
                            {'Income': 'High',
                            'Age Group': 'Retired',
                            'Cluster': 'L60',
                            'Cat1': ' Comfortable with Credit',
                            'Cat2': ' Upscale Retirees'},
                            {'Income': 'High',
                            'Age Group': 'Retired',
                            'Cluster': 'L61',
                            'Cat1': ' Rewarding Retirement',
                            'Cat2': ' Affluent Suburbanites'},
                            {'Income': 'High',
                            'Age Group': 'Retired',
                            'Cluster': 'L62',
                            'Cat1': ' Affluent Elders',
                            'Cat2': ' Older Upscale Suburbanites'},
                            {'Income': 'High',
                            'Age Group': 'Retired',
                            'Cluster': 'L63',
                            'Cat1': ' Established Wealth',
                            'Cat2': ' Suburban Retirees'},
                            {'Income': 'Elite',
                            'Age Group': 'Young',
                            'Cluster': 'M64',
                            'Cat1': ' Big Shots',
                            'Cat2': ' Young Upmarket Urbanites'},
                            {'Income': 'Elite',
                            'Age Group': 'Working Years',
                            'Cluster': 'N65',
                            'Cat1': ' Careers First',
                            'Cat2': ' Urbanites'},
                            {'Income': 'Elite',
                            'Age Group': 'Working Years',
                            'Cluster': 'N66',
                            'Cat1': ' Executive Spenders',
                            'Cat2': ' Suburban Families'},
                            {'Income': 'Elite',
                            'Age Group': 'Working Years',
                            'Cluster': 'N67',
                            'Cat1': ' Executive Spenders',
                            'Cat2': ' Suburban Couples'},
                            {'Income': 'Elite',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'O68',
                            'Cat1': ' Corner Offices',
                            'Cat2': ' Executive Urbanites'},
                            {'Income': 'Elite',
                            'Age Group': 'Pre-Retirement',
                            'Cluster': 'O69',
                            'Cat1': ' Champagne Tastes',
                            'Cat2': ' Executive Empty Nesters'},
                            {'Income': 'Elite',
                            'Age Group': 'Retired',
                            'Cluster': 'P70',
                            'Cat1': ' Flush Funds',
                            'Cat2': ' Wealthy Urban Seniors'},
                            {'Income': 'Elite',
                            'Age Group': 'Retired',
                            'Cluster': 'P71',
                            'Cat1': ' Diamonds and Pearls',
                            'Cat2': ' Wealthiest Retirees'},
                            {'Income': 'Missing',
                            'Age Group': 'Missing',
                            'Cluster': 'Missing',
                            'Cat1': 'Missing',
                            'Cat2': 'Missing'}]

    # Economic Cohorts Code - Turning the code into the corresponding income group and age group
    accepted_categories = ['A01','A02','A03','A04','A05','A06','B07','B08','B09','B10','B11','B12','C13','C14','C15','C16','C17','D18','D19','D20','D21','E22','E23','E24','E25','E26','E27','F28','F29',
            'F30','F31','F32','F33','G34','G35','G36','G37','G38','H39','H40','H41','H42','I43','I44','I45','I46','I47','I48','J49','J50','J51','J52','J53','J54','K55','K56','K57','K58','K59','L60','L61',
            'L62','L63','M64','N65','N66','N67','O68','O69','P70','P71']

    if payload['EconomicCohortsCode'] not in accepted_categories:
        payload['EconomicCohortsCode'] = 'F29'

    # Income Group
    income_group = next(item for item in ec_lookup_dict if item["Cluster"] == payload['EconomicCohortsCode'])['Income']
    if income_group == "Lower":
        income_group = 0
    elif income_group == "Moderate":
        income_group = 1
    elif income_group == "High":
        income_group = 2
    elif income_group == "Elite":
        income_group = 3
    else:
        income_group = 1

    payload['income_group'] = income_group

    # Age Group
    age_group = next(item for item in ec_lookup_dict if item["Cluster"] == payload['EconomicCohortsCode'])['Age Group']
    if age_group == "Young":
        age_group = 0
    elif age_group == "Working Years":
        age_group = 1
    elif age_group == "Pre-Retirement":
        age_group = 2
    elif age_group == "Retired":
        age_group = 3
    else:
        age_group = 1

    payload['age_group'] = age_group

    # Distance to Center
    if payload['distance_to_center__c'] > 500 or payload['distance_to_center__c'] < 0:
        payload['distance_to_center__c'] = 500.0

    # first_campaign_type: Replace 'misc weblead' with 'web lead', 'web lead 3 (IMS)' with web lead, and nans with other
    other_categories = ['Unknown Type', 'Web AB Test', 'Brochure',
           'Legacy Marketing Number', 'National Long Form', 'Newspaper',
           'Referral', 'Web-Video', 'Call-In Show']

    change_to_web_lead = ['Misc Weblead', 'Web Lead 3 (IMS)']

    accepted_categories = ['Web-SEM', 'National', 'Web Lead', 'Web-Phone', 'TV', 'Long-form TV', 'Web-display', 'Web-Email Program', 'Direct Mail', 'Other']

    # replace first_campaign_type values in lists with either "Other" or "Web Lead", and replace Nans with "Other"
    if payload['first_campaign_type'] in change_to_web_lead:
        payload['first_campaign_type'] = "Web Lead"
    if payload['first_campaign_type'] not in accepted_categories:
            payload['first_campaign_type'] = "Other"

    # Created by Channel
    accepted_categories = ['CCMS', 'Website']
        
    if payload['CreatedBy_Channel'] in accepted_categories:
        payload['CreatedBy_Channel'] = payload['CreatedBy_Channel']
    else:
        payload['CreatedBy_Channel'] = "Website"

    if payload['CreatedBy_Channel'] == "Website":
        payload['CreatedBy_Channel'] = 1
    else:
        payload['CreatedBy_Channel'] = 0

    # Has Email Address
    accepted_categories = ['Y', 'N', True, False, "True", "False"]
    if payload['has_email_address'] in accepted_categories:
            payload['has_email_address'] = payload['has_email_address']
    else:
        payload['has_email_address'] = "N"

    affirm_values = ["Y", True, "True"]
    if payload['has_email_address'] in affirm_values:
        payload['has_email_address'] = 1
    else:
        payload['has_email_address'] = 0

    # Dental Condition
    if type(payload['dental_condition']) == str:
        payload['dental_condition'] = payload['dental_condition'].lower()
    else:
        payload['dental_condition'] = 'No Response or Other'

    if 'denture' in payload['dental_condition']:
            payload['dental_condition'] = 'Denture or Partial Denture'
    elif 'bridge' in payload['dental_condition'] or 'crown' in payload['dental_condition'] or 'implant' in payload['dental_condition'] and 'failing' in payload['dental_condition']:
        payload['dental_condition'] = 'Bridge Crown or Implant Failing'
    elif 'perio' in payload['dental_condition'] or 'disease' in payload['dental_condition']:
        payload['dental_condition'] = 'Period Disease'
    elif 'cracked' in payload['dental_condition'] or 'loose' in payload['dental_condition'] or 'failing' in payload['dental_condition'] and 'teeth' in payload['dental_condition']:
        payload['dental_condition'] = 'Cracked Loose or Failing Teeth'
    elif 'missing' in payload['dental_condition']:
        if 'teeth' in payload['dental_condition']:
            payload['dental_condition'] = 'Missing Teeth'
        elif 'tooth' in payload['dental_condition']:
            payload['dental_condition'] = 'Missing Teeth'
    else:
        payload['dental_condition'] = 'No Response or Other'


    accepted_categories = ['Denture or Partial Denture', 'Bridge Crown or Implant Failing', 'Period Disease', 'Cracked Loose or Failing Teeth', 'Missing Teeth', 'No Response or Other']
    if payload['dental_condition'] in accepted_categories:
        payload['dental_condition'] = payload['dental_condition']
    else:
        payload['dental_condition'] = 'No Response or Other'

    if payload['dental_condition'] == 'No Response or Other':
        payload['dental_condition'] = 0
    elif payload['dental_condition'] == 'Period Disease':
        payload['dental_condition'] = 1
    elif payload['dental_condition'] == 'Denture or Partial Denture':
        payload['dental_condition'] = 2
    elif payload['dental_condition'] == 'Bridge Crown or Implant Failing':
        payload['dental_condition'] = 3
    elif payload['dental_condition'] == 'Cracked Loose or Failing Teeth':
        payload['dental_condition'] = 4
    elif payload['dental_condition'] == 'Missing Teeth':
        payload['dental_condition'] = 5

    return payload


def build_payload_to_send(payload):

    new_payload = {
                "ATP": payload['ATP'],
                "Affluence_Index": payload['AffluenceIndex'],
                "EconomicCohortsCodeNumeric": payload['EconomicCohortsCodeNumeric'],
                "Total_Income360": payload['Income360'],
                "Vantage Score Neighborhood Risk Score": payload['Vantage Score Neighborhood Risk Score'],
                "total_liquid_assets": payload['total_liquid_assets'],
                "netWorthGoldMin__c": payload['netWorthGoldMin__c'],
                "ESI": payload['ESI'],
                "distance_to_center__c": payload['distance_to_center__c'],
                "first_campaign_type": payload['first_campaign_type'],
                "CreatedBy_Channel": payload['CreatedBy_Channel'],
                "has_email_address": payload['has_email_address'],
                "dental_condition": payload['dental_condition'],
                "created_hour": payload['created_hour'],
                "created_dow":  payload['created_dow'],
                "income_group": payload['income_group'],
                "age_group": payload['age_group']
            }

    return new_payload


def call_collections_endpoint(data):

    def allowSelfSignedHttps(allowed):
        # bypass the server certificate verification on client side
        if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
            ssl._create_default_https_context = ssl._create_unverified_context

    allowSelfSignedHttps(True) # this line is needed if you use self-signed certificate in your scoring service.

    # Request data goes here
    # The example below assumes JSON formatting which may be updated
    # depending on the format your endpoint expects.
    # More information can be found here:
    # https://docs.microsoft.com/azure/machine-learning/how-to-deploy-advanced-entry-script

    data_to_send =  {
        "Inputs": {
        "WebServiceInput0": data}}

    body = str.encode(json.dumps(data_to_send))

    ''' Old (With MatchFlag)
    url = 'http://20.252.11.207:80/api/v1/service/collections-regression-endpoint/score'
    api_key = 'pYpX6hli19QEEZZ7sYTMLQzBCTEzcNvQ' # Replace this with the API key for the web service
    '''

    url = 'http://20.252.11.207:80/api/v1/service/collections-endpt-no-matchflags/score'
    api_key = 'e85XU18r3ckuRqwpbyEjPcK32rF8b6r3' # Replace this with the API key for the web service


    # The azureml-model-deployment header will force the request to go to a specific deployment.
    # Remove this header to have the request observe the endpoint traffic rules
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

    req = urllib.request.Request(url, body, headers)

    try:
        response = urllib.request.urlopen(req)

        result = response.read()
        # print(result)
        json_data = json.loads(result)
        return json_data

    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))

    return


def call_complete_endpoint(data):

    def allowSelfSignedHttps(allowed):
        # bypass the server certificate verification on client side
        if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
            ssl._create_default_https_context = ssl._create_unverified_context

    allowSelfSignedHttps(True) # this line is needed if you use self-signed certificate in your scoring service.

    # Request data goes here
    # The example below assumes JSON formatting which may be updated
    # depending on the format your endpoint expects.
    # More information can be found here:
    # https://docs.microsoft.com/azure/machine-learning/how-to-deploy-advanced-entry-script

    data_to_send =  {
        "Inputs": {
        "WebServiceInput0": data}}

    body = str.encode(json.dumps(data_to_send))

    ''' Old (With MatchFlag)
    url = 'http://20.252.11.207:80/api/v1/service/complete-classification-endpoint/score'
    api_key = '73eCMwM3jsZDthVABC2EUc4KOnWPT0e3' # Replace this with the API key for the web service
    '''

    url = 'http://20.252.11.207:80/api/v1/service/complete-endpoint-no-matchflags/score'
    api_key = 'qj7DSpgvmKwxJaSzwud2whcSmNFBpbrr' # Replace this with the API key for the web service

    # The azureml-model-deployment header will force the request to go to a specific deployment.
    # Remove this header to have the request observe the endpoint traffic rules
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

    req = urllib.request.Request(url, body, headers)

    try:
        response = urllib.request.urlopen(req)

        result = response.read()
        # print(result)
        json_data = json.loads(result)
        return json_data


    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))

    return
    

def make_api_calls(payload_list):

    compiled_payload_to_send = []
    ids = []
    for item in payload_list:
        ids.append(item['data']['id'])
        payload = impute_missing_values(item)
        payload = pre_process_fields(payload)
        payload = build_payload_to_send(payload)
        compiled_payload_to_send.append(payload)

    # Send request in batches if needed
    if len(compiled_payload_to_send) > 5000:
        data_dict_smaller_lists = list(split(compiled_payload_to_send, int(len(compiled_payload_to_send)/5000)))
    else:
        data_dict_smaller_lists = []
        data_dict_smaller_lists.append(compiled_payload_to_send)

    complete_predictions = []
    collections_predictions = []

    for data_list in data_dict_smaller_lists:
        complete_result = call_complete_endpoint(data_list)
        for x in range(len(complete_result['Results']['WebServiceOutput0'])):
            complete_prediction = complete_result['Results']['WebServiceOutput0'][x]['Scored Probabilities']
            complete_predictions.append(complete_prediction)

        collections_result = call_collections_endpoint(data_list)
        for x in range(len(collections_result['Results']['WebServiceOutput0'])):
            collections_prediction = collections_result['Results']['WebServiceOutput0'][x]['Scored Labels']
            collections_predictions.append(collections_prediction)

    return ids, complete_predictions, collections_predictions


def get_result_segments(ids, complete_predictions, collections_predictions):

    completed_segments = []
    collections_segments = []

    for x in range(len(complete_predictions)):

        if complete_predictions[x] >= completed_percentile_thresholds[0]:
            completed_segments.append(1)
        elif complete_predictions[x] >= completed_percentile_thresholds[1]:
            completed_segments.append(2)
        elif complete_predictions[x] >= completed_percentile_thresholds[2]:
            completed_segments.append(3)
        elif complete_predictions[x] >= completed_percentile_thresholds[3]:
            completed_segments.append(4)
        elif complete_predictions[x] >= completed_percentile_thresholds[4]:
            completed_segments.append(5)
        elif complete_predictions[x] >= completed_percentile_thresholds[5]:
            completed_segments.append(6)
        elif complete_predictions[x] >= completed_percentile_thresholds[6]:
            completed_segments.append(7)
        elif complete_predictions[x] >= completed_percentile_thresholds[7]:
            completed_segments.append(8)
        elif complete_predictions[x] >= completed_percentile_thresholds[8]:
            completed_segments.append(9)
        elif complete_predictions[x] >= completed_percentile_thresholds[9]:
            completed_segments.append(10)
        elif complete_predictions[x] >= completed_percentile_thresholds[10]:
            completed_segments.append(11)
        elif complete_predictions[x] >= completed_percentile_thresholds[11]:
            completed_segments.append(12)
        elif complete_predictions[x] >= completed_percentile_thresholds[12]:
            completed_segments.append(13)
        elif complete_predictions[x] >= completed_percentile_thresholds[13]:
            completed_segments.append(14)
        elif complete_predictions[x] >= completed_percentile_thresholds[14]:
            completed_segments.append(15)
        elif complete_predictions[x] >= completed_percentile_thresholds[15]:
            completed_segments.append(16)
        elif complete_predictions[x] >= completed_percentile_thresholds[16]:
            completed_segments.append(17)
        elif complete_predictions[x] >= completed_percentile_thresholds[17]:
            completed_segments.append(18)
        elif complete_predictions[x] >= completed_percentile_thresholds[18]:
            completed_segments.append(19)
        else:
            completed_segments.append(20)

        
        if collections_predictions[x] >= collections_percentile_thresholds[0]:
            collections_segments.append(1)
        elif collections_predictions[x] >= collections_percentile_thresholds[1]:
            collections_segments.append(2)
        elif collections_predictions[x] >= collections_percentile_thresholds[2]:
            collections_segments.append(3)
        elif collections_predictions[x] >= collections_percentile_thresholds[3]:
            collections_segments.append(4)
        elif collections_predictions[x] >= collections_percentile_thresholds[4]:
            collections_segments.append(5)
        elif collections_predictions[x] >= collections_percentile_thresholds[5]:
            collections_segments.append(6)
        elif collections_predictions[x] >= collections_percentile_thresholds[6]:
            collections_segments.append(7)
        elif collections_predictions[x] >= collections_percentile_thresholds[7]:
            collections_segments.append(8)
        elif collections_predictions[x] >= collections_percentile_thresholds[8]:
            collections_segments.append(9)
        elif collections_predictions[x] >= collections_percentile_thresholds[9]:
            collections_segments.append(10)
        elif collections_predictions[x] >= collections_percentile_thresholds[10]:
            collections_segments.append(11)
        elif collections_predictions[x] >= collections_percentile_thresholds[11]:
            collections_segments.append(12)
        elif collections_predictions[x] >= collections_percentile_thresholds[12]:
            collections_segments.append(13)
        elif collections_predictions[x] >= collections_percentile_thresholds[13]:
            collections_segments.append(14)
        elif collections_predictions[x] >= collections_percentile_thresholds[14]:
            collections_segments.append(15)
        elif collections_predictions[x] >= collections_percentile_thresholds[15]:
            collections_segments.append(16)
        elif collections_predictions[x] >= collections_percentile_thresholds[16]:
            collections_segments.append(17)
        elif collections_predictions[x] >= collections_percentile_thresholds[17]:
            collections_segments.append(18)
        elif collections_predictions[x] >= collections_percentile_thresholds[18]:
            collections_segments.append(19)
        else:
            collections_segments.append(20)

    result_segments = []    
    for x in range(len(completed_segments)):
        result_segments.append(int((completed_segments[x] + collections_segments[x]) / 2 ))

    result_list = []
    for x in range(len(result_segments)):
        temp_dict = {
                        "id": ids[x],
                        "segment_result": result_segments[x]
                    }
        result_list.append(temp_dict)

    '''
    result_dict = {
        "Results": {
            "WebServiceOutput0": result_list
            }
        }
    '''

    result_dict =  {
            "result": result_list
            }

    return result_dict


def split(a, n):
    # a is list to split, n is desired number of lists 
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

# initialize our Flask application
app = Flask(__name__)

@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')

@app.route("/get-segments", methods=["POST"])
def setName():
    if request.method == 'POST':
        posted_data = request.get_json()

        print("Posted_data:", posted_data)

        payload_list = posted_data['data']

        ids, complete_predictions, collections_predictions = make_api_calls(payload_list)

        return get_result_segments(ids, complete_predictions, collections_predictions)

#  main thread of execution to start the server
if __name__ == '__main__':
    app.run()