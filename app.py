from flask import Flask, jsonify, request
import requests
import json

def create_payload(posted_data):
    id = posted_data['Inputs']['WebServiceInput1'][0]['id']

    try:
        ATP = posted_data['Inputs']['WebServiceInput1'][0]['ATP']
    except:
        ATP = None
    try:
        Affluence_Index = posted_data['Inputs']['WebServiceInput1'][0]['Affluence_Index']
    except:
        Affluence_Index = None
    try:
        MatchFlag_AffluenceIndex = posted_data['Inputs']['WebServiceInput1'][0]['MatchFlag_AffluenceIndex']
    except:
        MatchFlag_AffluenceIndex = None
    try:
        EconomicCohortsCodeNumeric = posted_data['Inputs']['WebServiceInput1'][0]['EconomicCohortsCodeNumeric']
    except:
        EconomicCohortsCodeNumeric = None
    try:
        EconomicCohortsCode = posted_data['Inputs']['WebServiceInput1'][0]['EconomicCohortsCode']
    except:
        EconomicCohortsCode = None
    try:
        Total_Income360 = posted_data['Inputs']['WebServiceInput1'][0]['Total_Income360']
    except:
        Total_Income360 = None
    try:
        MatchFlag_Income360 = posted_data['Inputs']['WebServiceInput1'][0]['MatchFlag_Income360']
    except:
        MatchFlag_Income360 = None
    try:
        Vantage_Score_Neighborhood_Risk_Score = posted_data['Inputs']['WebServiceInput1'][0]['Vantage Score Neighborhood Risk Score']
    except:
        Vantage_Score_Neighborhood_Risk_Score = None
    try:
        total_liquid_assets = posted_data['Inputs']['WebServiceInput1'][0]['total_liquid_assets']
    except:
        total_liquid_assets = None
    try:
        netWorthGoldMin__c = posted_data['Inputs']['WebServiceInput1'][0]['netWorthGoldMin__c']
    except:
        netWorthGoldMin__c = None
    try:
        ESI = posted_data['Inputs']['WebServiceInput1'][0]['ESI']
    except:
        ESI = None
    try:
        distance_to_center__c = posted_data['Inputs']['WebServiceInput1'][0]['distance_to_center__c']
    except:
        distance_to_center__c = None
    try:
        first_campaign_type = posted_data['Inputs']['WebServiceInput1'][0]['first_campaign_type']
    except:
        first_campaign_type = None
    try:
        CreatedBy_Channel = posted_data['Inputs']['WebServiceInput1'][0]['CreatedBy_Channel']
    except:
        CreatedBy_Channel = None
    try:
        has_email_address = posted_data['Inputs']['WebServiceInput1'][0]['has_email_address']
    except:
        has_email_address = None
    try:
        dental_condition = posted_data['Inputs']['WebServiceInput1'][0]['dental_condition']
    except:
        dental_condition = None
    try:
        created_hour = posted_data['Inputs']['WebServiceInput1'][0]['created_hour']
    except:
        created_hour = None
    try:
        created_dow = posted_data['Inputs']['WebServiceInput1'][0]['created_dow']
    except:
        created_dow = None

    payload = json.dumps({
        "Inputs": {
            "WebServiceInput1": [
            {
                "id": id,
                "ATP": ATP,
                "Affluence_Index": Affluence_Index,
                "MatchFlag_AffluenceIndex": MatchFlag_AffluenceIndex,
                "EconomicCohortsCodeNumeric": EconomicCohortsCodeNumeric,
                "EconomicCohortsCode": EconomicCohortsCode,
                "Total_Income360": Total_Income360,
                "MatchFlag_Income360": MatchFlag_Income360,
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
            ]
        },
        "GlobalParameters": {}
        })

    return payload

# initialize our Flask application
app = Flask(__name__)


@app.route("/name", methods=["POST"])
def setName():
    if request.method == 'POST':
        posted_data = request.get_json()
        print("Posted_data:", posted_data)

        payload = create_payload(posted_data)

        url = "http://20.252.11.207:80/api/v1/service/lead-segmentation-endpoint/score"

        headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer NO1EPZHoxb9IqEACnfBsB1j3mvuee8h7'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()['Results']['WebServiceOutput0'][0]
        #return jsonify(str("Successfully stored  " + str(data)))


@app.route("/message", methods=["GET"])
def message():
    posted_data = request.get_json()
    name = posted_data['name']
    return jsonify(" Hope you are having a good time " + name + "!!!")


#  main thread of execution to start the server
if __name__ == '__main__':
    app.run()