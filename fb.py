
# coding: utf-8

# In[ ]:


from facebookads import FacebookSession, FacebookAdsApi
from facebookads.adobjects.user import User
from facebookads.adobjects.adaccount import AdAccount
from api_connectors.utils import iterate_flatten, df_to_csv, get_yesterdays_date
import pandas as pd
import requests
import json


# In[ ]:


class FacebookAdsInsights:

    def __init__(self, access_params):
        assert "app_id" in access_params.keys(), "access_params must have a app_id key"
        assert "app_secret" in access_params.keys(), "access_params must have a app_secret key"
        assert "access_token" in access_params.keys(), "access_params must have a access_token key"
        self.app_id = access_params["app_id"]
        self.app_secret = access_params["app_secret"]
        self.access_token = access_params["access_token"]
        self.session = FacebookSession(app_id = self.app_id, app_secret = self.app_secret,
                                       access_token=self.access_token)
        self.api = FacebookAdsApi(self.session, api_version="v3.1")
        FacebookAdsApi.set_default_api(self.api)
        self.me = User(fbid="me")

    def get_user_accounts(self):
        return self.me.get_ad_accounts()

    def request(self, config_params, request_params):
        assert "account_id" in config_params.keys(), "config_params must have a account_id key"
        assert "fields" in request_params.keys(), "request_params must have a fields key of type list"
        assert type(request_params["fields"]) is list, "field must be list"
        assert "params" in request_params.keys(), "request_params must have a params key of type dict"
        assert type(request_params["params"]) is dict, "params must be dict"
        assert "flatten" in config_params.keys(), "config_params must have a flatten key which can be True or False"
        assert "to_csv" in config_params.keys(), "config_params must have a to_csv key which can be True or False"

        flatten = config_params["flatten"] == "True"
        to_csv = config_params["to_csv"] == "True"

        response = AdAccount(config_params["account_id"]).get_insights(fields=request_params["fields"],
                                                            params=request_params["params"])

        if flatten and to_csv:
            response = pd.DataFrame([iterate_flatten(i.export_all_data()) for i in response])
            response = df_to_csv(response)
        elif flatten and not to_csv:
            response = pd.DataFrame([iterate_flatten(i.export_all_data()) for i in response])
        else:
            response = pd.DataFrame([iterate_flatten(i.export_all_data()) for i in response])
        return response


# In[ ]:


class FacebookInsights:

    def __init__(self, access_params, api_version = "v3.2"):
        assert "app_id" in access_params.keys(), "access_params must have a app_id key"
        assert "app_secret" in access_params.keys(), "access_params must have a app_secret key"
        assert "access_token" in access_params.keys(), "access_params must have a access_token key"
        self.app_id = access_params["app_id"]
        self.app_secret = access_params["app_secret"]
        self.access_token = access_params["access_token"]
        self.root = "https://graph.facebook.com/"
        self.api_version = api_version

    def get_report_(self, report_type, config_params, request_params):
        assert report_type in ["posts_created", "network_report"], "report not implemented"
        if report_type == "posts_created":
            c_params = {}
            r_params = {"node_id": request_params["node_id"], "parameters": {"fields": "id,name,posts"},
                              "node_component": ""}
            response = self.standard_request_(c_params, r_params)
            result = json.loads(response.text)
            df = pd.DataFrame(result["posts"]["data"])
            df["node_id"] = request_params["node_id"]
            if "filter" in config_params.keys():
                if config_params["filter"] == "yesterday":
                    df = df[pd.to_datetime(df["created_time"]).dt.date == get_yesterdays_date()]
            df = df_to_csv(df)
            return df

        elif report_type == "network_report":
            c_params = {}
            r_params = {"node_id": request_params["node_id"], "parameters": {
                "metric": """page_fan_adds,page_impressions_unique,page_engaged_users,page_fans,
                page_consumptions_by_consumption_type_unique,page_posts_impressions_organic,page_posts_impressions_paid,
                page_posts_impressions_unique,page_posts_impressions_viral,page_fan_removes,page_impressions,
                page_video_views_organic,page_video_views,page_impressions_viral""",
                "period": "day", "date_preset": "yesterday"}, "node_component": "insights"}
            response = self.standard_request_(c_params, r_params)
            response = json.loads(response.text)
            result = {}
            for i in response["data"]:
                result[i["name"]] = i["values"][0]["value"]
            df = pd.DataFrame(iterate_flatten(result), index=[0])
            df = df_to_csv(df)
            return df

    def standard_request_(self, config_params, request_params):
        assert "node_id" in request_params.keys(), "request_params must have an account_id key"
        url = self.root + self.api_version + "/" + request_params["node_id"]
        if len(request_params["node_component"]) > 0:
            url += "/" + request_params["node_component"]
        if "parameters" in request_params.keys():
            url += "?"
            for parameter in request_params["parameters"].keys():
                url += parameter+"="+request_params["parameters"][parameter]+"&"
        url += "access_token="+self.access_token
        response = requests.get(url)
        return response

    def request(self, config_params, request_params):
        assert "request_type" in config_params.keys()
        if config_params["request_type"] == "personalized":
            response = self.standard_request_(config_params, request_params)
        elif config_params["request_type"] == "predefined":
            response = self.get_report_(config_params["report_type"], config_params, request_params)
        else:
            raise Exception("request_type must be either personalized or predefined")
        return response

