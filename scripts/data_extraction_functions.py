import sys
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from scripts.sql_functions import  get_query_attentive_data, get_query_bluecore_data, get_query_sendgrid_data,get_query_epsilon_data,get_query_customer_transactional_features_data,get_query_promotional_feature_engg_data,promo_train_test_split,get_query_trigger_feature_engg_data,trigger_train_test_split,get_query_promotional_feature_engg_pred_data,get_query_trigger_feature_engg_pred_data,promo_model_data_pred,trigger_model_data_pred,get_query_attentive_pred_data,get_query_bluecore_pred_data,get_query_sendgrid_pred_data,get_query_epsilon_pred_data,get_query_customer_transactional_features_pred_data
from io import BytesIO 
from utils import tools as t, ssm_cnx as s
import gzip
from io import BytesIO, TextIOWrapper
import boto3
client = boto3.client('s3')
resource = boto3.resource('s3')

def sf_connection():

    p = '/home/ec2-user/SageMaker/Repos/data-science'
    if p not in sys.path:
        sys.path.append(p)

    sf_cnx = s.get_snowflake_connection()
    return sf_cnx

sf_cnx = sf_connection()
scur = sf_cnx.cursor()
#start_month, start_year, end_month, end_year = t.get_date_range()

def get_attentive_data(logger,  brand):
    query = get_query_attentive_data(brand)
    logger.log_info(f"Executing attentive query: {query}") 
    t.create_new_table(query.format(brand))
    logger.log_info('attentive query executed successfully')
    return True

def get_bluecore_data(logger, brand):
    query = get_query_bluecore_data(brand)
    logger.log_info(f"Executing bluecore query: {query}")  
    t.create_new_table(query.format(brand))
    logger.log_info('bluecore query executed successfully')
    return True

def get_sendgrid_data(logger,  brand):
    query = get_query_sendgrid_data(brand)
    logger.log_info(f"Executing bluecore query: {query}")  
    t.create_new_table(query.format(brand))
    logger.log_info('sendgrid query executed successfully')
    return True

def get_epsilon_data(logger,  brand):
    query = get_query_epsilon_data(brand)
    logger.log_info(f"Executing epsilon query: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('epsilon query executed successfully')
    return True

def get_customer_transactional_features_data(logger,  brand):
    query = get_query_customer_transactional_features_data(brand)
    logger.log_info(f"Executing customer transactional features query: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('customer transactional features query executed successfully')
    return True

def get_promotional_feature_engg_data(logger,  brand):
    query = get_query_promotional_feature_engg_data(brand)
    logger.log_info(f"Executing promotional features data: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('customer promotional features query executed successfully')
    return True

def get_trigger_feature_engg_data(logger,  brand):
    query = get_query_trigger_feature_engg_data(brand)
    logger.log_info(f"Executing trigger features data: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('customer trigger features query executed successfully')
    return True

def get_promo_trigger_model_ready_data(logger,  brand):
    promo_query = promo_train_test_split(brand)
    trigger_query = trigger_train_test_split(brand)
    logger.log_info(f"Extracting promotional  data: {promo_query}")
    promo_data = t.snow_flake_execute(promo_query)
    logger.log_info(f"Extracting trigger  data: {trigger_query}")
    trigger_data = t.snow_flake_execute(trigger_query)
    logger.log_info('promotional data executed successfully')
    logger.log_info('trigger data executed successfully')
    logger.log_info("Promotional Shape Results: {}".format(promo_data.shape))
    logger.log_info("Trigger Shape Results: {}".format(trigger_data.shape))
    return promo_data, trigger_data

def get_attentive_pred_data(logger,  brand):
    query = get_query_attentive_pred_data(brand)
    logger.log_info(f"Executing pred attentive query: {query}")  # Log the final query for verification
    t.create_new_table(query.format(brand))
    logger.log_info('pred attentive query executed successfully')
    return True

def get_bluecore_pred_data(logger, brand):
    query = get_query_bluecore_pred_data(brand)
    logger.log_info(f"Executing pred bluecore query: {query}")  
    t.create_new_table(query.format(brand))
    logger.log_info('pred bluecore query executed successfully')
    return True


def get_sendgrid_pred_data(logger,  brand):
    query = get_query_sendgrid_pred_data(brand)
    logger.log_info(f"Executing pred bluecore query: {query}")  
    t.create_new_table(query.format(brand))
    logger.log_info('pred sendgrid query executed successfully')
    return True

def get_epsilon_pred_data(logger,  brand):
    query = get_query_epsilon_pred_data(brand)
    logger.log_info(f"Executing pred epsilon query: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('pred epsilon query executed successfully')
    return True

def get_customer_transactional_features_pred_data(logger,  brand):
    query = get_query_customer_transactional_features_pred_data(brand)
    logger.log_info(f"Executing pred customer transactional features query: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('customer pred transactional features query executed successfully')
    return True

"""
==================================================================================================

SECTION: Prediction Functions

==================================================================================================
"""

def get_promotional_feature_engg_pred_data(logger,  brand):
    query = get_query_promotional_feature_engg_pred_data(brand)
    logger.log_info(f"Executing promotional features data: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('customer promotional features query executed successfully for prediction')
    return True

def get_trigger_feature_engg_pred_data(logger,  brand):
    query = get_query_trigger_feature_engg_pred_data(brand)
    logger.log_info(f"Executing trigger features data: {query}")
    t.create_new_table(query.format(brand))
    logger.log_info('customer trigger features query executed successfully for prediction')
    return True

def get_promo_trigger_model_ready_pred_data(logger,  brand):
    promo_query = promo_model_data_pred(brand)
    trigger_query = trigger_model_data_pred(brand)
    logger.log_info(f"Extracting promotional  data: {promo_query}")
    promo_data = t.snow_flake_execute(promo_query)
    logger.log_info(f"Extracting trigger  data: {trigger_query}")
    trigger_data = t.snow_flake_execute(trigger_query)
    logger.log_info('promotional data executed successfully for prediction')
    logger.log_info('trigger data executed successfully for prediction')
    logger.log_info("Promotional Shape Results: {}".format(promo_data.shape))
    logger.log_info("Trigger Shape Results: {}".format(trigger_data.shape))
    return promo_data, trigger_data






