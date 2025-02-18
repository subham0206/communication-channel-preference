import sys
import argparse
from datetime import date
from scripts.data_extraction_functions import  get_attentive_data, get_bluecore_data, get_sendgrid_data, get_epsilon_data,get_promotional_feature_engg_data, get_customer_transactional_features_data, get_trigger_feature_engg_data,get_promo_trigger_model_ready_data, get_promo_trigger_model_ready_pred_data,get_trigger_feature_engg_pred_data, get_promotional_feature_engg_pred_data,get_attentive_pred_data, get_bluecore_pred_data,get_sendgrid_pred_data, get_epsilon_pred_data,get_customer_transactional_features_pred_data
from scripts.modeling_and_evaluation import modeling_pipeline, pred_pipeline
from config.logging_object import ErrorLogger
from config.configs import channel_pref_analysis_config
from utils import tools as t
import boto3
import warnings
import subprocess
warnings.filterwarnings("ignore")
client = boto3.client('s3')
resource = boto3.resource('s3')
import logging


# Configure logging
logging.basicConfig(level=logging.DEBUG)

start_month, start_year, end_month, end_year = t.get_date_range()
batch_size = 10000
bucket_name = 'nmg-analytics-ds-prod'
save_dir_name = 'ds/prod/channel_preference'

def main(build, logger, brand):
    logger._send_sns_notification(f'Starting execution of Automated {build.project_name} on {build.prod_instance_name} Instance.')
    try:
        logger.log_info(f'Starting execution of Automated {build.project_name} on {build.prod_instance_name} Instance.\n\t Setting up initial use cases.')

        logger.log_info("\nExecuting function attentive pred data extraction...")
        get_attentive_pred_data(logger,  brand)

        logger.log_info("\nExecuting function bluecore pred data extraction...")
        get_bluecore_pred_data(logger,  brand)

        logger.log_info("\nExecuting function sendgrid pred data extraction...")
        get_sendgrid_pred_data(logger,  brand)

        logger.log_info("\nExecuting function epsilon pred data extraction...")
        get_epsilon_pred_data(logger,  brand)

        logger.log_info("\nExecuting function customer transactional features pred data extraction...")
        get_customer_transactional_features_pred_data(logger,  brand)

        logger.log_info("\nExecuting function trigger features engineering pred data...")
        get_trigger_feature_engg_pred_data(logger,  brand)

        logger.log_info("\nExecuting function promotional features engineering pred data...")
        get_promotional_feature_engg_pred_data(logger,  brand)

        logger.log_info("\nExecuting function to extract promotional and trigger model ready pred data")
        promo_data, trigger_data = get_promo_trigger_model_ready_pred_data(logger,  brand)

        logger.log_info("\nExecuting promotional model pred result")
        promo_results = pred_pipeline(logger, final_promo_df=promo_data,bucket_name= bucket_name, save_dir= save_dir_name, result_file_name=f'{brand}_channel_preference_promotional_predictions.csv', result_df_name=f'{brand}_channel_preference_promotional_predictions',first_model_file_name=f'first_model_{brand}_promo_model.pkl', second_model_file_name=f'second_model_{brand}_promo_model.pkl' , brand = brand)
        logger.log_info("\n pred promotional model result saved succesfully")

        logger.log_info("\nExecuting trigger model pred result")
        trigger_results = pred_pipeline(logger, final_promo_df=trigger_data,bucket_name= bucket_name, save_dir= save_dir_name, result_file_name=f'{brand}_channel_preference_trigger_predictions.csv', result_df_name=f'{brand}_channel_preference_trigger_predictions',first_model_file_name=f'first_model_{brand}_trigger_model.pkl', second_model_file_name=f'second_model_{brand}_trigger_model.pkl', brand = brand)
        logger.log_info("\n pred trigger model result saved succesfully")

        logger._send_sns_notification(f'Execution of Automated {build.project_name} script on {build.prod_instance_name} finished succesfully!')
    except Exception as e:
        logger.log_error(repr(e), f"Error in {build.project_name} running on {build.prod_instance_name}, please reach out to Subham if you cannot find source of error.\n {repr(e)}")
        logger.log_info(repr(e))
        print(repr(e))
        raise e
    finally:
        logger.upload_logs_to_s3()
        sys.exit(0)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand parameter")
    args = parser.parse_args()

    # Setting up logger object
    build = channel_pref_analysis_config()
    logger = ErrorLogger(
        git_project_name=build.project_name,
        s3_bucket=build.s3_bucket,
        s3_key_prefix=f"{build.s3_project_path}logs/",
        sns_topic_arn=build.sns_topic_arn
    )

    main(build=build, logger=logger, brand=args.brand)