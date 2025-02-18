import sys
import argparse
from datetime import date
from scripts.data_extraction_functions import  get_attentive_data, get_bluecore_data, get_sendgrid_data, get_epsilon_data, get_promotional_feature_engg_data, get_customer_transactional_features_data, get_trigger_feature_engg_data, get_promo_trigger_model_ready_data
from scripts.modeling_and_evaluation import modeling_pipeline
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
bucket_name = 'nmg-analytics-ds-prod'
save_dir_name = 'ds/prod/channel_preference/'

def main(build, logger, brand):
    logger._send_sns_notification(f'Starting execution of Automated {build.project_name} on {build.prod_instance_name} Instance.')
    try: 
        logger.log_info(f'Starting execution of Automated {build.project_name} on {build.prod_instance_name} Instance.\n\t Setting up initial use cases.')

        logger.log_info("\nExecuting function attentive data extraction...")
        get_attentive_data(logger,  brand)

        logger.log_info("\nExecuting function bluecore data extraction...")
        get_bluecore_data(logger,  brand)

        logger.log_info("\nExecuting function sendgrid data extraction...")
        get_sendgrid_data(logger,  brand)

        logger.log_info("\nExecuting function epsilon data extraction...")
        get_epsilon_data(logger,  brand)

        logger.log_info("\nExecuting function customer transactional features data extraction...")
        get_customer_transactional_features_data(logger,  brand)

        logger.log_info("\nExecuting function trigger features engineering data...")
        get_trigger_feature_engg_data(logger,  brand)

        logger.log_info("\nExecuting function promotional features engineering data...")
        get_promotional_feature_engg_data(logger,  brand)

        logger.log_info("\nExecuting function to extract promotional and trigger model ready data")
        promo_data, trigger_data = get_promo_trigger_model_ready_data(logger,  brand)

        logger.log_info("\nExecuting promotional model result")
        promo_results = modeling_pipeline(
    logger,
    final_promo_df=promo_data,
    bucket_name=bucket_name,
    save_dir=save_dir_name,
    result_file_name=f'{brand}_promo_train_model_results.json',
    model_file_name=f'{brand}_promo_model.pkl'
)
        logger.log_info("\n promotional model result saved succesfully")

        logger.log_info("\nExecuting trigger model result")
        trigger_results = modeling_pipeline(
    logger,
    final_promo_df=trigger_data,
    bucket_name=bucket_name,
    save_dir=save_dir_name,
    result_file_name=f'{brand}_trigger_train_model_results.json',
    model_file_name=f'{brand}_trigger_model.pkl'
)
        logger.log_info("\n trigger model result saved succesfully")

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