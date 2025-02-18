import pandas as pd
import numpy as np
import pickle
import json
import boto3
import os
from datetime import datetime
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, accuracy_score, f1_score
from imblearn.over_sampling import SMOTE
from utils import tools as t

def train_test_split_func(df, features, target, test_size=0.3, random_state=42, stratify=True, logger=None):
    """
    Function to perform train-test split.
    """
    stratify_col = df[target] if stratify else None
    logger.log_info("Performing train-test split...")
    X_train, X_test, y_train, y_test = train_test_split(
        df[features], df[target], test_size=test_size, random_state=random_state, stratify=stratify_col
    )
    logger.log_info("Train-test split completed.")
    return X_train, X_test, y_train, y_test

def train_and_save_model_to_s3(X_train, y_train, model_name, bucket_name, save_dir, model_file_name, smote=False, logger=None):
    """
    Function to train a model and save it as a pickle file in S3.
    """
    logger.log_info(f"Training model '{model_name}'...")

    # Apply SMOTE
    if smote:
        logger.log_info("Applying SMOTE to balance the dataset...")
        smote_instance = SMOTE(random_state=42)
        X_train, y_train = smote_instance.fit_resample(X_train, y_train)

    # Train the model
    model = XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42)
    model.fit(X_train, y_train)

    # Save the trained model to S3
    t.save_pickle_to_s3(
        obj_to_save=model,
        bucket_nameX=bucket_name,
        save_dir_nameX=save_dir,
        file_nameX=f"{model_name}{model_file_name}" 
    )
    logger.log_info(f"Model '{model_name}' trained and saved to S3.")
    return model

def evaluate_model(name, y_true, y_pred, logger=None):
    """
    Function to evaluate a model and print metrics.
    """
    logger.log_info(f"Evaluating model '{name}'...")

    accuracy = accuracy_score(y_true, y_pred)
    f1_weighted = f1_score(y_true, y_pred, average='weighted')
    class_report = classification_report(y_true, y_pred, output_dict=True)

    logger.log_info(f'{name} Performance:')
    logger.log_info(f'Accuracy: {accuracy:.4f}')
    logger.log_info(f'F1 Score (weighted): {f1_weighted:.4f}')
    logger.log_info('Classification Report:')
    logger.log_info(classification_report(y_true, y_pred))
    logger.log_info('-' * 50)

    metrics = {
        'accuracy': accuracy,
        'f1_weighted': f1_weighted,
        'class_report': class_report
    }
    return metrics

def save_results_to_s3(results, bucket_name, save_dir, result_file_name, logger=None):
    """
    Save the results to an S3 bucket as a JSON file.
    """
    logger.log_info(f"Saving results to S3 at: s3://{bucket_name}/{save_dir}/{result_file_name}")
    s3 = boto3.resource('s3')
    result_json = json.dumps(results, indent=4)
    s3.Object(bucket_name, f"{save_dir}/{result_file_name}").put(Body=result_json)
    logger.log_info(f"Results saved to S3 at: s3://{bucket_name}/{save_dir}/{result_file_name}")

def save_pred_results_to_s3(results, bucket_name, save_dir, result_file_name, logger):
    try:
        # Convert DataFrame to JSON serializable format
        s3 = boto3.resource('s3')
        result_json = json.dumps(results.to_dict(orient='records'), indent=4)
        # Save result_json to S3 (implementation assumed)
        s3.Object(bucket_name, f"{save_dir}/{result_file_name}").put(Body=result_json)
        logger.log_info(f"Results saved to S3 at: s3://{bucket_name}/{save_dir}/{result_file_name}")
    except Exception as e:
        logger.log_error(f"Error saving results to S3")
        raise

def modeling_pipeline(logger, final_promo_df, bucket_name, save_dir, result_file_name, model_file_name):
    """
    Two-Layer Modeling pipeline where the First Model's predictions (Neither vs Rest)
    are used as input for the Second Model (Email vs SMS vs Both).
    """
    logger.log_info("Modeling pipeline started")

    # Drop unnecessary columns before train-test split
    cols_to_drop = ['email_clicked_count', 'sms_clicked', 'email_engagement_ratio', 'sms_engagement_ratio']
    final_promo_df = final_promo_df.drop(columns=cols_to_drop)
    logger.log_info(f"Dropped columns: {cols_to_drop}")

    # Drop unnecessary columns
    if 'curr_customer_id' in final_promo_df.columns:
        final_promo_df = final_promo_df.drop(columns=['curr_customer_id'])
        logger.log_info("Dropped column 'curr_customer_id'")

    # Handle object columns
    object_cols = final_promo_df.select_dtypes(include='object').columns
    if len(object_cols) > 0:
        logger.log_info(f"Warning: Dropping object columns: {list(object_cols)}")
        final_promo_df = final_promo_df.drop(columns=object_cols)

    # Define features and target
    features = [col for col in final_promo_df.columns if col != 'preferred_channel_encoded']
    target = 'preferred_channel_encoded'

    # First Model: Neither (0) vs Rest (1)
    logger.log_info("Preparing First Model (Neither vs Rest)...")
    first_model_df = final_promo_df.copy()
    first_model_df[target] = np.where(first_model_df[target] == 0, 0, 1)

    # Train-test split for the first model
    X_train_first, X_test_first, y_train_first, y_test_first = train_test_split_func(
        first_model_df, features, target, logger=logger
    )

    model_first = train_and_save_model_to_s3(
        X_train_first, y_train_first, "first_model_", bucket_name, save_dir, model_file_name, logger=logger
    )
    y_pred_first = model_first.predict(X_test_first)
    first_metrics = evaluate_model("First Model", y_test_first, y_pred_first, logger=logger)

    # Second Model: Email (1), SMS (2), Both (3)
    logger.log_info("Preparing Second Model (Email vs SMS vs Both)...")
    second_model_df = final_promo_df[final_promo_df[target].isin([1, 2, 3])].copy()
    second_model_df[target] = second_model_df[target].replace({3: 0})

    # Train-test split for the second model
    X_train_second, X_test_second, y_train_second, y_test_second = train_test_split_func(
        second_model_df, features, target, logger=logger
    )
    model_second = train_and_save_model_to_s3(
        X_train_second, y_train_second, "second_model_", bucket_name, save_dir, model_file_name, smote=True, logger=logger
    )
    y_pred_second = model_second.predict(X_test_second)
    second_metrics = evaluate_model("Second Model", y_test_second, y_pred_second, logger=logger)

    # Combine results
    results = {
        'first_model': first_metrics,
        'second_model': second_metrics
    }

    # Print results in JSON format
    logger.log_info("Modeling Results:")
    logger.log_info(json.dumps(results, indent=4))

    # Save results to S3
    save_results_to_s3(results, bucket_name, save_dir,result_file_name, logger=logger)
    return results

def load_model_from_s3(bucket_name, save_dir, model_file_name, logger=None):
    """
    Loads a pickle model file from an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        save_dir (str): Directory path in the S3 bucket where the model is stored.
        model_file_name (str): Name of the pickle model file.
        logger (optional): Logger instance for logging.

    Returns:
        Object: The loaded model.
    """
    try:
        # Log the loading action
        if logger:
            logger.log_info(f"Loading model from S3: s3://{bucket_name}/{save_dir}/{model_file_name}")

        # Create S3 client
        s3 = boto3.client('s3')

        # Temporary local file path
        local_file_path = os.path.join('/tmp', model_file_name)

        # Download the model file from S3
        s3.download_file(bucket_name, f"{save_dir}/{model_file_name}", local_file_path)

        # Load the pickle model
        with open(local_file_path, 'rb') as model_file:
            model = pickle.load(model_file)

        if logger:
            logger.log_info(f"Model successfully loaded from S3: {model_file_name}")

        return model
    except Exception as e:
        if logger:
            logger.log_error(f"Error loading model from S3: {str(e)}")
        raise
def pred_pipeline(logger, final_promo_df, bucket_name, save_dir, result_file_name,result_df_name,
                            first_model_file_name, second_model_file_name, brand):
    """
    Prediction pipeline for modeling approach. 
    The first model predicts 'neither' vs 'rest', and the second model predicts 'email' vs 'sms' vs 'both'.

    Args:
        logger: Logger instance for logging information and errors.
        final_promo_df: DataFrame containing input data for predictions.
        bucket_name: S3 bucket name where the models are stored.
        save_dir: Directory within the bucket to save results or fetch the models.
        result_file_name: Name of the file to save prediction results.
        first_model_file_name: Name of the first model file (pickle format).
        second_model_file_name: Name of the second model file (pickle format).
        brand: The brand name to associate with predictions.

    Returns:
        Dictionary containing prediction results from both models.
    """
    try:
        logger.log_info("prediction pipeline started")

        # Record the run date and time
        run_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.log_info(f"Run date: {run_date}")

        # Preserve original DataFrame for customer_id and output purposes
        original_df = final_promo_df.copy()

        # Drop unnecessary columns for model input
        cols_to_drop = ['email_clicked_count', 'sms_clicked', 'email_engagement_ratio', 'sms_engagement_ratio']
        final_promo_df = final_promo_df.drop(columns=cols_to_drop, errors='ignore')
        logger.log_info(f"Dropped columns: {cols_to_drop}")

        if 'curr_customer_id' in final_promo_df.columns:
            final_promo_df = final_promo_df.drop(columns=['curr_customer_id'])
            logger.log_info("Dropped column 'curr_customer_id'")

        # Handle object columns
        object_cols = final_promo_df.select_dtypes(include='object').columns
        if len(object_cols) > 0:
            logger.log_info(f"Warning: Dropping object columns: {list(object_cols)}")
            final_promo_df = final_promo_df.drop(columns=object_cols)

        # Extract features (exclude target if present)
        features = [col for col in final_promo_df.columns if col != 'preferred_channel_encoded']
        X_input = final_promo_df[features]

        logger.log_info(f"X_input: {X_input.shape}")

        # Load the first model from S3
        logger.log_info("Loading first model (Neither vs Rest) from S3")
        first_model = load_model_from_s3(bucket_name, save_dir, first_model_file_name, logger=logger)
        logger.log_info("First model loaded successfully")

        # Predict 'neither' vs 'rest' with the first model
        logger.log_info("Making predictions using the first model")
        first_model_predictions = first_model.predict(X_input)
        logger.log_info(f"First model predictions: {first_model_predictions.shape}")

        # Filter data for the second model ('rest' class only)
        second_model_input = X_input[first_model_predictions == 1]  # Select rows where the first model predicts 'rest'
        logger.log_info(f"Second model input shape: {second_model_input.shape}")

        # Load the second model from S3
        logger.log_info("Loading second model (Email vs SMS vs Both) from S3")
        second_model = load_model_from_s3(bucket_name, save_dir, second_model_file_name, logger=logger)
        logger.log_info("Second model loaded successfully")

        # Predict 'email', 'sms', or 'both' using the second model
        logger.log_info("Making predictions using the second model")
        second_model_predictions = second_model.predict(second_model_input)
        logger.log_info(f"Second model predictions: {second_model_predictions.shape}")

        # Map predictions to labels
        first_channel_mapping = {0: "neither", 1: "rest"}
        second_channel_mapping = {0: "both", 1: "email", 2: "sms"}

        # Combine predictions into a single output
        combined_predictions = []
        second_model_idx = 0  # Pointer to track rows in second_model_predictions

        for i in range(len(X_input)):
            if first_model_predictions[i] == 0:  # First model predicts "neither"
                combined_predictions.append("neither")
            else:  # Use the second model's prediction
                if second_model_idx < len(second_model_predictions):
                    prediction_idx = second_model_predictions[second_model_idx]
                    if 0 <= prediction_idx < len(second_channel_mapping):
                        combined_predictions.append(second_channel_mapping[prediction_idx])
                    else:
                        logger.log_error(
                            "PRED_PIPELINE_MAPPING_ERROR",
                            f"Invalid prediction index {prediction_idx} for second_channel_mapping with size {len(second_channel_mapping)}"
                        )
                        combined_predictions.append("error")  # Fallback value
                    second_model_idx += 1
                else:
                    logger.log_error(
                        "PRED_PIPELINE_LENGTH_MISMATCH",
                        "Mismatch between first_model_predictions and second_model_predictions."
                    )
                    combined_predictions.append("error")  # Fallback value

        # Create predictions DataFrame
        predictions_df = pd.DataFrame({
            'brand': brand,
            'curr_cmd_id': original_df['curr_customer_id'],  # Use original_df to get customer_id
            'predicted_preferred_channel': combined_predictions,
            'run_date': run_date
        })

        logger.log_info(f"predictions_df: {predictions_df.shape}")
        # Save prediction results to S3
        logger.log_info("Saving prediction results to S3")
        t.save_df_to_s3(df_to_save=predictions_df, 
                        bucket_nameX='nmg-analytics-ds-prod',
                        save_dir_nameX='ds/prod/channel_preference/', 
                        file_nameX=result_file_name)
        logger.log_info("Prediction results saved to S3 successfully")

        # Save to Snowflake
        logger.log_info("Saving prediction results to Snowflake")
        t.upload_df_from_SageMaker_to_SF_Sandbox(predictions_df, result_df_name)
        logger.log_info("Prediction results saved to Snowflake successfully")

        logger.log_info("Two-layer prediction pipeline completed successfully")
        return {"status": "success", "predictions": predictions_df}

    except Exception as e:
        logger.log_error(f"An error occurred in the prediction pipeline: {str(e)}")
        return {"status": "error", "error": str(e)}
