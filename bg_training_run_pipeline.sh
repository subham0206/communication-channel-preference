#!/bin/bash

# Home Directory
cd /home/ec2-user/SageMaker/channel_response_model

# Activate the Conda environment
source /home/ec2-user/SageMaker/custom-miniconda-3.11/miniconda/bin/activate custom_python3.11

# Run main script for BG
python training_main_channel_pref.py --brand 'BG'