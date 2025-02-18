# README #

# Customer Communication Channel Preference Project

## Overview
This project aims to analyze and predict customer preferences for communication channels using data from various platforms. The primary goal is to understand and predict how customers interact with different communication channels for diffrent message types (promotional & trigger), including Email, SMS, and Push Notifications.

#### For detailed documentation, please visit my documentation: [https://mynmg-my.sharepoint.com/:w:/r/personal/subham_srivastava_neimanmarcus_com/Documents/Customer_Communication_Preference_Modeling_Report_v1_Sept2024.docx?d=w4edd1e5219a54c6e9ac2b04c708ed65d&e=4%3a4f8b0b6fbbd746d48535dc97d34295d8&sharingv2=true&fromShare=true&at=9]

## Communication Platforms
The project focuses on three types of communication platforms:

### Email
- **Epsilon**
- **Bluecore**
- **SendGrid**

### SMS
- **Attentive**

### Push Notification
- **Airship** (Data still not available in snowflake)

## Project Workflow
The project follows a structured workflow to ensure comprehensive analysis and modeling:

1. **Understanding Communication Platforms**: 
   - Gathered insights into the functionalities and data provided by each communication platform.

2. **Data Extraction**: 
   - Extracted data from the identified communication platforms.

3. **Data Processing**:
   - **Raw Data Processing**: Cleaned and preprocess the raw data for analysis.
   - **Exploratory Data Analysis (EDA)**: Performed EDA to understand data distributions, trends, and patterns.

4. **Data Transformation**:
   - Transformed raw data to prepare it for analysis.

5. **Analysis**:
   - Conducted various analyses, including:
     - **Univariate Analysis**: Analyze individual variables.
     - **Multivariate Analysis**: Examine relationships between multiple variables.
     - **Categorical Analysis**: Focus on categorical variables.
  
6. **Handling Missing Values**: 
   - Implemented strategies to address missing data in the dataset.

7. **Outlier Detection**: 
   - Identified and handle outliers in the data.

8. **Feature Engineering**: 
   - Created new features based on existing data to improve model performance.

9. **Correlation Check and Feature Selection**: 
   - Analyzed feature correlations and select relevant features for modeling.

10. **Modeling**:
    - Began with **Unsupervised Learning** techniques to identify patterns and clusters in the data.
    - Progress to **Supervised Learning**, experimenting with various models including:
      - **Decision Trees (DT)**
      - **Random Forest (RF)**
      - **LightGBM (LGBM)**
      - **XGBoost (XGB)**
      - Other ensemble-based models.
11. **Model Score and Evaluation**: 
	- Analyzed and Evaluated Model result

## Technologies Used
- Python
- Pandas
- NumPy
- Scikit-learn
- Matplotlib
- Seaborn

## Contribution
Contributions are welcome! Please feel free to submit a pull request or open an issue if you have suggestions or improvements. (subham_srivastava@neimanmarcus.com)


## Acknowledgments
- Epsilon
- Bluecore
- SendGrid
- Attentive
- Airship

