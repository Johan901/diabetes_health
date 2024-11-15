# Project ETL_ 📊

## Overview
This project is a demonstration of basic ETL (Extract, Transform, Load) processes and techniques for the ETL class. It focuses on analyzing a health-related dataset, specifically on diabetes, with detailed information on 1,879 patients. Throughout the project, various data analysis and visualization tools are utilized. More updates and features will be added in the future. 🚀



## Features 🌟
- Loading data from a CSV file into a PostgreSQL database. 📂 ➡️ 🛢️
- Exploratory Data Analysis (EDA) using Python and Jupyter Notebook. 📊
- Data visualization using charts to identify patterns and trends. 📈
- Secure storage of database credentials using configuration files. 🔒
- Implementation and documentation on GitHub with sensitive data protection. 🌐

## Technologies Used 🖥️
- Python 🐍
- Jupyter Notebook 📓
- PostgreSQL 🛢️
- Pandas 🐼
- Matplotlib 📊
- SQLAlchemy ⚙️
- dotenv (for managing environment variables) 🌐
- GitHub 🐙

## Setup 🛠️
1. **Clone the repository:**
   ```sh
   git clone https://github.com/Johan901/project_ETL.git

2. **Create and activate the virtual environment:**
    ```sh
    python -m venv venv
    venv\Scripts\activate  # On Windows
    source venv/bin/activate  # On MacOS/Linux

3. **Install the dependencies::**
    ```sh
    pip install -r requirements.txt

4. **Run Jupyter Notebook:**
    ```sh
    jupyter notebook


## Data Migration Process 📂 ➡️ 🛢️

- Create the PostgreSQL Database: We started by creating a local PostgreSQL database named diabetes_data.
- Load Data into PostgreSQL:
    The data was imported from a CSV file into the PostgreSQL database.
    We created a table named diabetes_data with the necessary columns to hold all the information.
    The data insertion was handled using Python scripts that connected to the database using credentials stored in a credentials.json file.

## Exploratory Data Analysis (EDA) 📊

- Data Import: The dataset was imported directly from the PostgreSQL database into our Jupyter Notebook environment using Pandas and SQLAlchemy.

- Data Cleaning: We identified and handled missing values, converted categorical variables (e.g., gender, ethnicity, smoking status) to appropriate formats, and ensured that continuous variables were within expected ranges (e.g., BMI, blood glucose levels).

- Data Visualization:

    Various charts were created to understand the distribution of health-related metrics such as blood pressure, glucose levels, and cholesterol.
    Boxplots and violin plots were used to analyze BMI and HbA1c levels across different genders and ethnicities.
    A correlation matrix was constructed to visualize the relationships between different health variables.




<h1>ACTUALIZATION 📊</h1>

<h2>Real-Time Streaming and Dashboard 📈</h2>
<ul>
    <li><strong>Kafka Producer:</strong> Streams transformed health metrics to the <code>merged_data_topic</code> topic.</li>
    <li><strong>Kafka Consumer:</strong> Processes and streams data to a real-time Streamlit dashboard.</li>
    <li><strong>Streamlit Dashboard:</strong> Displays live health metrics like average deaths, mortality trends, and correlations.</li>
</ul>

<h2>Dimensional Modeling 🗂️</h2>
<ul>
    <li><strong>Fact Tables:</strong> Metrics on mortality and diabetes.</li>
    <li><strong>Dimensions:</strong> Patient demographics, clinical measurements, and treatment details.</li>
</ul>

<h2>Power BI Dashboards 📊</h2>
<ul>
    <li><strong>Health Dashboard:</strong> Analyze BMI, smoking, and alcohol consumption impacts on health.</li>
    <li><strong>Patients Dashboard:</strong> Explore demographic patterns by gender and socioeconomic status.</li>
    <li><strong>Analysis Dashboard:</strong> Key insights into diabetes diagnostics and risk factors.</li>
</ul>


