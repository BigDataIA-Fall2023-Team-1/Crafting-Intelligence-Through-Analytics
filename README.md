# Crafting Intelligence through Analytics

## Technologies Used

[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](https://streamlit.io/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![FastAPI](https://img.shields.io/badge/fastapi-109989?style=for-the-badge&logo=FASTAPI&logoColor=white)](https://fastapi.tiangolo.com/)
[![Amazon AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![HTML5](https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/Guide/HTML/HTML5)
[![Pandas](https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![GitHub Actions](https://img.shields.io/badge/Github%20Actions-282a2e?style=for-the-badge&logo=githubactions&logoColor=367cfe)](https://github.com/features/actions)
![Static Badge](https://img.shields.io/badge/Docker-%232496ED?style=for-the-badge&logo=Docker&color=blue)
![Static Badge](https://img.shields.io/badge/Google%20Cloud%20Platform-%234285F4?style=for-the-badge&logoColor=%234285F4)
![Static Badge](https://img.shields.io/badge/ApacheSpark-%23E25A1C?style=for-the-badge&logo=ApacheSpark&logoColor=%23E25A1C&color=green)
![Static Badge](https://img.shields.io/badge/OpenAI-%23412991?style=for-the-badge&logo=OpenAI&logoColor=%23412991&color=red)
![Static Badge](https://img.shields.io/badge/Postgres-%234169E1?style=for-the-badge&logo=PostgreSQL&logoColor=%234169E1&color=black)

## Description:

This project entails the integration of Airflow, Docker, FastAPI, and Streamlit to create a robust end-to-end solution for job-related information retrieval and interaction. The primary scope involves utilizing web scraping tools and techniques to extract relevant job data from LinkedIn. The Airflow framework will be employed to orchestrate and schedule the scraping process, ensuring timely updates and data freshness. Docker containers will facilitate seamless deployment and scalability of the application. FastAPI will be leveraged to build a RESTful API, enabling efficient communication between components. Streamlit, as a user interface framework, will be employed to create an interactive front-end for end-users to engage with the chatbot.

In addition to web scraping, natural language processing (NLP) and machine learning technologies will be integrated to refine and process the extracted job information. These technologies will empower the language model within the chatbot, enhancing its ability to understand, generate, and respond to job-related queries with contextual accuracy.

Expected deliverables encompass a comprehensive dataset of job-related information sourced from LinkedIn, a scalable and containerized application architecture using Docker, a well-orchestrated workflow managed by Airflow, a RESTful API developed with FastAPI for seamless communication, and a user-friendly, interactive interface powered by Streamlit. The integration of NLP and machine learning will contribute to the sophistication of the language model, ensuring a more nuanced and context-aware interaction.

# Links

Codelabs - https://codelabs-preview.appspot.com/?file_id=1UmA1PbuQUKTWYhzkq_1JBijOSzBVlyjgwNkJzHTicBY#3

# Architecture:
![Alt text](Architecture.png)

# Team Information

Name | NUID | Contribution 
--- | --- | --- |
Manohar Indukuri | 002774942 | 34%
Prathamesh Kulkarni | 001560684 | 33%
Sarvesh Malpani | 002776061 | 33%


# Environment Variables

```
#Openai
OPENAI_API="Your_Secret_Key"

# Snowflake 
SNOWFLAKE_ACCOUNT=""
SNOWFLAKE_USER=""
SNOWFLAKE_PASSWORD=""
SNOWFLAKE_ROLE=""
SNOWFLAKE_WAREHOUSE=""
SNOWFLAKE_DATABASE=""
SNOWFLAKE_TABLE=""

# Postgres
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_HOST=""
POSTGRES_PORT="5432"
POSTGRES_DB=""
HOST_IP_ADDRESS=""

# email
MY_EMAIL=""
APP_PASSWORD=""

# Airflow
AIRFLOW_VAR_AWS_ACCESS_KEY1=""
AIRFLOW_VAR_AWS_SECRET_KEY1=""
AIRFLOW_VAR_S3_BUCKET_NAME1=""
AIRFLOW_VAR_AWS_REGION1=""

AIRFLOW_VAR_AWS_ACCESS_KEY=""
AIRFLOW_VAR_AWS_SECRET_KEY=""
AIRFLOW_VAR_S3_BUCKET_NAME=""
AIRFLOW_VAR_AWS_REGION=""
```

# Environment Setup

### Create virtual environment
```
make install
```

### Run FastAPI server

```
make runserver
```

### Run streamlit application

```
make streamlit
```

# Database Setup

- Create Table
```
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE,
    full_name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    hashed_password VARCHAR(255),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);
```

- Create user role
```
CREATE USER admin WITH PASSWORD 'password';
```

- Grant necessary privileges for generating primary key values
```
GRANT USAGE, SELECT ON SEQUENCE users_id_seq TO admin;
```

- Admin role can perform all CRUD operations
```
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO admin;
```

# ATTESTATION:

WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.