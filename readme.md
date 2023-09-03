# **Build ETL Pipeline on AWS Coud**
# **overview**
&emsp; The AWS COVID-19 Data Analysis project is a comprehensive endeavor aimed at leveraging the power of Amazon Web Services (AWS) to collect, store, process, and analyze data related to the COVID-19 pandemic. The project combines cloud computing technology with critical health data to gain insights into the pandemic's impact, track its progression, and contribute to informed decision-making.
## **Key Objectives:**

1. **Data Collection:** Set up data pipelines to collect COVID-19 data from various sources such as government health agencies, international organizations, and research institutions. AWS services like AWS Glue and Amazon S3 can be used to efficiently manage and store this data.

2. **Data Cleaning and Transformation:** Prepare the collected data for analysis by cleaning, transforming, and structuring it into a format suitable for analysis. AWS Glue, Amazon Athena, and AWS Data Pipeline can be employed for these tasks.

3. **Data Analysis:** Utilize AWS services like Amazon Redshift to perform advanced analytics on the COVID-19 data. This can include trend analysis, predictive modeling, and identifying geographical hotspots.

4. **Data Visualization:** Create visually compelling and informative dashboards using Amazon QuickSight or other visualization tools to present the analysis results in a user-friendly format.
<br>

## **Application Architecture**
**Application Architecture used**
1. Aws S3
2. AWS Glue
3. AWS Athena
4. AWS Redshift
5. AWS IAM

**Programming Language**
* Python

### **Modules**
* [boto3](https://pypi.org/project/boto3)\
 &emsp; Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) for Python, which allows Python developers to write software that makes use of services like Amazon S3 and Amazon EC2
* [pandas](https://pypi.org/project/pandas/)\
  &emsp; pandas is a Python package that provides fast, flexible, and expressive data structures designed to make working with "relational" or "labeled" data
* [redshift-connector](https://pypi.org/project/redshift-connector/)\
  &emsp;is a Python library that provides a connector for Amazon Redshift. It allows easy integration with pandas and numpy, as well as support for numerous Amazon Redshift specific features, IAM authentication, specific datatypes.

## **Table Scehma**
### &emsp; **Data Model**
&emsp; ![](image\data_model.jpg)

### &emsp; **Work Flow Diagram**
&emsp; ![](image\workflow.jpg)

### &emsp; **Dimesnional Model**
&emsp; ![](image\dimension_model.jpg)
<br>

## **Getting Started**
### &emsp;**Installation**
* boto3
     > pip insall boto3
* redshift_connector 
     >&emsp; pip install redshift_connector
* pandas
     >&emsp; pip isntall pandas

### &emsp;**Prerequisite**
* create S3 buckets
  * To store the metadata and query result of athena. 
  * To store the dimensional model or tables.
  * To store the Data we will be processing.
  * Upload the data to S3 bucket.
    * source of data - https://aws.amazon.com/covid-19-data-lake/
* create a IAM role
  * In this role redshift service is given permission to access the S3 bucket.
* create redshift cluster

## To Start
* Full fill the prerequisite requirenemnt.
* Set the values of variable in config file.
* Run the covid_data_ETL.py file

<br>
<br>
<hr style="height:2px;border:none;color:#333;background-color:#333;">