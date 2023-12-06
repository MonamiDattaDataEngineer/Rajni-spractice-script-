"""
Project          : Maintenance Dashboard
Developer        : Rajni Danu
Glue Job Name    : msil_transformed_conditional_recheck_ok_not_ok_daily_detailed_report
Purpose          : Reads data from curated layer(S3) and apply transformations as per business logic to load final report in Redshift.
Glue Job Type    : PySpark Job
Python Version   : 3.9
Created Date     : 04 July 2023
Version          : 1.0
"""



import os
import sys
import time
import s3fs
import boto3 
import traceback
import pyspark
from datetime import date,datetime,timedelta
from delta.tables import *
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.types as sqltypes
from awsglue.utils import getResolvedOptions
from dateutil.relativedelta import relativedelta
import transformed_functions as tf
from datetime import datetime, timedelta
from pyspark.sql.functions import col 
from pyspark.sql.functions import regexp_replace


print(pyspark.__version__)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
fs = s3fs.S3FileSystem(anon=False)
spark = tf.InitiateSparkSession()


#Name of the dashboard, used to get config information
#project_name = 'Maintenance'
#dashboard_name = 'Maintenance'
report_name = 'conditional_recheck_ok_not_ok_daily_detailed_report'

#Flag to be used to changed in case of a history run
history_load = True 
print(f'History Load - {history_load}')


'''
All the output reports are given here
with their respective values.
Will change according to dashboard

- primary_keys - Primary Keys for the output data in S3 
- refresh_type - Refresh Type for the data in S3 
    Options - 'replace' (or) 'merge'
- merge_type - if refresh_type is merge
    Options - Check in Function MergeOutputs 
- redshift_refresh_type' : 'truncate_load'
- redshift_schema' : 'transformed_dev'
- partition_columns' : ['YEAR', 'MONTH']

'''
#Output table/object details
output_details = {'conditional_recheck_ok_not_ok_daily_detailed_report': 
    {
    'primary_keys': []
    ,'refresh_type': 'replace'
    ,'merge_type' : ''
    ,'redshift_refresh_type' : 'truncate_load'
    ,'redshift_schema' : 'transformed_dev'
    ,'partition_columns' : []
    }
}

def PrintFailure():
    '''
    Function to display the complete error stack
    '''
    exception_type, exception_value, trace = sys.exc_info()
    print(f'Exception Type : \n{exception_type}')
    print(f'Exception Value : \n{exception_value}')
    trace_str = ''.join(traceback.format_tb(trace))
    print(f'Stack Trace : \n{trace_str}')
    project_name = 'MSIL Curated Redshift'
    resource_name = 'AWS Glue'
    job_name = 'msil_transformed_conditional_recheck_ok_not_ok_daily_detailed_report'
    status = 'Failed'
    description = 'Failed when executing the tranformed job - msil_transformed_conditional_recheck_ok_not_ok_daily_detailed_report'
    topicArn = 'arn:aws:sns:ap-south-1:376039757673:msil_dwh_notification'
    subject = 'msil_transformed_conditional_recheck_ok_not_ok_daily_detailed_report Failed'
    #tf.SendSNS(project_name, resource_name, job_name, status, description, trace_str, topicArn, subject)
    

def ApplyTransformations(all_table_dfs):
    '''
    Transformation logics for all reports
    are to be build in this function
    Input Required - Dictionary of all dataframes
    '''
    
    '''
    Total Reports/Outputs - 1
    Input Tables - 5
    Names Of Reports 
        - conditional_recheck_ok_not_ok_daily_detailed_report
    '''
    print('Started building transformations')
    globals().update(all_table_dfs)
    try:
        #Logic for Report 1 Starts
        '''
        Report 1 - conditional_recheck_ok_not_ok_daily_detailed_report
        Output DF Name - conditional_recheck_ok_not_ok_daily_detailed_report_DF
        '''
        global mmat_mndp_DF
        global mmat_mncw_DF
        global mmat_mnec_DF
        global mmat_mneq_DF
        
        #  filter data in mmat_mndp_DF &  mmat_mncw_DF
        mmat_mndp_DF = mmat_mndp_DF.select('MNDP_CODE', 'MNDP_COMP_CODE','MNDP_DEPT_CODE','MNDP_START_DATE','MNDP_CLOSE_DATE').distinct()
        mmat_mncw_DF = mmat_mncw_DF.withColumn('MNCW_MNEQ_CODE_MOD' , substring('MNCW_MNEQ_CODE',1,3))
        mmat_mncw_DF = mmat_mncw_DF.where(col('MNCW_STATUS').isin('SNC', 'DATC', 'DAMC','SNAC'))
        #mmat_mncw_DF.select('MNCW_STATUS').distinct().show()
        mmat_mnec_DF = mmat_mnec_DF.select('MNEC_CHKPOINT_DESC','MNEC_COMP_CODE','MNEC_MNEQ_CODE','MNEC_CHKPOINT_CODE')
        mmat_mneq_DF = mmat_mneq_DF.select('MNEQ_DESC','MNEQ_COMP_CODE','MNEQ_CODE')
        
        
        # main cursor and all joins
        current_date = datetime.utcnow()
        next_date = (datetime.now() + timedelta(1)) 
        mmat_mndp_DF= mmat_mndp_DF.filter((col('MNDP_START_DATE') <= current_date) 
                                          & (coalesce(col('MNDP_CLOSE_DATE'), lit(next_date)) >= current_date))
        mmat_mndp_DF.count()
        REP_MNRCHKNRML_DF = mmat_mncw_DF.join(mmat_mndp_DF, ( (mmat_mndp_DF.MNDP_COMP_CODE == mmat_mncw_DF.MNCW_COMP_CODE)
                                                            & (mmat_mndp_DF.MNDP_CODE == mmat_mncw_DF.MNCW_MNEQ_CODE_MOD)),'inner')\
                                        .select(                                   
                                            col('MNCW_COMP_CODE').alias('COMPANY_CODE') ,
                                            col('MNCW_DONE_ON') ,
                                            col('MNCW_GROUP').alias('GROUP') ,
                                            col('MNDP_DEPT_CODE').alias('DEPARTMENT') ,
                                            col('MNCW_STATUS') ,
                                            substring(col('MNCW_MNEQ_CODE'),1,3).alias('SHOP') ,
                                            col('MNCW_MNEQ_CODE').alias('EQUIPMENT_CODE') ,
                                            col('MNCW_HTH_CHK1').alias('HEALTH_CONDITION1') ,
                                            col('MNCW_HTH_CHK2').alias('HEALTH_CONDITION2') ,
                                            col('MNCW_MNEC_CHKPOINT_CODE').alias('ELEMENT') ,
                                            col('MNCW_CHK1_FREQ1').alias('MOTOR1') ,
                                            col('MNCW_CHK1_FREQ2').alias('DRIVEN_EQUIPMENT1') ,
                                            col('MNCW_CHK1_FREQ3') ,
                                            col('MNCW_CHK1_FREQ4') ,
                                            col('MNCW_CHK1_FREQ5') ,
                                            col('MNCW_CHK1_FREQ6') ,
                                            col('MNCW_CHK2_FREQ1').alias('MOTOR2') ,
                                            col('MNCW_CHK2_FREQ2').alias('DRIVEN_EQUIPMENT2') ,
                                            col('MNCW_CHK2_FREQ3') ,
                                            col('MNCW_CHK2_FREQ4') ,
                                            col('MNCW_CHK2_FREQ5') ,
                                            col('MNCW_CHK2_FREQ6') ,
                                            regexp_replace(col("MNCW_ANALYSIS"), "\n|\t", " ").alias('ANALYSIS') ,
                                            regexp_replace(col("MNCW_COUNTERMEASURE1"), "\n|\t", " ").alias('COUNTER_MEASURE') ,
                                            col('MNCW_START_TIME').alias('CHECKING_DATE1') ,
                                            col('MNCW_CLOSE_DATE').alias('CHECKING_DATE2') ,
                                            col('MNCW_HTH_CHK2').alias('PRESENT_STATUS') ,
                                            col('MNCW_REMARKS').alias('REMARKS') ,
                                            col('MNCW_END_TIME') ,
                                            col('MNCW_MAN_HOURS').alias('HOURS') ,
                                            col('MNCW_WORK_DONE_DETAILS').alias('WORKDONE') ,
                                            col('MNCW_WORK_ORDER_NO') ,
                                            substring(col('MNCW_MNEF_MNCM_CODE'),5,1).alias('MNCM_CODE')
                                            )
        REP_MNRCHKNRML_DF =REP_MNRCHKNRML_DF.join(mmat_mneq_DF, ((mmat_mneq_DF.MNEQ_COMP_CODE == REP_MNRCHKNRML_DF.COMPANY_CODE)
                                                    & (mmat_mneq_DF.MNEQ_CODE == REP_MNRCHKNRML_DF.EQUIPMENT_CODE)) ,'left')
        REP_MNRCHKNRML_DF =REP_MNRCHKNRML_DF.join(mmat_mnec_DF, ((mmat_mnec_DF.MNEC_COMP_CODE == REP_MNRCHKNRML_DF.COMPANY_CODE)
                                                        & (mmat_mnec_DF.MNEC_MNEQ_CODE == REP_MNRCHKNRML_DF.EQUIPMENT_CODE)
                                                        & (mmat_mnec_DF.MNEC_CHKPOINT_CODE == REP_MNRCHKNRML_DF.ELEMENT)) ,'left')
        REP_MNRCHKNRML_DF = REP_MNRCHKNRML_DF.withColumn("REPORT_TYPE" , when(col('MNCW_STATUS') == 'SNC', "RECHECK OK DETAIL REPORT").otherwise("RECHECK NOT OK DETAIL REPORT"))
        REP_MNRCHKNRML_DF = REP_MNRCHKNRML_DF.select(
                                                    col('COMPANY_CODE') ,
                                                    col('DEPARTMENT') ,
                                                    col('MNCW_DONE_ON').alias('DONE_DATE') ,
                                                    col('GROUP') ,
                                                    col('REPORT_TYPE') ,
                                                    col('MNCW_WORK_ORDER_NO').alias('WORK_ORDER_NO.') ,
                                                    col('SHOP').alias('SHOP') ,
                                                    col('EQUIPMENT_CODE').alias('EQUIPMENT_CODE') ,
                                                    regexp_replace(col("MNEQ_DESC"), "\n|\t", " ").alias('EQUIPMENT_NAME') ,
                                                    col('ELEMENT').alias('ELEMENT_CODE') ,
                                                    regexp_replace(col("MNEC_CHKPOINT_DESC"), "\n|\t", " ").alias('ELEMENT_NAME') ,
                                                    col('MOTOR1').alias('PRE_MOTOR/JT-1') ,
                                                    col('DRIVEN_EQUIPMENT1').alias('PRE_DRIVEN_EQUIPMENT/JT-2') ,
                                                    col('MNCW_CHK1_FREQ3').alias('PRE_JT-3') ,
                                                    col('MNCW_CHK1_FREQ4').alias('PRE_JT-4') ,
                                                    col('MNCW_CHK1_FREQ5').alias('PRE_JT-5') ,
                                                    col('MNCW_CHK1_FREQ6').alias('PRE_JT-6') ,
                                                    col('HEALTH_CONDITION1').alias('PRE_HEALTH_CONDITION') ,
                                                    col('CHECKING_DATE1').alias('PRE_CHECKING_DATE') ,
                                                    col('MOTOR2').alias('POST_MOTOR/JT-1') ,
                                                    col('DRIVEN_EQUIPMENT2').alias('POST_DRIVEN_EQUIPMENT/JT-2') ,
                                                    col('MNCW_CHK2_FREQ3').alias('POST_JT-3') ,
                                                    col('MNCW_CHK2_FREQ4').alias('POST_JT-4') ,
                                                    col('MNCW_CHK2_FREQ5').alias('POST_JT-5') ,
                                                    col('MNCW_CHK2_FREQ6').alias('POST_JT-6') ,
                                                    col('HEALTH_CONDITION2').alias('POST_HEALTH_CONDITION') ,
                                                    col('CHECKING_DATE2').alias('POST_CHECKING_DATE') ,
                                                    col('WORKDONE').alias('WORK_DONE_DETAIL') ,
                                                    col('MNCW_END_TIME').alias('ACTION_TAKEN_DATE') ,
                                                    col('HOURS').alias('MAN_HOURS') ,
                                                    col('REMARKS').alias('REMARKS')
                                                    ).distinct()
        
        

        # TODO check date filter with user and adjust as required. min 2010-10-18 00:00:00
        # cursor_1_DF = cursor_1_DF.filter(col('DUE_DATE') == '2020-04-01')
        conditional_recheck_ok_not_ok_daily_detailed_report_DF =  REP_MNRCHKNRML_DF
        
        '''
        The final dictionary with output/report 
        names with the dataframe object
        '''
        all_output_dfs = {'conditional_recheck_ok_not_ok_daily_detailed_report_DF' : conditional_recheck_ok_not_ok_daily_detailed_report_DF}
        
        '''
        SQL Queries for each output
        '''
        conditional_recheck_ok_not_ok_daily_detailed_report_query  = 'no_query'
        conditional_recheck_ok_not_ok_daily_detailed_report_add_path  = 'no_path'
        all_output_queries = {'conditional_recheck_ok_not_ok_daily_detailed_report_query' : conditional_recheck_ok_not_ok_daily_detailed_report_query ,
                            'conditional_recheck_ok_not_ok_daily_detailed_report_add_path' : conditional_recheck_ok_not_ok_daily_detailed_report_add_path
                            }
        
        #df_descriptions = {}
        ##To print logical plan for all outputs, uncomment for debugging
        #for df_name,df in all_output_dfs.items():
        #    df.explain(True) 
        
        ##To print Schema for all outputs, uncomment for debugging
        #for df_name,df in all_output_dfs.items():
        #    df.printSchema()
        
        ##To compute and print counts for all outputs, uncomment for debugging
        #for df_name,df in all_output_dfs.items():
        #    print(df.count())
         
        ##To compute and show sample for all outputs, uncomment for debugging
        #for df_name,df in all_output_dfs.items():
        #    df.show(10)
        print('Completed building transformations')
        return all_output_dfs, all_output_queries
    except Exception as e:
        print('Error occured in Apply Transformations')
        print('Error occur while applying transformations.')
        PrintFailure()
        raise Exception(f'Error occur while applying transformations. {e}')
    
    
    
def MergeLogic(target_df, output_df, primary_keys, partition_columns):
    try:
        ##Insert Logic to join the output dataframe and the deltatable already present in output, should return a spark dataframe
        pass
        ##End Logic
        return df
    except Exception as e:
        print('Error occured in Merge Logic')
        print('Error occur while executing Merge Logic')
        PrintFailure()
        raise Exception(f'Error occur while executing Merge Logic. {e}')


def MergeOutputs(output_df_name, output_path, output_df, primary_keys, partition_columns):
    '''
    Merge Type Options - 
        Use the required key word from below options
        
        1. add_rows_upsert - Upsert rows based on primary key
        2. add_rows - Append rows
        3. process - Calculate/Reprocess
    '''
    merge_type = output_details[output_df_name]['merge_type']
    try:
        if merge_type == 'add_rows_upsert':
            deltaTable = DeltaTable.forPath(spark, output_path)
            #Building Merge condition
            merge_condition = ' and '.join([f'delta.{primary_key} = output.{primary_key}' for primary_key in primary_keys])
            #Executing merge statment.
            output_df.drop_duplicates(primary_keys)
            deltaTable.alias('delta') \
                .merge(output_df.alias('output'), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            deltaTable = DeltaTable.forPath(spark, output_path)
            deltaTable.generate("symlink_format_manifest")
            return 1
        elif merge_type == 'add_rows':
            if len(partition_columns) > 0: 
                output_df.write\
                    .mode("append")\
                    .format("delta")\
                    .partitionBy(*partition_columns)\
                    .save(output_path)
            else:
                output_df.write\
                    .mode("append")\
                    .format("delta")\
                    .save(output_path)
            deltaTable = DeltaTable.forPath(spark, output_path)
            deltaTable.generate("symlink_format_manifest")
            return 1
        elif merge_type == 'process':
            deltaTable = DeltaTable.forPath(spark, output_path)
            target_df = deltaTable.toDF()
            final_df = MergeLogic(target_df, output_df, primary_keys, partition_columns)
            deltaTable = DeltaTable.forPath(spark, output_path)
            deltaTable.generate("symlink_format_manifest")
            return final_df
        else:
            print(f'Please select a valid merge type')
            exit(1)
    except Exception as e:
        print('Error occured in Merging Outputs')
        print('Error occur while Merging Outputs')
        PrintFailure()
        raise Exception(f'Error occur while Merging Outputs. {e}')


def ReplaceWriteS3(output_path, partition_columns, output_df_name, output_df):
    '''
    This function will write a dataframe to the given path
    '''
    try:
        os.system(f'aws s3 rm {output_path} --recursive --quiet')
        if len(partition_columns) > 0:
            print(f'Full replace in transformed layer for {output_df_name}')
            #data.write.format("delta").save("/tmp/delta-table")   
            output_df.write \
            .mode("overwrite") \
            .format("delta") \
            .partitionBy(*partition_columns) \
            .save(output_path)
        else:
            print(f'Full replace in transformed layer for {output_df_name}')
            output_df.write \
            .mode("overwrite") \
            .format("delta") \
            .save(output_path)
            
        deltaTable = DeltaTable.forPath(spark, output_path)
        deltaTable.generate("symlink_format_manifest")
    except Exception as e:
        print('Error occured in ReplaceWriteS3')
        print('Error occur while writing the output to S3.')
        PrintFailure()
        raise Exception('Error occur while writing the output to S3.')
        

def RefreshOutputs(history_load,output_details,all_output_dfs, transformed_bucket_name):
    '''
    The data will be refreshed in the Transformed Layer based on the output type
    If not pre-compiled, the transformations will be executed before writing the file to output
    Input Required - Dictionary of all output dataframes
    '''
    try:
        output_bucket = transformed_bucket_name
        
        for output_df_name,output_df in all_output_dfs.items():
            print(f'Starting output process for {output_df_name}')
            output_df_name = output_df_name.replace('_DF','')
            primary_keys = output_details[output_df_name]['primary_keys']
            refresh_type = output_details[output_df_name]['refresh_type']
            partition_columns = output_details[output_df_name]['partition_columns']
            print(f'Output file Schema - {output_df_name}')
            output_df.printSchema()
            output_path = f"s3://{output_bucket}/{output_df_name}"
            print(f'Output Path - {output_path}')
            path_exists = fs.exists(output_path)
            print(f'{output_path} Folder Exists - {path_exists}')
            #Check if a full replace is required
            if history_load or refresh_type == 'replace' or path_exists == False:
                ReplaceWriteS3(output_path, partition_columns,output_df_name, output_df)
            #Merge with the existing table in the Transformed bucket
            elif refresh_type == 'merge':
                print(f'Merge in transformed layer for {output_df_name}')
                final_df = MergeOutputs(output_df_name, output_path, output_df, primary_keys, partition_columns)
                if final_df != 1:
                    ReplaceWriteS3(output_path, partition_columns,output_df_name, output_df)
            print(f'Completed output process for {output_df_name}')
    except Exception as e:
        print('Error occured in RefreshOutputs')
        print('Error occur while writing the output to S3.')
        PrintFailure()
        raise Exception('Error occur while writing the output to S3.')

        

#Function calls

start = time.time()
input_table_details, transformed_bucket_name = tf.FetchConfigDetails(report_name)
print(f'Time taken to Read the Config {time.time() - start}')


start = time.time()
all_table_dfs = tf.ReadFiles(history_load, input_table_details, transformed_bucket_name)
print(f'Time taken to Read the files {time.time() - start}')

start = time.time()
all_output_dfs, all_output_queries = ApplyTransformations(all_table_dfs)
print(f'Time taken to ApplyTransformations {time.time() - start}')

start = time.time()
RefreshOutputs(history_load, output_details, all_output_dfs, transformed_bucket_name)
print(f'Time taken to RefreshOutputs {time.time() - start}')


start = time.time()
tf.LoadToRedshift(output_details, all_output_queries, transformed_bucket_name)
print(f'Time taken to Trigger Redshift jobs {time.time() - start}')

print('Completed')