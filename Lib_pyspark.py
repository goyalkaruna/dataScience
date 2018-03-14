%%writefile cj_features.py
'''
This library extracts the customer journey features 
given the ORC files containing the standard CJ schema
'''
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import pandas as pd
import numpy as np 
import datetime
from datetime import datetime
from dateutil.parser import parse
import time
import sys
import os


sc =SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

### Global Variables ###
COS_BUCKET_NAME="******"
COS_FILE_PATH="******"
COS_ACCESS_KEY="******"
COS_SECRET_ACCESS="******"
COS_ENDPOINT="http://******"
USER_LIST_JSON=[{'user_id':'*****'}, 
                {'user_id':'*****'} 
               ]

TIME_OUT =1800 # Session time out in seconds while computing activity time

### Global Functions ####

def getOrcFilePathForDateRange(bucketName, fromDate, toDate=None):
    '''
    if toDate is None:  then return the expression only for the fromDate
    else return an expression which will load ORC date fromDate to toDate
    '''
    dateList =[]
    if (toDate==None):
        file_path="journey/orc/datestr="+fromDate
    else:
        fromDateStr = datetime.strptime(fromDate, '%Y-%m-%d')
        toDateStr = datetime.strptime(toDate, '%Y-%m-%d')
        if (toDateStr < fromDateStr):
            return "To date should be greater than From Date"
        elif (fromDateStr.year == toDateStr.year):
            noOfMonths = toDateStr.month - fromDateStr.month
            for x in range (fromDateStr.month + 1, toDateStr.month ):
                month = str(x)
                year = str(fromDateStr.year)
                if x<10:
                    monthYear = year +"-0"+month+"-"
                else:
                    monthYear = year +"-"+month+"-"
                newDate = monthYear.strip('\n').replace('\"','')
                dateList.append(newDate)

            for x in range (fromDateStr.day, 32):
                date= str(x)
                month = str(fromDateStr.month)
                year = str(fromDateStr.year)
                if fromDateStr.month<10:
                    fullFromDate = year+"-0"+month+"-"+date
                else:
                    fullFromDate = year+"-"+month+"-"+date 
                dateList.append(fullFromDate)
            
            for y in range (1, toDateStr.day+1):
                date= str(y)
                month = str(toDateStr.month)
                year = str(toDateStr.year)
                if toDateStr.month<10:
                    fullToDate = year+"-0"+month+"-"+date
                else:
                    fullToDate = year+"-"+month+"-"+date 
                dateList.append(fullToDate)
        dateList = ', '.join(dateList)
        file_path="journey/orc/datestr={"+dateList+"}*" 
    return file_path
  
    
def readDataFromCOS(bucket_name,file_path): 
    '''
    Read data from the cos data
    format s3a://<<bucketName>>/<<path>>
    '''
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", COS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", COS_SECRET_ACCESS)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", COS_ENDPOINT)
    df = sqlContext.read.orc("s3a://"+bucket_name+"/"+file_path)
    #df.cache()
    return df
    

def filter_df_with_user_list(user_list, df, event_time='event_timestamp'):
    '''
    Filter data with a list of users
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string]
    '''
    USER_LIST= map(lambda user:user['user_id'], user_list)  
    return df.where(F.col("user_id").isin(USER_LIST)).sort('user_id',event_time)


def compute_event_date(df, event_time='event_timestamp'):
    '''
    Compute event_date from event_timestamp column
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date]
    '''
    return df.withColumn("event_date",F.to_date(event_time))


def compute_week_year(df):
    '''
    Compute yearWeek from event_date column
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string]
    '''
    return df.withColumn("yearWeek",F.concat(F.year('event_date'),F.weekofyear('event_date')))  



def compute_new_time(df, event_time='event_timestamp'):  
    '''
    Create a temporary time column
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp]
    '''
    w = Window().partitionBy('user_id').orderBy(F.col(event_time))
    return df.select("*", F.lead(event_time).over(w).alias("new_time"))


def compute_time_diff(df): 
    '''
    Compute time diff column
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp, diff: biigint]
    '''
    timeFmt = "yyyy-MM-dd HH:mm:ss.SSS"
    return df.withColumn("diff", F.when(F.isnull(F.col("new_time")),0).otherwise(F.unix_timestamp('new_time')-F.unix_timestamp('event_timestamp')))


def compute_time_out(df):
    '''
    Compute time_out flag column
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp, diff: biigint]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp, diff: biigint,time_out: int]
    '''
    return df.withColumn("time_out", F.when((df.diff >= TIME_OUT) | F.isnull(df.new_time), 1).otherwise(0))


def compute_diff_corrected(df):
    '''
    Compute corrected diff column to set diff as 0 for columns with timeout flag as 1
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp, diff: biigint,time_out: int]
    Output DataFrame   - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp, diff: biigint,time_out: int,diff_corrected: bigint]
    '''
    return df.withColumn("diff_corrected", F.when(df.time_out != 1,df.diff).otherwise(0))

def compute_total_time_spent_per_user(df):
    '''
    Compute total time spent by each user
    Expected DataFrame - DataFrame[activity_name: string, event_id: string, event_name: string, event_timestamp: timestamp, offering_name: string, user_id: string, event_date: date, yearWeek: string, new_time: timestamp, diff: biigint,time_out: int,diff_corrected: bigint]
    Output DataFrame   - DataFrame[user_id: string, event_timestamp: timestamp, event_date: date, yearWeek: string, total_time: bigint]
    '''
    w = Window.partitionBy('user_id').rowsBetween(-sys.maxsize -1,0)
    return df.select('*',F.sum("diff_corrected").over(w).alias('total_time')).sort('user_id','event_timestamp')


def compute_user_activity_start_date(df):  
    '''
    Compute uesr activity start date
    Expected DataFrame - DataFrame[user_id: string, event_timestamp: timestamp, event_date: date, yearWeek: string, total_time: bigint]
    Output DataFrame   - DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint]
    '''
    return  df.groupBy('user_id').agg(F.min('event_timestamp').alias('activity_start_date'),F.max('total_time').alias('total_time'),F.countDistinct('event_date').alias('total_active_days'),F.countDistinct('yearWeek').alias('total_active_weeks'))
    

def no_of_days_since_start_date(df):
    # Need to clarify. Should we compare it till today ?? If the data set is only for certain period this might not give the right number ?    
    '''
    Compute number of days passed till today since the user has started an activity
    Expected DataFrame - DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint]
    Output DataFrame   - DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint, days_since_start_date: int]
    '''
    computeBusinessDays = F.udf(lambda date: len(pd.bdate_range(date,pd.to_datetime('today')).tolist()), IntegerType())
    df =  df.withColumn('days_since_start_date',computeBusinessDays(F.col('activity_start_date')))
    return df.select('user_id','days_since_start_date')



def compute_avg_hours_spent_per_day_since_start_date_active(df, avg_hspd='avg_hours_spent_per_day'):
    '''
    Compute average hours spent per day 
    avg_hours_spent_per_day = total_time/total_active_days
    Expected DataFrame - DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint]
    Output DataFrame   - DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint, avg_hours_spent_per_day: string]
    '''
    df = df.withColumn(avg_hspd ,F.round((F.col('total_time')/F.col('total_active_days'))/3600,5))
    return df.select('user_id',avg_hspd)


def compute_avg_hours_spent_per_week_since_start_date_active(df, avg_hspw='avg_hours_spent_per_week', total_active_weeks='total_active_weeks'):
    '''
    Compute average hours spent per week 
    avg_hours_spent_per_day = total_time/total_active_weeks
    Expected DataFrame -  DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint]
    Output DataFrame   -  DataFrame[user_id: string, activity_start_date: timestamp, total_time: bigint, total_active_days: bigint, total_active_weeks: bigint, days_since_start_date: int, avg_hours_spent_per_week: string]
    '''
    df = df.withColumn(avg_hspw, F.round((F.col('total_time')/F.col(total_active_weeks))/3600,5))
    return df.select('user_id',avg_hspw)

def compute_deviation_total_time_per_day(df):
    '''
    Compute std deviation for total time spent per day by user
    Expected DataFrame - 
    Output DataFrame   - 
    '''
    w = Window.partitionBy('user_id','event_date').orderBy('event_date')
    df_total_time_per_day = df.select('*',F.sum("diff_corrected").over(w).alias('total_time_per_day')).sort('user_id','event_timestamp')
    df_max_total_tpd = df_total_time_per_day.groupBy('user_id','event_date').agg(F.max('total_time_per_day').alias('total_time_per_day')).sort('user_id','event_date')
    w2_calc_std = Window.partitionBy('user_id')
    df_std_total_time = df_max_total_tpd.groupBy('user_id','total_time_per_day').agg(F.stddev("total_time_per_day").over(w2_calc_std).alias("Std_total_time_per_day"))
    return (df_std_total_time.groupBy('user_id','Std_total_time_per_day').agg(F.round(F.when(F.isnan(F.col("Std_total_time_per_day")),0).otherwise(F.col("Std_total_time_per_day")),4).alias("Std_total_tpd"))).select('user_id','Std_total_tpd')

def compute_deviation_total_time_per_week(df, yearWeek='yearWeek', event_timestamp='event_timestamp'):
    '''
    Compute std deviation for total time spent per week by user
    Expected DataFrame - 
    Output DataFrame   - 
    '''
    w = Window.partitionBy('user_id',yearWeek).orderBy(yearWeek)
    df_total_time_per_week = df.select('*',F.sum("diff_corrected").over(w).alias('total_time_per_week')).sort('user_id',event_timestamp)
    df_max_total_tpw = df_total_time_per_week.groupBy('user_id',yearWeek).agg(F.max('total_time_per_week').alias('total_time_per_week')).sort('user_id',yearWeek)
    w2_calc_std = Window.partitionBy('user_id')
    df_std_total_time = df_max_total_tpw.groupBy('user_id','total_time_per_week').agg(F.stddev("total_time_per_week").over(w2_calc_std).alias("Std_total_time_per_week"))
    return (df_std_total_time.groupBy('user_id','Std_total_time_per_week').agg(F.round(F.when(F.isnan(F.col("Std_total_time_per_week")),0).otherwise(F.col("Std_total_time_per_week")),4).alias("Std_total_tpw"))).select('user_id','Std_total_tpw')
    


def checkRequiredColumns(current_col_list,list_req_col):
    '''
    Check if the requred column names are prensent in the given dataframe
    '''
    return set(list_req_col).issubset(set(current_col_list))


def total_active_days(df, event_date='event_date'):
    '''
    Compute total active days for every user
    '''
    return df.groupBy('user_id',event_date).agg(F.count(event_date).alias('nDaysActive')).select('user_id','nDaysActive')
    

def total_active_weeks(df, yearWeek='yearWeek'):
    '''
    Compute total active weeks for every user
    '''
    return df.groupBy('user_id',yearWeek).agg(F.count(yearWeek).alias('nWeeksActive')).select('user_id','nWeeksActive')


def compute_avg_sd_active_days_a_week(df, yearWeek='yearWeek'):
    '''
    Compute avg no of days a user is active and its Standard Deviation 
    return df.select("user_id",F.avg('nDays').over(w).alias('andw'),F.stddev('nDays').over(w).alias('sdfandw'))
    '''
    df=df.groupBy('user_id',yearWeek).agg(F.countDistinct('event_date').alias('nDays'))
    w = Window.partitionBy('user_id')
    #return df.select("user_id",F.avg('nDays').over(w).alias('andw'),F.when(F.isnan(F.stddev('nDays').over(w).alias('sdfandw')),0).otherwise(F.stddev('nDays').over(w)).alias('sdfandw'))
    return df.select("user_id",F.avg('nDays').over(w).alias('andw'),F.stddev('nDays').over(w).alias('sdfandw'))

def computeAvgHoursBetweenMilestones(df):
    milestone_df = df.filter(df.event_name=='milestone')
    milestone_totals_df = compute_total_time_spent_per_user(milestone_df)
    milestone_totals_df = milestone_totals_df.groupby('user_id').agg(F.max('total_time').alias('total_milestone_hours'),F.count('event_name').alias('total_milestone_events'))
    return milestone_totals_df.withColumn('anhbm',F.format_number(milestone_totals_df.total_milestone_hours/milestone_totals_df.total_milestone_events,3).cast('float')).select('user_id','anhbm')


def computeNoOfTimesSupportPageVisited(df):
    uniq_activityName = df.select('activity_name').distinct().sort('activity_name')
    # uniq_activityName.show(20000,False)
    filtr_support_activity = df.select('*').where("activity_name like '%Support%'")
    # filtr_support_activity.show(100,False)
    #For every user; count the number of times he visits the support page
    return filtr_support_activity.groupBy('user_id').count().alias("nSupportPageVisits")
    #If we want to filter based on a particular user
    #filter based on user: 595910b7-45af-4d3f-a8ef-0951b4cbb444
    # user_supportpage_visit=userS_supportpage_visit.filter(filtr_support_activity.user_id == '595910b7-45af-4d3f-a8ef-0951b4cbb444' )
    # output_df.printSchema();

def computeTotalDaysNotLoggedIn(df):
    au15_datecol = df.withColumn('justdate', F.date_format('event_timestamp', 'MM/dd/yyy').alias('date'))
    au15 = au15_datecol.groupby(au15_datecol.user_id).agg(F.countDistinct("justdate").alias('loggedInDuration'),F.min("event_timestamp"),F.datediff(F.date_format(F.current_date(),"yyyy-MM-dd HH:mm:ss.SSS"),F.min("event_timestamp")).alias("interval"))
    au15 = au15.withColumn('totalDuration', au15.interval + 1)
    totalDaysNotLoggedIn = au15.withColumn('nDaysOut', au15.totalDuration - au15.loggedInDuration)
    return totalDaysNotLoggedIn.select('user_id','nDaysOut')

def computeavgEventsPeWeek(df): 
    au8 = df.groupBy("user_id").agg(F.count("user_id").alias("totalEvents"),F.countDistinct('yearWeek').alias('total_active_weeks'))
    avgEperWeek = au8.withColumn('avgEventsPerWeek',au8.totalEvents/au8.total_active_weeks)
    avgEperWeek = avgEperWeek.withColumn("avgEventsPerWeek", F.round(avgEperWeek["avgEventsPerWeek"],1))
    return avgEperWeek.select('user_id','avgEventsPerWeek')

def compueAvgEventsPerDay(df):
    au7 = df.groupBy("user_id").agg(F.count("user_id").alias("totalEvents"),F.countDistinct('event_date').alias('total_active_days'))
    avgEperDay = au7.withColumn('avgEventsPerDay',au7.totalEvents/au7.total_active_days)
    avgEperDay = avgEperDay.withColumn("avgEventsPerDay", F.round(avgEperDay["avgEventsPerDay"],1))
    #avgEperDay.show(1000,False)
    return avgEperDay.select('user_id','avgEventsPerDay')

def filterTopNRankValuesInACol(df,primary_key,threshold_col,threshold_val):
    input_col = df.columns
    window = Window.partitionBy().orderBy(df[threshold_col].desc())
    return df.select('*', F.rank().over(window).alias('rank')).filter(F.col('rank') <= threshold_val).drop('rank')


flag_af=False
flag_au=False
flag_wc=False
def extractFeatures(flag_af, flag_au, flag_wc, USER_LIST_JSON, master_df):
    if flag_au == True:
        #df_user_filtered = filter_df_with_user_list(USER_LIST_JSON,master_df)
        df_with_event_date = compute_event_date(master_df)
        df_with_week_year = compute_week_year(df_with_event_date)
        df_with_new_time = compute_new_time(df_with_week_year)
        df_with_time_diff = compute_time_diff(df_with_new_time)
        df_with_time_out = compute_time_out(df_with_time_diff)
        df_with_diff_corrected = compute_diff_corrected(df_with_time_out)
        df_with_total_time_per_user = compute_total_time_spent_per_user(df_with_diff_corrected)
        df_with_activity_start_date = compute_user_activity_start_date(df_with_total_time_per_user)
        '''
        au1 - Number of business days since start date (excluding weekends)
        '''
        df_with_days_since_start_date_column = no_of_days_since_start_date(df_with_activity_start_date)
        output_df =  df_with_days_since_start_date_column
        '''
        au2 - Avg hours spent per day
        '''
        df_au2 = compute_avg_hours_spent_per_day_since_start_date_active(df_with_activity_start_date)
        if checkRequiredColumns(output_df.columns,df_au2.columns) == False:
            output_df = output_df.join(df_au2,"user_id","fullouter") 
        '''
        au3 - Avg hours spent per week
        '''
        df_au3 = compute_avg_hours_spent_per_week_since_start_date_active(df_with_activity_start_date)
        if checkRequiredColumns(output_df.columns,df_au3.columns) == False:
            output_df = output_df.join(df_au3,"user_id","fullouter")
        '''
        au4 - Average number of hours spent between milestones
        '''
        df_au4 = computeAvgHoursBetweenMilestones(df_with_diff_corrected)
        if checkRequiredColumns(output_df.columns,df_au4.columns) == False:
            output_df = output_df.join(df_au4,"user_id","fullouter") 
        '''
        au7 - average number of events per day = total number of events / total active days
        '''
        df_au7 = compueAvgEventsPerDay(df_with_event_date)
        if checkRequiredColumns(output_df.columns,df_au7.columns) == False:
            output_df = output_df.join(df_au7,"user_id","fullouter")
        '''
        au8 - average number of events per week = total number of events / total active weeks
        '''
        df_au8 = computeavgEventsPeWeek(df_with_week_year)
        if checkRequiredColumns(output_df.columns,df_au8.columns) == False:
            output_df = output_df.join(df_au8,"user_id","fullouter") 
        '''
        au15 - Total number of days not Logged In
        '''
        df_au15 = computeTotalDaysNotLoggedIn(master_df) 
        if checkRequiredColumns(output_df.columns,df_au15.columns) == False:
            output_df = output_df.join(df_au15,"user_id","fullouter")
        '''
        au16 - To find the number of times the user viewed support pages
        '''
        df_au16 = computeNoOfTimesSupportPageVisited(master_df)    
        if checkRequiredColumns(output_df.columns,df_au16.columns) == False:
            output_df = output_df.join(df_au16,"user_id","fullouter")
        '''
        au17 - Total number of days a user is active
        '''
        df_au17 = total_active_days(df_with_week_year)
        if checkRequiredColumns(output_df.columns,df_au17.columns) == False:
            output_df = output_df.join(df_au17,"user_id","fullouter")
        '''
        au18 - Total number of weeks a user is active
        '''
        df_au18 = total_active_weeks(df_with_week_year)
        if checkRequiredColumns(output_df.columns,df_au18.columns) == False:
            output_df = output_df.join(df_au18,"user_id","fullouter")
        '''
        au19 - Standard deviation for avg hours spent per day
        '''
        std_total_time_per_day = compute_deviation_total_time_per_day(df_with_total_time_per_user)
        if checkRequiredColumns(output_df.columns,std_total_time_per_day.columns) == False:
            output_df = output_df.join(std_total_time_per_day,"user_id","fullouter")
        '''
        au20 - Standard deviation for avg hours spent per week
        '''
        std_total_time_per_week = compute_deviation_total_time_per_week(df_with_total_time_per_user)
        if checkRequiredColumns(output_df.columns,std_total_time_per_week.columns) == False:
            output_df = output_df.join(std_total_time_per_week,"user_id","fullouter")
        '''
        au22 and au23 - avg and sd of active days in a week
        '''    
        df_au22_23 = compute_avg_sd_active_days_a_week(df_with_week_year)
        if checkRequiredColumns(output_df.columns,df_au22_23.columns) == False:
            output_df = output_df.join(df_au22_23,"user_id","fullouter") 
        #df = output_df.selectExpr("avg_hours_spent_per_day as au2", "avg_hours_spent_per_week as au3" ,"Std_total_tpd as sdpd", "Std_total_tpw as sdpw")  
        return output_df
    
    elif flag_wc==True:
        #df_user_filtered = filter_df_with_user_list(USER_LIST_JSON,master_df)
        watsonConv_data = master_df.filter(master_df.offering_name == 'Conversation' )
        watsonConv_data.show(20,False)
        '''
        wc10 - State of service (Free or Paid)
        ''' 
        df_wc10 = watsonConv_data.where(watsonConv_data.event_name.isin(['Added New Service/Free','Added New Service/Paid'])).groupBy('event_name').count()
        df_wc10.show(20,False)
        '''
        wc11 : No of milestones achived by each user
        Find all the users who have completed milestones
        '''
        milestone_data=watsonConv_data.filter(watsonConv_data.event_name == 'milestone' )
        milestone_data.show(10,False)
        '''
        wc1,wc2,wc3 Total number of intents created,Number of Dialog Nodes created & Number of entities created by each user for Watson Coverstation service with event name as 'milestone'
        '''
        watsonConv_by_user_by_activities = milestone_data.groupby('user_id','activity_name').count()
        watsonConv_by_user_by_activities.show(20,False)
        '''
        wc1,wc2,wc3 Total number of intents created,Number of Dialog Nodes created & Number of entities created overall for Watson Coverstation service with event name as 'milestone'
        '''
        watsonConv_by_activities = milestone_data.groupby('activity_name').count()
        watsonConv_by_activities.show(20,False)
        '''
        wc11: find the number of milestones completed by each user
        '''
        milestone_count = milestone_data.groupBy('user_id').count()
        milestone_count.show(100,False)
        return milestone_count




## Testing the above Library

# Every time you change something would have to restart the Kernel to reload the import 
from cj_features import * 
file_path = getOrcFilePathForDateRange(COS_BUCKET_NAME, "2017-05-10", "2017-08-12")
print(file_path)
master_df = readDataFromCOS(COS_BUCKET_NAME,file_path)
master_df.printSchema()

df = extractFeatures(False, True, False, USER_LIST_JSON, master_df)
df.printSchema()
