#!/usr/bin/env python
# coding: utf-8

# In[32]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, trim, col, to_date, max


spark = SparkSession.builder.appName("Job_Search_ETL").getOrCreate()


# In[33]:


df_js_tracker = spark.read.csv("hdfs://hdfs-namenode:9000/job_search_etl/input/job_Search_tracker.csv", header=True, inferSchema=True)


# In[ ]:


# Remove extra null value columns in csv
df = df_js_tracker.drop(*[c for c in df_js_tracker.columns if c.startswith("_c")])
df = df.withColumnRenamed('LINK- JOB POSTING', 'URL')


# In[36]:


(df.columns)


# In[37]:


df.dtypes


# In[38]:


# Remove whitespaces
# trim column names(header) using strip() and use alias to rename the column name
# use trim func from pyspark to trim the values of the column
df = df.select(
    [trim(col(colname)).alias(colname.strip()) for colname in df.columns]
         )


# In[39]:


df.columns


# In[41]:


# testing 'where' api 
where_test = df.select('JOB TITLE',
 'URL',
 'LOCATION',
 'TYPE OF EMPLOYMENT',
 'MODE OF WORK').where(col('MODE OF WORK') == 'Toronto')

#withColumn test
wc = df.withColumn('MODE', col('MODE OF WORK'))
where_test.show()
wc.select('JOB TITLE', 'MODE').where(col('MODE')=='Hybrid').show(3)



# In[42]:


df.select('DATE OF APPLICATION').where(col('MODE OF WORK') == 'Hybrid').count()


# In[43]:


# format dates
# 1. format application date, Rejection date and First response date
df = df.withColumn(
    'DATE OF APPLICATION',
    to_date(
        col(
        'DATE OF APPLICATION'
    ))).withColumn(
    'FIRST RESPONSE DATE',
    to_date(
        col(
            'FIRST RESPONSE DATE'
        ))).withColumn('REJECTION DATE',
            to_date(
                col(
                    'REJECTION DATE'
                )))
df.printSchema()


# In[44]:


# Handle Null values
# if company name is null, drop it
# check if any null values are present in organization name
company = df.select('ORGANIZATION NAME', 'FOLLOW-UP EMAIL').where(col('ORGANIZATION NAME').isNull())

#drop rows with empty organization name
df.dropna(subset=['ORGANIZATION NAME'])


# In[45]:


# Set default values for null values
# 1. No for - Interview Scheduled, Follow-up email, Reference
df = df.fillna(value='No', subset=['INTERVIEW SCHEDULED', 'FOLLOW-UP EMAIL', 'REFERENCE'])
df.select('INTERVIEW SCHEDULED', 'FOLLOW-UP EMAIL', 'REFERENCE').show()
                                  
# df.select('ORGANIZATION NAME', 'INTERVIEW SCHEDULED').where(col('INTERVIEW SCHEDULED').isNotNull()).show()
# 2. Onsite, Remote, Hybrid Column
# 3. Default location for wrong values


# In[46]:


# assign reference value to 'Yes' for reference companies
# solace company is a reference - assign Yes to 'REFERENCE'
# before assigning Yes
df.select(
    'ORGANIZATION NAME'
).where(col(
    'ORGANIZATION NAME') == 'Solace'
       ).show()

# assigning Yes to reference for company - solace
df = df.withColumn(
    'REFERENCE', when(col(
        'ORGANIZATION NAME'
    ) == 'Solace', 'Yes'
                     ).when(col('ORGANIZATION NAME') == 'Citi', 'Yes').otherwise(col('REFERENCE')))

# after changing reference value to Yes
print(f'Using .isin() method')
df.select(
    'ORGANIZATION NAME', 'REFERENCE'
).where(col(
    'ORGANIZATION NAME').isin(['Citi', 'Solace'])
       ).show(5)

# Another way
print(f"using column")
df.select('ORGANIZATION NAME', 'REFERENCE').where(col('REFERENCE')== 'Yes').show()



# In[47]:


# Seperate df's (split the csv for better visual)
locations = df.select('ORGANIZATION NAME', 'JOB TITLE', 'LOCATION')
applications = df.select('ORGANIZATION NAME', 'JOB TITLE', 'DATE OF APPLICATION', 'FIRST RESPONSE DATE', 'REJECTION DATE')
references = df.select('ORGANIZATION NAME', 'JOB TITLE', 'REFERENCE')
socials = df.select('ORGANIZATION NAME', 'JOB TITLE', 'URL', 'CONTACT INFORMATION', 'LINKEDIN CONNECTIONS', 'FOLLOW-UP EMAIL')
links = df.select('ORGANIZATION NAME', 'JOB TITLE', 'URL')
notes = df.select('ORGANIZATION NAME', 'JOB TITLE', 'NOTES')
work_type = df.select('ORGANIZATION NAME', 'JOB TITLE', 'TYPE OF EMPLOYMENT', 'MODE OF WORK')

locations.select('*').distinct().show()
applications.show(10)
references.where(col('REFERENCE')=='Yes').show()
socials.show(10)
links.show(10)
notes.where(col('NOTES').isNotNull()).show()
work_type.show(10)


# In[63]:


# Most number of applications for a single company
organization_count = applications.groupby(col('ORGANIZATION NAME')).count()
organization_count.show()
maxval = organization_count.agg({'count':'max'}).first()[0]
organization_count.select('*').where(col('count').between(2, maxval)).show()




# In[49]:


# Group by Onsite, Remote and Hybrid
count_employment_type = work_type.groupBy(col('TYPE OF EMPLOYMENT')).count()


# In[50]:


# Group by location
count_location = locations.groupBy(col('LOCATION')).count()


# In[51]:


# Group by companies
count_organizations = applications.groupBy(col('ORGANIZATION NAME')).count()


# In[52]:


# Average response or rejection delay
first_response = applications.select('ORGANIZATION NAME', 'JOB TITLE', 'FIRST RESPONSE DATE').where(col('FIRST RESPONSE DATE').isNotNull())

rejection_dates = applications.select('ORGANIZATION NAME', 'JOB TITLE', 'DATE OF APPLICATION', 'REJECTION DATE').where(col('REJECTION DATE').isNotNull())


# In[53]:


#write locations to hdfs output
locations.write.mode('overwrite').option('header', True).csv("hdfs://hdfs-namenode:9000/job_search_etl/output/")


# In[54]:


#write locations to hdfs output
locations.write.mode('overwrite').option('header', True).csv("/opt/spark-apps/job_search_etl/output/")


# In[ ]:




