from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
import ConfigParser
import sys
import os

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
configParser = ConfigParser.RawConfigParser()
configFilePath = r'DEV.cfg'
configParser.read(configFilePath)
sourcefilePath = configParser.get('gs_bucket_path', 'ps_sourceFile')
targetfilePath = configParser.get('gs_bucket_path', 'csv_targetFile')
sourceTablelist = ["PS_ADM_APPL_PROG"]
targetFileName =  targetfilePath+os.path.basename(sys.argv[0])[:-3]
def createTempTables(sparkSess,sourceTablelist):
    for tabList in sourceTablelist:
        df_source = sparkSess.read.option("header", "true").csv(sourcefilePath+tabList+".csv")
        df_source.registerTempTable(tabList)
    return df_source
df_source=createTempTables(spark,sourceTablelist)
df_staging = spark.sql("""
    SELECT DISTINCT
    COALESCE(RTRIM(LTRIM(EMPLID)),'-') AS EMPLID,
    COALESCE(RTRIM(LTRIM(ACAD_CAREER)),'-') AS ACAD_CAREER,
    COALESCE(RTRIM(LTRIM(STDNT_CAR_NBR)),'-') AS STDNT_CAR_NBR,
    COALESCE(RTRIM(LTRIM(ADM_APPL_NBR)),'-') AS ADM_APPL_NBR,
    COALESCE(RTRIM(LTRIM(APPL_PROG_NBR)),'-') AS APPL_PROG_NBR,
    COALESCE(RTRIM(LTRIM(ACAD_PROG)),'-') AS ACAD_PROG,
    1 AS ECD_CNT
    FROM PS_ADM_APPL_PROG WHERE PROG_ACTION IN ('DEIN')

""")
df_staging = df_staging.withColumn("SEQ_ID",row_number().over(Window.orderBy(monotonically_increasing_id())))

df_staging.write.option("header", "true").mode('overwrite').csv(targetFileName)
