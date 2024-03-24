from pyspark.sql import  SparkSession

spark=SparkSession.builder\
      .appName("MyPySparkApp")\
      .getOrCreate()

data=[("ABC","Q1",2000),
      ("ABC","Q2",3000),
      ("ABC","Q3",6000),
      ("ABC","Q4",1000),
      ("XYZ","Q1",5000),
      ("XYZ","Q2",4000),
      ("XYZ","Q3",1000),
      ("XYZ","Q4",2000),
      ("KLM","Q1",2000),
      ("KLM","Q2",3000),
      ("KLM","Q3",1000),
      ("KLM","Q4",5000)
      ]
column=["Company","Quarter","Revenue"]
df=spark.createDataFrame(data=data,schema=column)
df.show()
df1=df.groupBy("Company").pivot("Quarter").sum("Revenue")
df1.show()
df1.selectExpr('Company',"stack(4,'Q1',Q1,'Q2',Q2,'Q3',Q3,'Q4',Q4) AS (quarter,revenue)").show()