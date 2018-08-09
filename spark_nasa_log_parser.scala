import org.apache.spark.sql.DataFrame


case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

val logFile = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")
val accessLog = logFile.map(parseLogLine)
val accessDf = accessLog.toDF()
accessDf.printSchema
val inputData=prepareData(accessDf) 
println("topLogRecord")
topLogRecord(inputData).show
println("highTrafficWeefDay")
highTrafficWeefDay(inputData).show
println("lowTrafficWeefDay")
lowTrafficWeefDay(inputData).show
println("highTrafficHour")
highTrafficHour(inputData).show
println("lowTrafficHour")
lowTrafficHour(inputData).show
println("countByHTTP")
countByHTTP(inputData).show



def parseLogLine(log: String) : 
 LogRecord = {  
   log match {  case PATTERN(host, group2, group3,timeStamp,group5,url,group7,httpCode,group8) => LogRecord(s"$host",s"$timeStamp",s"$url", s"$httpCode".toInt) 
		case _ => LogRecord("Empty", "", "",  -1 )}}

def prepareData (input: DataFrame): DataFrame = {
 input.select($"*").filter($"host" =!= "Empty").withColumn("Date",unix_timestamp(accessDf.col("timeStamp"), "dd/MMM/yyyy:HH:mm:ss").cast("timestamp")).withColumn("unix_ts" , unix_timestamp($"Date") ).withColumn("year", year(col("Date"))).withColumn("m
onth", month(col("Date"))).withColumn("day", dayofmonth(col("Date"))).withColumn("hour", hour(col("Date"))).withColumn("weekday",from_unixtime(unix_timestamp($"Date", "MM/dd/yyyy"), "EEEEE"))	
}

def topLogRecord(input: DataFrame): DataFrame = {
	input.select($"url").filter(upper($"url").like("%HTML%")).groupBy($"url").agg(count("*").alias("cnt")).orderBy(desc("cnt")).limit(10)
    
}

 def highTrafficWeefDay (input: DataFrame): DataFrame = {
	 input.select($"weekday").groupBy($"weekday").agg(count("*").alias("count_weekday")).orderBy(desc("count_weekday")).limit(5)
	 
 }

 def lowTrafficWeefDay (input: DataFrame): DataFrame = {
	 input.select($"weekday").groupBy($"weekday").where($"weekday" not null).agg(count("*").alias("count_weekday")).orderBy(asc("count_weekday")).limit(5)
	 
 }

def highTrafficHour (input: DataFrame): DataFrame = {
	 input.select($"hour").groupBy($"hour").agg(count("*").alias("count_hour")).orderBy(desc("count_hour")).limit(5)
	 
 }

def lowTrafficHour (input: DataFrame): DataFrame = {
	 input.select($"hour").groupBy($"hour").agg(count("*").alias("count_hour")).orderBy(asc("count_hour")).limit(5)
	 
 }

def countByHTTP (input: DataFrame): DataFrame = {
	 input.select($"httpCode").groupBy($"httpCode").agg(count("*").alias("count_httpCode"))
	 
 }






