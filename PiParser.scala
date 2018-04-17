package com.pii.parser

import java.util.GregorianCalendar
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import java.io.File
import java.util.Properties
import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode

object PiParser {

  def main(args: Array[String]): Unit = {

    val startTime = new GregorianCalendar().getTime()

    if (args.length < 1) {
      System.err.println("Usage: PiParser <Configuration File Path>")
      System.exit(1)
    }

    val configMap = loadConfig(args(0))

    println(configMap)

    val localMaster = if (System.getProperty("pipeline.master", "true") == "true") true else false

    val spark = if (localMaster) {
      SparkSession.builder().master("local[*]").getOrCreate()
    } else {
      SparkSession.builder.getOrCreate()
    }
    setAwsProperties(spark, configMap)

    println("\n\nSpark Start Time" + spark.sparkContext.startTime)

    val bucketName = configMap("S3_BUCKET_NAME")
    val inputFile = configMap("S3_INPUT_FILE")
    val outputFile = configMap("S3_OUTPUT_FILE")
    val s3_inputFile = s"s3a://${bucketName}/${inputFile}"
    val s3_outputFile = s"s3a://${bucketName}/${outputFile}"

    System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");

    //spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", configMap("S3_ACCESSKEY"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", configMap("S3_SECRETKEY"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", configMap("S3_ENDPOINT"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")

    identifyPII(spark, configMap, s3_inputFile, s3_outputFile)

    val endTime = new GregorianCalendar().getTime()

    System.out.println("Start Time = " + startTime)
    System.out.println("End Time = " + endTime)
  }

  def identifyPII(spark: SparkSession, configMap: Map[String, String], s3_inputFile: String, s3_outputFile: String): Unit = {
    println("\n\nInput File --> " + s3_inputFile)

    val options = Map("header" -> "true", "delimiter" -> "|")
    var df = spark.sqlContext.read.options(options).csv(s3_inputFile)
    df.show()

    val fields = df.schema.fieldNames

    // Method to identify PII data and Mask it
    def maskSensitiveCol = (s: String) => {
      //s.toLowerCase().replaceAll("\\s", "")
      //println(s)
      if (CommonRegex.time.findFirstIn(s).size > 0) {
        CommonRegex.time.replaceAllIn(s, "xxxx")
      } else if (CommonRegex.phone.findFirstIn(s).size > 0) {
        CommonRegex.phone.replaceAllIn(s, "xxxx")
      } else if (CommonRegex.link.findFirstIn(s).size > 0) {
        CommonRegex.link.replaceAllIn(s, "xxxx")
      } else if (CommonRegex.date.findFirstIn(s).size > 0) {
        CommonRegex.date.replaceAllIn(s, "xxxx")
      } else if (CommonRegex.ip.findFirstIn(s).size > 0) {
        CommonRegex.ip.replaceAllIn(s, "xxxx")
      } else if (CommonRegex.email.findFirstIn(s).size > 0) {
        CommonRegex.email.replaceAllIn(s, "xxxx")
      } else {
        s
      }
    }

    println("SATRTED")
    println(df.schema)

    import org.apache.spark.sql.Column
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.udf

    val convertUdf = udf(maskSensitiveCol)

    println("DONE")
    fields.foreach(c => {
      df = df.withColumn(c, convertUdf(df(c)))
    })

    //df.show()

    df.write
      .mode(SaveMode.Overwrite)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("trailer", "false")
      .option("delimiter", "|")
      .csv(s3_outputFile)
      
  }

  def loadConfig(jobConfigPath: String): Map[String, String] = {
    val config = ConfigFactory.parseFile(new File(jobConfigPath))
    val props = propsFromConfig(config)

    import scala.collection.JavaConverters._
    val jobConfig = props.asScala
    jobConfig.toMap
  }

  def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._
    val props = new Properties()

    val map: Map[String, Object] = config.entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map)
    props
  }

  def setAwsProperties(spark: SparkSession, configMap: Map[String, String]): Unit = {
    //spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", configMap("S3_ACCESSKEY"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", configMap("S3_SECRETKEY"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", configMap("S3_ENDPOINT"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  }

}
