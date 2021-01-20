package com.sparrowrecsys.offline.spark.featureeng

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author LinYuRong
 * @Date 2021/1/20 14:42
 * @Version 1.0
 */
object FeatureEngineering {

  def oneHotEncoderExample(samples: DataFrame): Unit = {
    val samplesWithIdNumber = samples.withColumn("movieIdNumber", col("movieId").cast(sql.types.IntegerType))

    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array("movieIdNumber"))
      .setOutputCols(Array("movieIdVector"))
      .setDropLast(false)

    val oneHotEncoderSamples = oneHotEncoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber)
    oneHotEncoderSamples.printSchema()
    oneHotEncoderSamples.show(10)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder
      .appName("FeatureEngineering")
      .getOrCreate()

    // 数据文件需上传到集群服务器对应路径
    val filepath = "resources/movies.csv"
    val movieSamples = sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(filepath)

    println("Raw movie Samples:")
    movieSamples.printSchema()
    movieSamples.show(10)

    println("OneHotEncoder Example:")
    oneHotEncoderExample(movieSamples)

    sparkSession.stop()
  }
}
