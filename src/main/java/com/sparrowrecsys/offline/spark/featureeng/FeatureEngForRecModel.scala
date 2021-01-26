package com.sparrowrecsys.offline.spark.featureeng

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
 * 特征处理
 *
 * @Author LinYuRong
 * @Date 2021/1/26 14:11
 * @Version 1.0
 */
object FeatureEngForRecModel {

  val NUMBER_PRECISION = 2

  /**
   * 添加样本标签
   * 规则：打分大于3.5为1，否则为0
   *
   * @param ratingSamples
   * @return
   */
  def addSampleLabel(ratingSamples: DataFrame): DataFrame = {
    ratingSamples.printSchema()
    ratingSamples.show(10, truncate = false)
    val sampleCount = ratingSamples.count()
    ratingSamples.groupBy(col("rating")).count().orderBy(col("rating"))
      .withColumn("percentage", col("count") / sampleCount).show(100, truncate = false)

    ratingSamples.withColumn("label", when(col("rating") >= 3.5, 1).otherwise(0))
  }

  /**
   * 添加电影特征
   *
   * @param movieSamples
   * @param ratingSamples
   * @return
   */
  def addMovieFeatures(movieSamples: DataFrame, ratingSamples: DataFrame): DataFrame = {
    // add movie basic features
    val sampleWithMovie1 = ratingSamples.join(movieSamples, Seq("movieId"), "left")
    // add release year
    val extractReleaseYearUdf = udf({ (title: String) => {
      if (title == null || title.trim.length < 6) {
        // default value
        1990
      } else {
        val releaseYear = title.trim.substring(title.length - 5, title.length - 1)
        releaseYear.toInt
      }
    }
    })

    val sampleWithMovie2 = sampleWithMovie1.withColumn("releaseYear", extractReleaseYearUdf(col("title")))

    // split genres
    val sampleWithMovie3 = sampleWithMovie2.withColumn("movieGenre1", split(col("genres"), "\\|").getItem(0))
      .withColumn("movieGenre2", split(col("genres"), "\\|").getItem(1))
      .withColumn("movieGenre3", split(col("genres"), "\\|").getItem(2))

    // add rating feature
    val movieRatingFeatures = sampleWithMovie3.groupBy(col("movieId"))
      .agg(count(lit(1)).as("movieRatingCount"),
        format_number(avg(col("rating")), NUMBER_PRECISION).as("movieAvgRating"),
        stddev(col("rating")).as("movieRatingStddev"))
      .na.fill(0).withColumn("movieRatingStddev", format_number(col("movieRatingStddev"), NUMBER_PRECISION))

    val sampleWithMovie4 = sampleWithMovie3.join(movieRatingFeatures, Seq("movieId"), "left")
    sampleWithMovie4.printSchema()
    sampleWithMovie4.show(10, truncate = false)

    sampleWithMovie4
  }

  /**
   * 添加用户特征
   *
   * @param ratingSamples
   * @return
   */
  def addUserFeatures(ratingSamples: DataFrame): DataFrame = {
    val samplesWithUserFeatures = ratingSamples
      .withColumn("userPositiveHistory", collect_list(when(col("label") === 1, col("movieId")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userPositiveHistory", reverse(col("userPositiveHistory")))
      .withColumn("userRatedMovie1", col("userPositiveHistory").getItem(0))
      .withColumn("userRatedMovie2", col("userPositiveHistory").getItem(1))
      .withColumn("userRatedMovie3", col("userPositiveHistory").getItem(2))
      .withColumn("userRatedMovie4", col("userPositiveHistory").getItem(3))
      .withColumn("userRatedMovie5", col("userPositiveHistory").getItem(4))
      .withColumn("userRatingCount", count(lit(1))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userAvgReleaseYear", avg(col("releaseYear"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)).cast(IntegerType))
      .withColumn("userReleaseYearStddev", stddev(col("releaseYear"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userAvgRating", format_number(avg(col("rating"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)), NUMBER_PRECISION))
      .withColumn("userRatingStddev", stddev(col("rating"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userGenres", extractGenres(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1))))
      .na.fill(0)
      .withColumn("userRatingStddev", format_number(col("userRatingStddev"), NUMBER_PRECISION))
      .withColumn("userReleaseYearStddev", format_number(col("userReleaseYearStddev"), NUMBER_PRECISION))
      .withColumn("userGenre1", col("userGenres").getItem(0))
      .withColumn("userGenre2", col("userGenres").getItem(1))
      .withColumn("userGenre3", col("userGenres").getItem(2))
      .withColumn("userGenre4", col("userGenres").getItem(3))
      .withColumn("userGenre5", col("userGenres").getItem(4))
      .drop("genres", "userGenres", "userPositiveHistory")
      .filter(col("userRatingCount") > 1)

    samplesWithUserFeatures.printSchema()
    samplesWithUserFeatures.show(100, truncate = false)

    samplesWithUserFeatures
  }

  val extractGenres: UserDefinedFunction = udf { (genreArray: Seq[String]) => {
    val genreMap = mutable.Map[String, Int]()
    genreArray.foreach((element: String) => {
      val genres = element.split("\\|")
      genres.foreach((oneGenre: String) => {
        genreMap(oneGenre) = genreMap.getOrElse[Int](oneGenre, 0) + 1
      })
    })
    val sortedGenres = ListMap(genreMap.toSeq.sortWith(_._2 > _._2): _*)
    sortedGenres.keys.toSeq
  }
  }

  def splitAndSaveTrainingTestSamples(samples: DataFrame, savePath: String) = {
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.1)

    //split training and test set by 8:2
    val Array(training, test) = smallSamples.randomSplit(Array(0.8, 0.2))

    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(savePath + "/trainingSamples")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(savePath + "/testSamples")
  }

  /**
   * bin/spark-submit \
   *  --class com.sparrowrecsys.offline.spark.featureeng.FeatureEngForRecModel --master yarn --deploy-mode cluster \
   *  ./examples/jars/SparrowRecSysZero2One.jar \
   *  file:///opt/module/spark-2.4.3/resources/movies.csv \
   *  file:///opt/module/spark-2.4.3/resources/ratings.csv \
   *  file:///opt/module/spark-2.4.3/resources 10
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("FeatureEngForRecModel").getOrCreate()

    val movieSamplePath = args(0)
    val ratingSamplePath = args(1)

    val movieSamples = sparkSession.read.format("csv").option("header", "true").load(movieSamplePath)
    val ratingSamples = sparkSession.read.format("csv").option("header", "true").load(ratingSamplePath)

    val ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    ratingSamplesWithLabel.show(10, truncate = false)

    val samplesWithMovieFeatures = addMovieFeatures(movieSamples, ratingSamplesWithLabel)
    val samplesWithUserFeatures = addUserFeatures(samplesWithMovieFeatures)

    val savePath = args(2)
    // save samples as csv format
    splitAndSaveTrainingTestSamples(samplesWithUserFeatures, savePath)
  }
}
