package com.sparrowrecsys.offline.spark.embedding

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @Author LinYuRong
 * @Date 2021/1/21 18:13
 * @Version 1.0
 */
object Embedding {

  def processItemSequence(sparkSession: SparkSession, rawSampleDataPath: String): RDD[Seq[String]] = {
    //path of rating data
    val ratingSamples = sparkSession.read.format("csv").option("header", "true").load(rawSampleDataPath)

    //sort by timestamp udf
    val sortUdf: UserDefinedFunction = udf((rows: Seq[Row]) => {
      rows.map { case Row(movieId: String, timestamp: String) => (movieId, timestamp) }
        .sortBy { case (_, timestamp) => timestamp }
        .map { case (movieId, _) => movieId }
    })

    ratingSamples.printSchema()

    //process rating data then generate rating movie sequence data
    val userSeq = ratingSamples
      .where(col("rating") >= 3.5)
      .groupBy("userId")
      .agg(sortUdf(collect_list(struct("movieId", "timestamp"))) as "movieIds")
      .withColumn("movieIdStr", array_join(col("movieIds"), " "))

    userSeq.select("userId", "movieIdStr").show(10, truncate = false)
    userSeq.select("movieIdStr").rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)
  }

  def trainItem2vec(sparkSession: SparkSession, samples: RDD[Seq[String]], embLength: Int, embOutputFilename: String, saveToRedis: Boolean, redisKeyPrefix: String): Word2VecModel = {
    val word2vec = new Word2Vec()
      .setVectorSize(embLength)
      .setWindowSize(5)
      .setNumIterations(10)

    val model = word2vec.fit(samples)


    val synonyms = model.findSynonyms("158", 20)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    val file = new File(embOutputFilename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (movieId <- model.getVectors.keys) {
      bw.write(movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
    }
    bw.close()

    if (saveToRedis) {
      // TODO
    }

    embeddingLSH(sparkSession, model.getVectors)
    model
  }

  /**
   * 局部敏感哈希：解决向量最近邻搜索问题
   *
   * @param spark
   * @param movieEmbMap
   */
  def embeddingLSH(spark: SparkSession, movieEmbMap: Map[String, Array[Float]]): Unit = {

    val movieEmbSeq = movieEmbMap.toSeq.map(item => (item._1, Vectors.dense(item._2.map(f => f.toDouble))))
    val movieEmbDF = spark.createDataFrame(movieEmbSeq).toDF("movieId", "emb")

    //LSH bucket model
    val bucketProjectionLSH = new BucketedRandomProjectionLSH()
      .setBucketLength(0.1)
      .setNumHashTables(3)
      .setInputCol("emb")
      .setOutputCol("bucketId")

    val bucketModel = bucketProjectionLSH.fit(movieEmbDF)
    val embBucketResult = bucketModel.transform(movieEmbDF)
    println("movieId, emb, bucketId schema:")
    embBucketResult.printSchema()
    println("movieId, emb, bucketId data result:")
    embBucketResult.show(10, truncate = false)

    println("Approximately searching for 5 nearest neighbors of the sample embedding:")
    val sampleEmb = Vectors.dense(0.795, 0.583, 1.120, 0.850, 0.174, -0.839, -0.0633, 0.249, 0.673, -0.237)
    bucketModel.approxNearestNeighbors(movieEmbDF, sampleEmb, 5).show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder
      .appName("Embedding")
      .getOrCreate()

    /**
     * args(0): 输入文件全路径（本地文件注意用file:// 当前缀，如：file:///opt/module/spark-2.4.3/resources/ratings.csv）
     * args(1): 输出文件全路径（如：/opt/module/spark-2.4.3/resources/item2vecEmb.csv）
     */
    val rawSampleDataPath = args(0)
    val exportDataPath = args(1)
    val embLength = 10

    val samples = processItemSequence(sparkSession, rawSampleDataPath)
    trainItem2vec(sparkSession, samples, embLength, exportDataPath, saveToRedis = false, "i2vEmb")
  }
}
