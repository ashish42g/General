package practise

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object SparkPractise {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Spark <file>")
      System.exit(1)
    }

    val path = args(0)
    val conf = new SparkConf().setMaster("local[*]") setAppName ("General")
    val sc = new SparkContext(conf)
    //filterOprn(sc, path)
    //wholeFileOprn(sc, path)
    //flatMapOprn(sc, path)
    //pairOprn(sc, path)
    keyByOprn(sc,path)
    //complexPairOprn(sc,path)
    //rowLinkOprn(sc, path)
    //avgWordLengthOprn(sc, path)
    //debugStringOprn(sc,path)
    sc.stop()
  }

  def filterOprn(sc: SparkContext, path: String) = {
    val file = sc.textFile(path)
    val data = file.map(_.toUpperCase())
    val filterData = data.filter(_.startsWith("I"))
    filterData.take(2).foreach(println)
  }

  def wholeFileOprn(sc : SparkContext, path : String) = {
    val myrdd1 = sc.wholeTextFiles(path)
    myrdd1.foreach(println)
    val myrdd2 = myrdd1.map(pair => JSON.parseFull(pair._2).get.asInstanceOf[Map[String,String]])
    myrdd2.foreach(println)
    for (record <- myrdd2.take(2)){
      println(record.getOrElse("firstName",null))
    }
  }

  def flatMapOprn(sc : SparkContext, path : String) = {
    val file = sc.textFile(path)
    file.foreach(print)
    val rdd1 = file.flatMap(_.split(' '))
    rdd1.foreach(println)
    val distinctRdd = rdd1.distinct()
    distinctRdd.foreach(println)
  }

  //create pair RDD operations

  def pairOprn(sc : SparkContext, path : String) = {
    val users = sc.textFile(path )
    users.map(_.split(' ')).map(l => (l(0),l(1))).foreach(println)
  }

  def keyByOprn(sc : SparkContext, path : String)={
    val logFile = sc.textFile(path)
    val keyVal = logFile.keyBy(line => line.split(' ')(2))
    keyVal.foreach(println)
  }

  def complexPairOprn(sc : SparkContext, path : String) = {
    val postalFile = sc.textFile(path)
    postalFile.map(_.split(' ')).map(l => (l(0),(l(1),l(2)))).foreach(println)
  }

  def rowLinkOprn(sc : SparkContext, path : String) = {
    val rows = sc.textFile(path)
    val finalRows = rows.map(_.split(' ')).map(l => (l(0),l(1))).flatMapValues(_.split(':'))
    finalRows.foreach(println)
    finalRows.groupByKey().foreach(println)
    val op = finalRows.groupByKey().sortByKey().collect()
    op.foreach(println)
  }

  def avgWordLengthOprn(sc : SparkContext, path : String): Unit ={
    val file = sc.textFile(path)
    val splittedLines = file.flatMap(_.split("\\W"))
    splittedLines.foreach(println)
    val words = splittedLines.map(word => (word, word.length))
    words.foreach(println)
    val groupedWords = words.groupByKey()
    groupedWords.foreach(println)
    val avglens = groupedWords.map(pair => (pair._1, pair._2.sum/pair._2.size.toDouble))
    avglens.foreach(println)
  }

  def debugStringOprn (sc : SparkContext, path : String): Unit = {
    val avglens = sc.textFile(path).
      flatMap(line => line.split("\\W")).
      map(word => (word(0),word.length)).
      groupByKey().
      map(pair => (pair._1, pair._2.sum/pair._2.size.toDouble))

    println(avglens.toDebugString)
  }

  def isEmptyCheck (sc : SparkContext, path : String): Unit ={
    val file = sc.textFile(path).flatMap(l => l.split("\\W")).isEmpty().equals(true)
  }
}