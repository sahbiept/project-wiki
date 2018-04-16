package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup

object TestHtml extends App {


//  val spark = SparkSession.builder().master("local[*]").getOrCreate()
//  System.setProperty("hadoop.home.dir","C:\\hadoop\\hadoop-2.3.0\\bin" )
//
//  import spark.implicits._
//  spark.sqlContext.setConf("spark.sql.shuffle.partitions","3")
//
//  val k = getLeagueStandingSeq("https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2016-2017","Ligue 1",2017)
//  k.toDS().repartition(1).write.format("json").option("header",true).option("inferSchema", true).mode("overwrite").save("tentative")

  def getLeagueStandingSeq(url: String, league: String, season: Int): Seq[LeagueStanding] = {
    var initseq: collection.immutable.Seq[LeagueStanding] = collection.immutable.Seq.empty[LeagueStanding]
    val doc = Jsoup.connect(url).get
    val extractclasstest1 = doc.getElementsByClass("wikitable gauche")
    val extractclasstest2 = doc.getElementsByClass("wikitable")

    if (extractclasstest1.size() != 0) {
//      val tbody = extractclasstest1.get(0).getElementsByTag("tbody").get(0)
//      if (tbody.getElementsByTag("tr").get(0).getElementsByTag("th").size() == 10) {
     val tr = FiltrageTable.ChoixTable(extractclasstest1)


        for (i <- 1 until tr.size()) {
          val position = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(0).text().trim).toSeq.head.toInt
          val team = tr.get(i).getElementsByTag("td").get(1).getElementsByTag("a").get(0).text().trim
          val points = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(2).text().trim).toSeq.head.toInt
          val played = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(3).text().trim).toSeq.head.toInt
          val won = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(4).text().trim).toSeq.head.toInt
          val drawn = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(5).text().trim).toSeq.head.toInt
          val lost = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(6).text().trim).toSeq.head.toInt
          val goalsFor = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(7).text().trim).toSeq.head.toInt
          val goalsAgainst = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(8).text().trim).toSeq.head.toInt
          val goalsDifference = (tr.get(i).getElementsByTag("td").get(9).text().trim).toInt


          val extending = LeagueStanding(league,
            season,
            position,
            team,
            points,
            played,
            won,
            drawn,
            lost,
            goalsFor,
            goalsAgainst,
            goalsDifference)
          initseq = initseq :+ extending
        }
      }

    else {
//      val tbody = extractclasstest2.get(0).getElementsByTag("tbody").get(0)
//      println(tbody.getElementsByTag("tr").get(0).getElementsByTag("th").size())
//      if (tbody.getElementsByTag("tr").get(0).getElementsByTag("th").size() == 10) {

        val tr = FiltrageTable.ChoixTable(extractclasstest2)

        for (i <- 1 until tr.size()) {
          val position = ("""\d+""".r findAllIn (tr.get(i).getElementsByTag("td").get(0).text().trim)).toSeq.head.toInt

          val team = tr.get(i).getElementsByTag("td").get(1).getElementsByTag("a").get(0).text().trim
          //parser le team comme  Grenoble Foot 38 en 2010 dans ligue 1 et juventus FC
          val points = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(2).text().trim).toSeq.head.toInt
          val played = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(3).text().trim).toSeq.head.toInt
          val won = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(4).text().trim).toSeq.head.toInt
          val drawn = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(5).text().trim).toSeq.head.toInt
          val lost = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(6).text().trim).toSeq.head.toInt
          val goalsFor = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(7).text().trim).toSeq.head.toInt
          val goalsAgainst = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(8).text().trim).toSeq.head.toInt
          val goalsDifference = (tr.get(i).getElementsByTag("td").get(9).text().trim).toInt


          val extending = LeagueStanding(league,
            season,
            position,
            team,
            points,
            played,
            won,
            drawn,
            lost,
            goalsFor,
            goalsAgainst,
            goalsDifference)

          initseq = initseq :+ extending
        }

    }


    initseq
  }
}
