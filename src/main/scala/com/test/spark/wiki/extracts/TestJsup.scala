package com.test.spark.wiki.extracts
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
object TestJsup extends App {


  def getLeagueStandingSeq(url: String, league: String, season: Int): Seq[LeagueStanding] = {

    val initseq = Seq.empty[LeagueStanding]
    val doc = Jsoup.connect(url).get
    val extractclasstest1 = doc.getElementsByClass("wikitable gauche")
    val extractclasstest2 = doc.getElementsByClass("wikitable")

    if (extractclasstest1.size()== 1 ) {
      val tbody = extractclasstest1.get(0).getElementsByTag("tbody").get(0)
      val tr = tbody.getElementsByTag("tr")
      for (i <- 1 until tr.size()) {
        val position = tr.get(i).getElementsByTag("td").get(0).text().toInt
        val team = tr.get(i).getElementsByTag("td").get(1).text()
        val points = tr.get(i).getElementsByTag("td").get(2).text().toInt
        val played = tr.get(i).getElementsByTag("td").get(3).text().toInt
        val won = tr.get(i).getElementsByTag("td").get(4).text().toInt
        val drawn = tr.get(i).getElementsByTag("td").get(5).text().toInt
        val lost = tr.get(i).getElementsByTag("td").get(6).text().toInt
        val goalsFor = tr.get(i).getElementsByTag("td").get(7).text().toInt
        val goalsAgainst = tr.get(i).getElementsByTag("td").get(8).text().toInt
        val goalsDifference = tr.get(i).getElementsByTag("td").get(9).text().toInt


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
        initseq :+ extending
      }
    }
    else
      {
        val tbody = extractclasstest2.get(0).getElementsByTag("tbody").get(0)
        val tr = tbody.getElementsByTag("tr")
        for (i <- 1 until tr.size()) {
          val position = tr.get(i).getElementsByTag("td").get(0).text().toInt
          val team = tr.get(i).getElementsByTag("td").get(1).text()
          val points = tr.get(i).getElementsByTag("td").get(2).text().toInt
          val played = tr.get(i).getElementsByTag("td").get(3).text().toInt
          val won = tr.get(i).getElementsByTag("td").get(4).text().toInt
          val drawn = tr.get(i).getElementsByTag("td").get(5).text().toInt
          val lost = tr.get(i).getElementsByTag("td").get(6).text().toInt
          val goalsFor = tr.get(i).getElementsByTag("td").get(7).text().toInt
          val goalsAgainst = tr.get(i).getElementsByTag("td").get(8).text().toInt
          val goalsDifference = tr.get(i).getElementsByTag("td").get(9).text().toInt


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
          initseq :+ extending
        }
      }
      initseq


    }

}
