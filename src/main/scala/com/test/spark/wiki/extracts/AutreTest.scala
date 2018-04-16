package com.test.spark.wiki.extracts

import org.jsoup.Jsoup
import org.jsoup.select.Elements

object AutreTest {


  def filtrage(url:String,extractclasstest:Elements):Elements = {
    val initseq = Seq.empty[LeagueStanding]
    val doc = Jsoup.connect(url).get
    var tbody:Elements= null
    for (i <- 0 until extractclasstest.size()) {
      val k = extractclasstest.get(i).getElementsByTag("caption")

      if (k.size() != null && (k.text() == "Classement" || k.text().contains("Classement —"))) {

        tbody= extractclasstest.get(i).getElementsByTag("tbody")
      }

    }
    tbody
  }
}
