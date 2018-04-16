package com.test.spark.wiki.extracts

import org.jsoup.select.Elements

object FiltrageTable {
  def ChoixTable(extractclasstest: Elements): Elements = {

    var rbody:Elements= null
    for (i <- 0 until extractclasstest.size()) {
      val tbody = extractclasstest.get(i).getElementsByTag("tbody").get(0)
      if (tbody.getElementsByTag("tr").get(0).getElementsByTag("th").size() == 10) {
         rbody=tbody.getElementsByTag("tr")



      }
    }
rbody
  }
}
