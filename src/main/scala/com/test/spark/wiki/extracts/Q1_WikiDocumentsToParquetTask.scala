package com.test.spark.wiki.extracts

import java.net.URL
import java.io.File
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

//object Q1_WikiDocumentsToParquetTask extends App {
case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {

  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  System.setProperty("hadoop.home.dir","C:\\hadoop\\hadoop-2.3.0\\bin" )
  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)
    import session.implicits._

    val result = getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
    // result.show(40)

    val resultfinal = result.flatMap(
      x =>
        try {
          TestHtml.getLeagueStandingSeq(x._2._2, x._2._1, x._1)
        } catch {
          case _: Throwable =>
            // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
            logger.warn(s"Can't parse season ${x._1} from ${x._2._2}")
            // c.printStackTrace
            Seq.empty
        }

    )
    println("le nombre de ligne est : " + resultfinal.count())


    resultfinal
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

  }



//            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
//            // ATTENTION:
//            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
//            //  - Il faut veillez à recuperer uniquement le classement général
//            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage

//              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
  //les logs vont etre placés vers S3. Dans l'emplacement S3, et plus précisément dans : task-attempts / <applicationid> / <firstcontainer> / *

//      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket

  //on utilise une des deux méthodes suivantes :  repartition(nombre de partition), coalesce(numPartitions)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?

  // ** Fournit un support extensible pour les codages par colonne (par exemple, delta, durée d'exécution, etc.)
  // ** Fournir l'extensibilité de stocker plusieurs types de données dans les données de colonne (par exemple, les index, les filtres bloom, les statistiques)
  // ** Offre de meilleures performances en écriture en stockant les métadonnées à la fin du fichier

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
  // on pourra exécuter des requetes SQL sur des données structurées grace aux datasets
 // }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml

    import java.nio.charset.StandardCharsets
    import java.nio.file.Files
    import java.nio.file.Paths
   val  yamlSource = new String(Files.readAllBytes(Paths.get("src/main/resources/leagues.yaml")), StandardCharsets.UTF_8)

    mapper.readValue(yamlSource, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe


case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)


