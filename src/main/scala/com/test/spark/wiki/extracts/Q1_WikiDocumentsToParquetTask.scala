package com.test.spark.wiki.extracts

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}
import java.io.FileReader

import scala.collection.JavaConversions._

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().appName("Spark wiki extracts").config("spark.master", "local").getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  import session.implicits._

  def main(args: Array[String]): Unit = {
    run()
  }

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDF("s").as[Seq[LeagueInput]]
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage


          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .XXXX
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    /* Parquet est un format orienté colonnes contrairement aux autres formats qui sont généralement orientés ligne.
      Il est donc plus efficace en terme de stockage et performant lors de requêtes sur des colonnes spécifiques. */

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    //Un dataset spark permet d'effectuer plus d'opérations avec des fonctions optimisées
  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val inputStream = new FileReader("../resources/leagues.yaml")
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") _name: String,
                       @JsonProperty("url") _url: String)

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
