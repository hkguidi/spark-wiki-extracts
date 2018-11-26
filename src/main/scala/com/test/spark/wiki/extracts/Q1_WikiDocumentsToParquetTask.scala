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

import com.test.spark.wiki.extracts
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try


case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().appName("Spark wiki extracts").config("spark.master", "local").getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  import session.implicits._

  var leagueDF = Seq[LeagueStanding]()

  def main(args: Array[String]): Unit = {
    run()
  }

  def printList(args: Seq[_]): Unit = {
    args.foreach(println)
  }


  def extractLigue1(year: Int): Unit = {
    //println(year+1)
    val urltest = "https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s".format(year, year+1)
    val doc = Jsoup.connect(urltest)
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15")
      .get
    val body = doc.body.text
    val table = doc.select("table.wikitable.gauche")
    val rows = table.select("tr")
    val rowl = rows.takeRight(rows.size - 1).take(20)

    for (row <- rowl) {
      leagueDF = leagueDF :+ LeagueStanding("Ligue 1", year+1, row.child(0).text.toInt, row.child(1).text,
        Try(row.child(2).text.toInt)getOrElse(row.child(2).text.take(2).toInt),
        row.child(3).text.toInt, row.child(4).text.toInt,
        row.child(5).text.toInt, row.child(6).text.toInt, row.child(7).text.toInt,
        row.child(8).text.toInt, row.child(9).text.toInt)
    }
  }


  def extractSerieA(year: Int): Unit = {
    //println(year+1)
    val urltest = "https://fr.wikipedia.org/wiki/Championnat_d'Italie_de_football_%s-%s".format(year, year+1)
    val doc = Jsoup.connect(urltest)
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15")
      .get
    val body = doc.body.text
    val table = doc.select("table.wikitable.gauche")
    val rows = table.select("tr")
    val rowl = rows.takeRight(rows.size - 1).take(20)

    for (row <- rowl) {
      leagueDF = leagueDF :+ LeagueStanding("Serie A", year+1, row.child(0).text.toInt, row.child(1).text,
        row.child(2).text.toInt, row.child(3).text.toInt, row.child(4).text.toInt,
        row.child(5).text.toInt, row.child(6).text.toInt, row.child(7).text.toInt,
        row.child(8).text.toInt, row.child(9).text.toInt)
    }
  }


  def extractLiga(year: Int): Unit = {
    //println(year+1)
    val urltest = "https://fr.wikipedia.org/wiki/Championnat_d'Espagne_de_football_%s-%s".format(year, year+1)
    val doc = Jsoup.connect(urltest)
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15")
      .get
    val body = doc.body.text
    val table = doc.select("table.wikitable.gauche")
    val rows = table.select("tr")
    val rowl = rows.takeRight(rows.size - 1).take(20)

    for (row <- rowl) {
      leagueDF = leagueDF :+ LeagueStanding("Liga", year+1, row.child(0).text.toInt, row.child(1).text,
        row.child(2).text.toInt, row.child(3).text.toInt, row.child(4).text.toInt,
        row.child(5).text.toInt, row.child(6).text.toInt, row.child(7).text.toInt,
        row.child(8).text.toInt, row.child(9).text.toInt)
    }
  }


  def extractBundesliga(year: Int): Unit = {
    //println(year+1)
    val urltest = "https://fr.wikipedia.org/wiki/Championnat_d'Allemagne_de_football_%s-%s".format(year, year+1)
    val doc = Jsoup.connect(urltest)
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15")
      .get
    val body = doc.body.text
    val table = doc.select("table.wikitable.gauche")
    val rows = table.select("tr")
    val rowl = rows.takeRight(rows.size - 1).take(18)

    for (row <- rowl) {
      leagueDF = leagueDF :+ LeagueStanding("Bundesliga", year+1, row.child(0).text.toInt, row.child(1).text,
        row.child(2).text.toInt, row.child(3).text.toInt, row.child(4).text.toInt,
        row.child(5).text.toInt, row.child(6).text.toInt, row.child(7).text.toInt,
        row.child(8).text.toInt, row.child(9).text.toInt)
    }
  }


  def extractPremierLeague(year: Int): Unit = {
    //println(year+1)
    val urltest = "https://fr.wikipedia.org/wiki/Championnat_d'Angleterre_de_football_%s-%s".format(year, year+1)
    val doc = Jsoup.connect(urltest)
      .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15")
      .get
    val body = doc.body.text
    val table = doc.select("table.wikitable.gauche")
    val rows = table.select("tr")
    val rowl = rows.takeRight(rows.size - 1).take(20)

    for (row <- rowl) {
      leagueDF = leagueDF :+ LeagueStanding("Premier League", year+1, row.child(0).text.toInt, row.child(1).text,
        Try(row.child(2).text.toInt)getOrElse(row.child(2).text.take(2).toInt),
        row.child(3).text.toInt, row.child(4).text.toInt,
        row.child(5).text.toInt, row.child(6).text.toInt, row.child(7).text.toInt,
        row.child(8).text.toInt, row.child(9).text.toInt)
    }
  }


  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    try {
      println("***** Extraction Ligue 1 *****")
      (fromDate.getYear to toDate.getYear).map( year => extractLigue1(year))
      println("***** Extraction Serie A *****")
      (fromDate.getYear to toDate.getYear).map( year => extractSerieA(year))
      println("***** Extraction Liga *****")
      (fromDate.getYear to toDate.getYear).map( year => extractLiga(year))
      println("***** Extraction Bundesliga *****")
      (fromDate.getYear to toDate.getYear).map( year => extractBundesliga(year))
      println("***** Extraction PremierLeague *****")
      (fromDate.getYear to toDate.getYear).map( year => extractPremierLeague(year))
    } catch {
      case _: Throwable => println("Conversion nombre")
    }

    leagueDF.toDS().write.mode(SaveMode.Overwrite).parquet(bucket)

  }

  /*
  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS().as[Seq[LeagueInput]]
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
              year => year + 1 -> ((input.name, input.url.format(year, year + 1)))
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
            //val urltest = "http://en.wikipedia.org/wiki/New_York_City"
            //val doc = Jsoup.connect(urltest).get
            //val body = doc.body.text
            //println(body)

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
  }*/

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val inputStream = new FileReader("../resources/leagues.yaml")
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(name: String, //@JsonProperty("url") _
                       url: String)

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
