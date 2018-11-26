package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().appName("Spark wiki extracts").config("spark.master", "local").getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    run()
  }

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    val sqlStandings = s"standings"
    standings.createTempView(sqlStandings)
    //val res1 = session.sql(s"SELECT league, season, ROUND(AVG(goalsFor), 1) FROM $sqlStandings " +
    //  s"GROUP BY league, season ORDER BY league, season").show()


    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    //standings.where($"league" === "Ligue 1" && $"position" === 1).groupByKey(row => row.team).count().show()
    val res2 = session.sql(s"SELECT league, position, team, SUM(position) FROM $sqlStandings " +
      s"WHERE league = 'Ligue 1' AND position = 1 " +
      s"GROUP BY league, team, position ORDER BY sum(position) desc").show()



    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    //En commentaire car avg ne reconnait pas la colonne points.
    //standings.where($"position" === 1).groupByKey(row => (row.league)).agg(avg(_.points)).show()
    val res3 = session.sql(s"SELECT league, ROUND(AVG(points), 1) AS pointsMoy FROM $sqlStandings " +
      s"WHERE position = 1 " +
      s"GROUP BY league").show()

    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val decade: Integer => String = _.toString.dropRight(1).concat("0")
    val decadeUDF = udf(decade)
    standings.withColumn("decade", decadeUDF('season))


    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")
    standings.where($"position" === 1 && $"position" === 10).groupByKey(row => (row.league, row.season))


  }
}
