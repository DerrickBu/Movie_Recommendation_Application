val musicrdd1 = sc.textFile("music.csv")
val temp0 = musicrdd1.take(1)
val temp1 = temp0.mkString(",")
val musicrdd2 = musicrdd1.filter(line => line != temp1)
val musicrdd3 = musicrdd2.map(_.split(","))
val musicrdd4 = musicrdd3.map(line => (line(23), line(29), line(33), line(34)))
val musicrdd5 = musicrdd4.filter(line => line._1.matches("^-?[0-9]+(\\.[0-9]+)?$"))
val musicrdd6 = musicrdd5.top(1000)
val musicrdd7 = musicrdd6.drop(2)
val musicrdd8 = musicrdd7.filter(line => line._4 != "0")
val musicrdd9 = musicrdd8.filter(line => line._2 == "hip hop")
val musicrdd10 = musicrdd9.map(line => (line._4, (line._1, line._3)))
val musicrdd11 = sc.parallelize(musicrdd10)
val musicrdd12 = musicrdd11.map(line => (line._1, line._2._1.toDouble))
val musicrdd13 = musicrdd12.reduceByKey(_ + _)
musicrdd13.count()
val musicrdd14 = musicrdd13.takeOrdered(12)(Ordering[Double].reverse.on(x => x._2));
val musicrdd15 = sc.parallelize(musicrdd14)

val movierdd1 = sc.textFile("/user/sb6606/title.basics.tsv")
val movierdd2 = movierdd1.map(_.split("\t"))
val movierdd3 = movierdd2.map(line => line.mkString(","))
val temp2 = movierdd3.take(1)
val temp3 = temp2.mkString(",")
val movierdd4 = movierdd3.filter(line => line != temp3)
val movierdd5 = movierdd4.map(_.split(","))
val movierdd6 = movierdd5.map(line => (line(0), line(2), line(5)))
val movierdd7 = movierdd6.filter(line => line._3 == "2001")
val movierdd8 = movierdd7.map(line => (line._1, (line._2, line._3)))

val movie1 = sc.textFile("/user/sb6606/title.ratings.csv")
val temp4 = movie1.take(1)
val temp5 = temp4.mkString(",")
val movie2 = movie1.filter(_ != temp5)
val movie3 = movie2.map(_.split(","))
val movie4 = movie3.map(line => (line(0), line(1), line(2)))
val movie5 = movie4.map(line => (line._1, (line._2, line._3)))

val joined_movie = movierdd8.join(movie5)
val joined_movie2 = joined_movie.map(line => (line._1, line._2._1._1, line._2._2._1, line._2._2._2))
joined_movie2.count()
val joined_movie3 = joined_movie2.map(line => (line._1, line._2, line._3.toDouble, line._4.toInt))
val most_votes = joined_movie3.takeOrdered(16605)(Ordering[Int].reverse.on(x => x._4));
val most_rates = joined_movie3.filter(line => line._4 > 10000).takeOrdered(16605)(Ordering[Double].reverse.on(x => x._3));

spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._
val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project/title.ratings.csv"))
val total = df.show
val highest = df.sort(df("C1").desc).show()
val lowest = df.sort(df("C2").desc).show()


val rdd1 = sc.textFile("project/title.ratings.csv")
rdd1.take(3)
val rdd2 = rdd1.take(1)
val rdd3 = rdd2.mkString(",")
val rdd4 = rdd1.filter(line => line != rdd3)
rdd4.take(3)
val rdd5 = rdd4.map(line => line.split(",")) 
rdd5.take(3)
val rdd6 = rdd5.map(line => (line(0).toString,line(1).toDouble,line(2).toInt))
rdd6.take(3)
val rdd7 = rdd6.filter(line => line._3 >= 800).takeOrdered(100)(Ordering[Int].reverse.on(line => line._3))
val genre = sc.textFile("project/title.basics.tsv")
genre.take(3)
val genre1 = genre.map(_.split("\t"))
genre1.take(3)
val genre2 = genre1.take(1)
val g = genre2(0)
val g2 = g.mkString(",")
val genre4 = genre1.map(line => line.mkString(","))
val genre3 = genre4.filter(line => line != g2)
genre3.take(3)
val genre5 = genre3.map(line => line.split(","))
genre5.take(3)
val movie_id = rdd7.map(line => line._1)
val genre6 = genre5.filter(line => movie_id contains line(0))
genre6.take(3)
val genre7 = genre6.map(line => (line(0), (line(8), line(2))))
genre7.take(3)
val rdd8 = rdd7.zipWithIndex.map{ case (r, i) => (r._1, r._2, i) }
val rdd8_new = sc.parallelize(rdd8)
val rdd9 = rdd8.map(line => (line._1, (line._2, line._3)))
val rdd10 = sc.parallelize(rdd9)
val moviejoin = rdd10.join(genre7)
moviejoin.take(3)
val moviejoin1 = moviejoin.map(line => (line._2._1._2, line._1, line._2._1._1, line._2._2._1, line._2._2._2))
val moviejoin2 = moviejoin1.sortBy(_._1, true)

//>=800 votes, top 100, its genre and ratings
