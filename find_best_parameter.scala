import org.apache.spark.mllib.recommendation.ALS
package org.apache.spark.examples.mllib
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

val small_ratings_raw_data = sc.textFile("/user/sb6606/ml-latest-small/ratings.csv")
val small_ratings_raw_data_header = small_ratings_raw_data.take(1)
val res11 = small_ratings_raw_data_header.mkString(",")
val small_ratings_data = small_ratings_raw_data.filter(line => line != res11)
val small_ratings_data_split = small_ratings_data.map(line => line.split(","))
val small_ratings_data_final =  small_ratings_data_split.map(line => (line(0), line(1), line(2)))

val small_movies_raw_data = sc.textFile("/user/sb6606/ml-latest-small/movies.csv")
val small_movies_raw_data_header = small_movies_raw_data.take(1)
val res12 = small_movies_raw_data_header.mkString(",")
val small_movies_data = small_movies_raw_data.filter(line => line != res12)
val small_movies_data_split = small_movies_data.map(line => line.split(","))


val splits = small_ratings_data_final.randomSplit(Array(0.6, 0.2, 0.2), seed = 11L)
val trainData = splits(0).cache
val testData = splits(1).map(line => (line._1, line._2))
val validateData = splits(2).map(line => (line._1, line._2))
val validateRDD = splits(2)

val seed = 5L
val numIterations = 10
val ranks = Seq(4, 10, 12)
var errors = Array(0, 0, 0)
var error = 0
var tolerance = 0.02
var minErr = 0.0
var br = -1
var bi = -1


for(var rank <- ranks){
    val ratings = trainData.map(line => Array(line._1, line._2, line._3))
    val new_ratings = ratings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)}
    val vD = validateRDD.map(line => Array(line._1, line._2, line._3))
    val new_validate = vD.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)}
    val model = ALS.train(new_ratings, rank, numIterations, 0.01)
    val new_validateData = validateData.map{case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)}
    val predictions = model.predict(new_validateData).map{case Rating(user, movie, rating) =>((user, movie),rating)}
    val ratesAndPreds = new_validate.map{case Rating(user, movie, rating) => ((user, movie),rating)}.join(predictions)

    val MSE = ratesAndPreds.map{case ((user, movie), (r1, r2)) => (r1 - r2) * (r1 - r2)}.mean()
    println(s"Mean Squared Error = $MSE")
}
