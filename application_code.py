import json, pprint, requests, textwrap
host = 'http://localhost:8998'
data = {'kind': 'spark'}
headers = {'Content-Type': 'application/json'}
r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
r.json()
session_url = host + r.headers['location']
r = requests.get(session_url, headers=headers)
r.json()
statements_url = session_url + '/statements'
data = {'code': '1 + 1'}
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
r.json()
statement_url = host + r.headers['location']
r = requests.get(statement_url, headers=headers)
pprint.pprint(r.json())
data = {
  'code': textwrap.dedent("""
    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
    import org.apache.spark.mllib.recommendation.Rating

    val seed = 5L;
    val numIterations = 10;
    val rank = 10;
    var errors = Array(0, 0, 0);
    var error = 0;
    var tolerance = 0.02;
    var minErr = 0.0;
    var br = -1;
    var bi = -1;

    val large_d = sc.textFile("/user/sb6606/ml-latest/ratings.csv");
    val large_d_header = large_d.take(1);
    val res11 = large_d_header.mkString(",");
    val large_r_d = large_d.filter(line => line != res11);
    val large_r_d_split = large_r_d.map(line => line.split(","));
    val large_r_d_final =  large_r_d_split.map(line => (line(0).toInt, line(1).toInt, line(2).toDouble));

    val large_m_d = sc.textFile("/user/sb6606/ml-latest/movies.csv");
    val large_m_d_header = large_m_d.take(1);
    val res12 = large_m_d_header.mkString(",");
    val large_m = large_m_d.filter(line => line != res12);
    val large_m_split = large_m.map(line => line.split(","));
    val large_m_final =  large_m_split.map(line => (line(0).toInt, line(1), line(2)));
    val large_movies_title =  large_m_split.map(line => (line(0).toInt, line(1)));
    val movie_ID_with_RDD = large_ratings_data_final.map(line => (line._2, line._3)).groupByKey();
    val movie_ID_with_avg_RDD = movie_ID_with_RDD.map{ case(movieID, ratings) => {val nlength = ratings.size; (movieID, (nlength, ratings.sum.toDouble / nlength.toDouble))}};
    val movie_rating_RDD = movie_ID_with_avg_RDD.map(line => (line._1, line._2._1));

    val newuser_ratings = Array((0, 260, 9.0), (0, 1, 8.0), (0, 16, 7.0), (0, 25, 8.0), (0, 32, 9.0), (0, 335, 4.0), (0, 379, 3.0), (0, 296, 7.0), (0, 858, 10.0), (0, 50, 8.0));
    val newuser_ratings_RDD = sc.parallelize(newuser_ratings);
    val complete_data_with_new_ratings_RDD = large_ratings_data_final.union(newuser_ratings_RDD);

    val complete_ratings = complete_data_with_new_ratings_RDD.map(line => Array(line._1, line._2, line._3));
    val new_complete_ratings = complete_ratings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)};
    val new_ratings_model = ALS.train(new_complete_ratings, rank, numIterations, 0.01);

    val newuser_ids = newuser_ratings.map(line => line._2);
    val newuser_unrated_movies_RDD = large_m_final.filter(line => !(newuser_ids contains line._1)).map(line => (0, line._1, 4.0));
    val new_unrated = newuser_unrated_movies_RDD.map(line => Array(line._1, line._2, line._3));
    val new_unrated_RDD = new_unrated.map{ case Array(user, movie, ratings) => Rating(user.toInt, movie.toInt, ratings.toDouble)};
    val new_unrated_RDD_final = new_unrated_RDD.map { case Rating(user, movie, ratings) => (user, movie)};
    val newuser_RDD = new_ratings_model.predict(new_unrated_RDD_final);
    val newuser_rating_RDD = newuser_RDD.map(line =>  (line.product, line.rating));
    val newuser_rating_title_and_count_RDD = newuserating_RDD.join(large_movies_title).join(movie_rating_RDD);
    val RDD = newuser_rating_title_and_count_RDD.map(line =>  (line._2._1._2, line._2._1._1, line._2._2));
    val top_movies = RDD.filter(line => line._3 >= 25).takeOrdered(25)(Ordering[Double].reverse.on(x => x._2));
    val new_r = sc.parallelize(top_movies);
    new_r.saveAsTextFile("/user/sb6606/top_movies");
""")
}

r = requests.post(statements_url, data=json.dumps(data), headers=headers)
pprint.pprint(r.json())

statement_url = host + r.headers['location']
r = requests.get(statement_url, headers=headers)
pprint.pprint(r.json())

