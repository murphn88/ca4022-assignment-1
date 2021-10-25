--  load tables
movies = LOAD './ml-latest-small/cleaned/movies.csv' using PigStorage('\t') AS (movieId:int,title:chararray,year:int);
ratings = LOAD './ml-latest-small/ratings.csv' using PigStorage(',') AS (userId:int,movieId:int,rating:float,timestamp:int);


-- QUERY 1:
-- What is the title of the movie with the highest number of ratings (top-rated movie)?
ratings_grouped_by_movie = GROUP ratings BY movieId;
ratings_per_movie = FOREACH ratings_grouped_by_movie GENERATE FLATTEN(group) AS (movieId), COUNT($1) AS ratingsCount;
movies_ratings_count =  JOIN movies BY movieId, ratings_per_movie BY movieId;
movies_ratings_count = ORDER movies_ratings_count BY ratingsCount DESC;
most_rated_10 = LIMIT movies_ratings_count 10; 
DUMP most_rated_10; 
-- Forrest Gump has the most reviews (329)


-- QUERY 2:
-- What is the title of the most liked movie?
movie_rating_tuple = GROUP ratings BY (movieId,rating);
movie_rating_count = FOREACH movie_rating_tuple GENERATE FLATTEN(group) AS (movieId,rating), COUNT(ratings) AS countOfStars;

-- count of 5 star ratings
movie_fiveStar_count = FILTER movie_rating_count BY rating == 5.0;
-- count of ratings above 4 stars
movie_fourOrAbove_count = FILTER movie_rating_count BY rating >= 4.0;
movie_fourOrAbove_count = GROUP movie_fourOrAbove_count BY (movieId);
movie_fourOrAbove_count = foreach movie_fourOrAbove_count GENERATE(group) AS movieId, SUM(movie_fourOrAbove_count.countOfStars) AS countAbove4Stars;
-- join
movies_rating_aggr =  JOIN movies BY movieId, movie_fiveStar_count BY movieId, movie_fourOrAbove_count BY movieId;
movies_rating_aggr = FOREACH movies_rating_aggr GENERATE $0 AS movieId, title, year, countOfStars AS countOf5Stars, countAbove4Stars;

-- variation 1: movie with most 5 star ratings:
most_5_star_ratings = ORDER movies_rating_aggr BY countOf5Stars DESC;
most_5_star_ratings_10 = LIMIT most_5_star_ratings 10; 
DUMP most_5_star_ratings_10; 
-- Shawshank Redemption (153 5 star ratings)

-- varitation 2: movie with most rating of 4 stars or higher:
most_above_4_star_ratings = ORDER movies_rating_aggr BY countAbove4Stars DESC;
most_above_4_star_ratings_10 = LIMIT most_above_4_star_ratings 10; 
DUMP most_above_4_star_ratings_10; 
-- Shawshank Redemption (274 rating of 4 or more stars)


-- QUERY 3:
-- What is the User with the highest average rating?
ratings_grouped_by_user = GROUP ratings BY userId;
avg_per_user = foreach ratings_grouped_by_user GENERATE(group) AS userId, AVG($1.rating) AS avgRating;
avg_per_user_ordered = ORDER avg_per_user BY avgRating DESC;
avg_per_user_top10 = LIMIT avg_per_user_ordered 10; 
DUMP avg_per_user_top10; 
-- User 53 seems to be the most generous reviewer



