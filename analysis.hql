-- create and load tables
CREATE TABLE IF NOT EXISTS movies (movieId int, title String, yr Int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' ;
LOAD DATA LOCAL INPATH '/home/niamh/hadoop/assignment_1/ml-latest-small/cleaned/movies.csv' OVERWRITE INTO TABLE movies;
ALTER TABLE movies SET TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS genres (movieId int, genre String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' ;
LOAD DATA LOCAL INPATH '/home/niamh/hadoop/assignment_1/ml-latest-small/cleaned/genres.csv' OVERWRITE INTO TABLE genres;
ALTER TABLE genres SET TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ratings (userId int, movieId int, rating float, timeStmp int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' ;
LOAD DATA LOCAL INPATH '/home/niamh/hadoop/assignment_1/ml-latest-small/ratings.csv' OVERWRITE INTO TABLE ratings;
ALTER TABLE ratings SET TBLPROPERTIES ("skip.header.line.count"="1");

-- QUERY 1:
-- What is the title of the movie with the highest number of ratings (top-rated movie)?

INSERT OVERWRITE LOCAL DIRECTORY '/home/niamh/hadoop/assignment_1/ml-latest-small/output/1' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT  * 
FROM    movies
JOIN    (SELECT  movieId as mid, COUNT(*) AS cnt
        FROM    ratings
        GROUP BY    ratings.movieId) AS r
        ON (r.mid = movies.movieId)
ORDER BY cnt DESC
LIMIT 10;


-- QUERY 2:
-- What is the title of the most liked movie? (movie with most ratings of or above 4 stars)
INSERT OVERWRITE LOCAL DIRECTORY '/home/niamh/hadoop/assignment_1/ml-latest-small/output/2' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT  * 
FROM    movies
JOIN    (SELECT  movieId as mId, COUNT(*) AS cnt
        FROM    ratings
        WHERE rating >= 4.0
        GROUP BY    ratings.movieId) AS r
        ON (r.mId = movies.movieId)
ORDER BY cnt DESC
LIMIT 10;


-- QUERY 3:
-- What is the User with the highest average rating?
INSERT OVERWRITE LOCAL DIRECTORY '/home/niamh/hadoop/assignment_1/ml-latest-small/output/3' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT  userId, AVG(rating) AS avgrat, COUNT(rating)
FROM    ratings
GROUP BY userId
ORDER BY avgrat DESC;


-- QUERY 4:
-- Count the number of ratings for each star level (How many 1 star ratings? ... How many 5* ratings?)
INSERT OVERWRITE LOCAL DIRECTORY '/home/niamh/hadoop/assignment_1/ml-latest-small/output/4' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT  rating, COUNT(rating)
FROM    ratings
GROUP BY rating
ORDER BY rating;


-- QUERY 5:
-- What is the most popular rating?
SELECT  rating, COUNT(rating) AS cnt
FROM    ratings
GROUP BY rating
ORDER BY cnt DESC
LIMIT 1;


-- QUERY 6:
-- How are ratings distributed by genre? 
INSERT OVERWRITE LOCAL DIRECTORY '/home/niamh/hadoop/assignment_1/ml-latest-small/output/6' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT genre, COUNT(rating), AVG(rating)
FROM genres
JOIN ratings ON (genres.movieId = ratings.movieId)
GROUP BY genre;

-- QUERY 7:
-- Popularity of genres overtime
INSERT OVERWRITE LOCAL DIRECTORY '/home/niamh/hadoop/assignment_1/ml-latest-small/output/7' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT yr, genre, COUNT(rating)
FROM genres
JOIN ratings ON (genres.movieId = ratings.movieId)
JOIN movies ON (genres.movieId = movies.movieId)
GROUP BY yr, genre
ORDER BY yr, genre;