-- Query 1 --
CREATE TABLE query1 AS 
SELECT a.name, COUNT(c.movieid) AS moviecount
FROM genres a
INNER JOIN hasagenre b on b.genreid = a.genreid 
INNER JOIN movies c on c.movieid = b.movieid 
GROUP BY a.name

-- Query 2 --
CREATE TABLE query2 AS 
SELECT a.name, avg(d.rating) AS rating 
FROM genres a
INNER JOIN hasagenre b on b.genreid = a.genreid 
INNER JOIN movies c on c.movieid = b.movieid 
INNER JOIN ratings d on d.movieid = c.movieid
GROUP BY a.name

-- Query 3 --
CREATE TABLE query3 as
SELECT a.title, COUNT(b.rating) AS countofratings
FROM movies a
INNER JOIN ratings b on b.movieid = a.movieid
GROUP BY a.title
HAVING COUNT(b.rating)>=10

-- Query 4 --
CREATE TABLE query4 AS 
SELECT a.movieid, a.title
FROM movies a
INNER JOIN hasagenre b on b.movieid = a.movieid
INNER JOIN genres c on c.genreid = b.genreid 
WHERE c.name  = 'Comedy' 

-- Query 5 --
CREATE TABLE query5 as
SELECT b.title, avg(a.rating) AS average
FROM ratings a 
INNER JOIN movies b on a.movieid = b.movieid
GROUP BY b.title

-- Query 6 --
CREATE TABLE query6 as
SELECT AVG(a.rating) AS average
FROM ratings a
INNER JOIN hasagenre b on a.movieid = b.movieid
INNER JOIN genres c on b.genreid = c.genreid
WHERE c.name = 'Comedy'

-- Query 7 --
CREATE TABLE query7 as
SELECT AVG(a.rating) AS average
FROM ratings a
WHERE movieid in (
	SELECT movieid 
	FROM hasagenre b
	INNER JOIN genres c on b.genreid = c.genreid
	GROUP BY movieid 
	HAVING COUNT (
			CASE 
				WHEN c.name = 'Comedy' THEN 1
			END
		) = 1
		AND COUNT (
			CASE 
				WHEN c.name = 'Romance' THEN 1
			END
		) = 1
	)
	
-- Query 8 --
CREATE TABLE query8 as
SELECT AVG(a.rating) AS average
FROM ratings a
WHERE movieid in (
	SELECT movieid 
	FROM hasagenre b
	INNER JOIN genres c on b.genreid = c.genreid
	GROUP BY movieid 
	HAVING COUNT (
			CASE 
				WHEN c.name = 'Comedy' THEN 1
			END
		) = 0
		AND COUNT (
			CASE 
				WHEN c.name = 'Romance' THEN 1
			END
		) = 1
	)
	
-- Query 9 --	
CREATE TABLE query9 AS
SELECT movieid,rating
FROM ratings
WHERE userid = :v1;

-- Query 10 --
CREATE VIEW movies_rated_by_user AS
SELECT DISTINCT movies.movieid
FROM movies, ratings
WHERE movies.movieid = ratings.movieid AND userid = :v1;

CREATE VIEW movies_avg_ratings AS
SELECT movies.movieid, movies.title, avg_ratings.average
FROM(
        SELECT movieid,avg(rating) AS average
        FROM ratings
        GROUP BY movieid
    )	AS avg_ratings, movies
WHERE movies.movieid = avg_ratings.movieid;

CREATE VIEW user_rated_avg_to_other_cross AS
SELECT avg_ratings1.movieid,avg_ratings1.title,avg_ratings1.average AS average_i,avg_ratings2.average AS average_l,movies_rated_by_user.movieid AS movieid_rated_by_user
FROM movies_rated_by_user,movies_avg_ratings AS avg_ratings1, movies_avg_ratings AS avg_ratings2
WHERE movies_rated_by_user.movieid != avg_ratings1.movieid AND movies_rated_by_user.movieid = avg_ratings2.movieid;

CREATE VIEW similarity AS
SELECT user_rated_avg_to_other_cross.*,(1 - (abs(average_i - average_l)) / 5) AS sim
FROM user_rated_avg_to_other_cross;
CREATE VIEW predictions AS
SELECT movieid, sum(sim * rating) / sum(sim) AS p
FROM(
		SELECT ratings.rating,similarity.*
        FROM similarity,ratings
        WHERE ratings.userid = :v1AND ratings.movieid = similarity.movieid_rated_by_user
    ) AS user_similarity
GROUP BY user_similarity.movieid;

CREATE TABLE recommendation AS
SELECT movies.title
FROM predictions, movies
WHERE movies.movieid = predictions.movieid AND p > 3.9 AND predictions.movieid NOT IN (
        SELECT movieid
        FROM movies_rated_by_user
    );