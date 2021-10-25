REGISTER '/home/niamh/hadoop/pig-0.17.0/contrib/piggybank/java/piggybank.jar';
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

-- load movies.csv
movies_orig = LOAD './ml-latest-small/movies.csv' using CSVExcelStorage() AS (movieId:int, title:chararray, genres:chararray);

-- current state: (movieId, title (year), list of genres seperated by '|')
-- e.g. (1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy)

-- Split genres on '|', and store in new table with only one genre per row (form: movieId, genre)
mId_genres = FOREACH movies_orig GENERATE movieId,FLATTEN(STRSPLIT(genres,'\\|'));
genres = FOREACH mId_genres GENERATE $0 AS movieId,FLATTEN(TOBAG($1..));

-- seperate title and year, and remove genres
movies_clean = FOREACH movies_orig GENERATE  
    movieId,
    REGEX_EXTRACT(title,'(.+?(?= \\([0-9][0-9][0-9][0-9]\\)))|(.+)',0) as title,
    REGEX_EXTRACT(title,'(?<=\\()[1-2][0-9][0-9][0-9](?=\\))',0) as year;

-- save tables
STORE movies_clean INTO './ml-latest-small/cleaned/movies.csv'  USING PigStorage('\t');
STORE genres INTO './ml-latest-small/cleaned/genres.csv'  USING PigStorage('\t');
