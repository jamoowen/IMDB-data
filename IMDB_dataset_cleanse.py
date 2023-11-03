import pyspark.sql.functions as psf

# dependencies - spark
# limitations - datasets are not comprehensive - only main crew members are listed
# ** Each table was created in a separate cell (On databricks)
# because there are many joins going on and many views, I rather write out the dataset then read back in == faster
# imdb datasets - title.basics.tsv.gz, title.crew.tsv.gz, title.principals.tsv.gz, title.ratings.tsv.gz, name.basics.tsv.gz

df_imdb_name = spark.read.csv(S3_PATH+'/JO/name.basics.tsv.gz', header=True, sep='\t', nullValue='\\N')
df_imdb_title = spark.read.csv(S3_PATH+'/JO/title.basics.tsv.gz', header=True, sep='\t', nullValue='\\N')
df_imdb_crew = spark.read.csv(S3_PATH+'/JO/title.crew.tsv.gz', header=True, sep='\t', nullValue='\\N')
df_imdb_rating = spark.read.csv(S3_PATH+'/JO/title.ratings.tsv.gz', header=True, sep='\t', nullValue='\\N')
df_imdb_principals = spark.read.csv(S3_PATH+'/JO/title.principals.tsv.gz', header=True, sep='\t', nullValue='\\N')

# principals = crew members (actor, dir, cameraman etc) for a given title(tconst)
# join with movies to get moviename and join with name dataset for crew names
df_principals_extended = df_imdb_principals.filter("category like 'act%' or category like 'dir%'") \
  .join(df_imdb_title, "tconst", 'left').filter("startYear > 1958 and titleType like '%movie%'") \
  .join(df_imdb_name, "nconst", 'left').filter("primaryName is not null") \
  .select("nconst", "tconst", "category", "characters", "primaryTitle","startYear", "runtimeMinutes", "genres", "primaryName", "birthYear",  "knownForTitles")

df_principals_extended.write.mode('overwrite').parquet(S3_PATH+'/JO/principals_extended.parquet')
df_principals_extended = spark.read.parquet(S3_PATH+'/JO/principals_extended.parquet')

# list of distinct actors with the movies they have appeared in
df_actors_distinct = df_principals_extended.filter("category like 'act%'") \
  .groupBy(["nconst", "primaryName", "birthYear"]).agg(psf.collect_set('primaryTitle').alias('knownFor'), psf.collect_set('tconst').alias('titlesTconst')) \
  .orderBy('primaryName')

df_actors_distinct.write.mode('overwrite').parquet(S3_PATH+'/JO/actors_distinct.parquet')
df_actors_distinct = spark.read.parquet(S3_PATH+'/JO/actors_distinct.parquet')

# finds list of distinct directors for search
df_directors_distinct = df_principals_extended.filter("category like 'dir%'") \
  .groupBy(["nconst", "primaryName", "birthYear"]).agg(psf.collect_set('primaryTitle').alias('knownFor'), psf.collect_set('tconst').alias('titlesTconst')) \
  .orderBy('primaryName').withColumnRenamed('nconst', 'dirconst')

df_directors_distinct.write.mode('overwrite').parquet(S3_PATH+'/JO/directors_distinct.parquet')
df_directors_distinct = spark.read.parquet(S3_PATH+'/JO/directors_distinct.parquet')

# each row is a movie with a 1 actor
df_movies_all = df_imdb_title.filter("lower(titleType) like 'movie' and startYear > 1958") \
  .join(df_directors_with_movies.selectExpr("primaryName as director", "tconst", "dirconst"), "tconst", "left").filter("dirconst is not null") \
  .join(df_actors_with_movies.selectExpr("nconst", "tconst", "primaryName"), "tconst", "left") \
  .filter("dirconst is not null or nconst is not null") \
  .select("tconst", "primaryTitle", "startYear", "runtimeMinutes", "genres", "dirconst", "director", "nconst","primaryName").orderBy('primaryTitle')

df_movies_all.write.mode('overwrite').parquet(S3_PATH+'/JO/movies_all.parquet')
df_movies_all = spark.read.parquet(S3_PATH+'/JO/movies_all.parquet')

# Find distinct movies (actors are grouped into an array)
df_movies_distinct = df_movies_all.groupBy(["tconst", "primaryTitle", "startYear", "runtimeMinutes", "genres", "dirconst", "director"]) \
  .agg(psf.collect_set('primaryName').alias('cast'), psf.collect_set('nconst').alias('castNconst')) \
  .groupBy(["tconst", "primaryTitle", "startYear", "runtimeMinutes", "genres", "castNconst", "cast"]) \
  .agg(psf.collect_set('director').alias('director'), psf.collect_set('dirconst').alias('dirconst')) \
  .orderBy('primaryTitle') 
  
df_movies_distinct.write.mode('overwrite').parquet(S3_PATH+'/JO/movies_distinct.parquet')
df_movies_distinct = spark.read.parquet(S3_PATH+'/JO/movies_distinct.parquet')

# ******** At this point, the files were ready, but the files needed to be uploaded as .csv
# .csv does not support the array type cols
# 3 main files
df_actors_distinct = spark.read.parquet(S3_PATH+'/JO/actors_distinct.parquet')
df_directors_distinct = spark.read.parquet(S3_PATH+'/JO/directors_distinct.parquet')
df_movies_distinct = spark.read.parquet(S3_PATH+'/JO/movies_distinct.parquet')

# Convert array columns to string representations
df_actors_distinct_csv = df_actors_distinct.withColumn("knownFor", psf.expr("concat_ws(',', knownFor)")).withColumn("titlesTconst", psf.expr("concat_ws(',', titlesTconst)"))
df_directors_distinct_csv = df_directors_distinct.withColumn("knownFor", psf.expr("concat_ws(',', knownFor)")).withColumn("titlesTconst", psf.expr("concat_ws(',', titlesTconst)"))
df_movies_distinct_dirs_csv = df_movies_distinct_dirs.withColumn("cast", psf.expr("concat_ws(',', cast)")).withColumn("castNconst", psf.expr("concat_ws(',', castNconst)")).withColumn("director", psf.expr("concat_ws(',', cast)")).withColumn("dirconst", psf.expr("concat_ws(',', cast)"))

df_actors_distinct_csv.coalesce(1).write.mode('overwrite').csv(S3_PATH+'/JO/actors_distinct_csv.csv', header=True, sep=',')
df_directors_distinct_csv.coalesce(1).write.mode('overwrite').csv(S3_PATH+'/JO/directors_distinct_csv.csv', header=True, sep=',')
df_movies_distinct_csv.coalesce(1).write.mode('overwrite').csv(S3_PATH+'/JO/movies_distinct_csv.csv', header=True, sep=',')