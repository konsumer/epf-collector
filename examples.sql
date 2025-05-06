-- Get basic info for an app
SELECT
  application.application_id AS id,
  application.title,
  application.recommended_age,
  application.artist_name,
  application.seller_name,
  application.company_url,
  application.support_url,
  application.view_url,
  application.artwork_url_large,
  application.artwork_url_small,
  application.itunes_release_date,
  application.copyright,
  application.description,
  application.version,
  application.itunes_version,
  application.download_size
FROM application
WHERE application.application_id=479516143;

-- Get a joined-array app
SELECT
  app.application_id AS id,
  app.title,
  app.recommended_age,
  app.artist_name,
  app.seller_name,
  app.company_url,
  app.support_url,
  app.view_url,
  app.artwork_url_large,
  app.artwork_url_small,
  app.itunes_release_date,
  app.copyright,
  app.description,
  app.version,
  app.itunes_version,
  app.download_size,
  (SELECT ARRAY_AGG(language_code)
    FROM application_detail
    WHERE application_id = app.application_id) AS languages,
  (SELECT ARRAY_AGG(name)
    FROM genre
    JOIN genre_application ON genre.genre_id = genre_application.genre_id
    WHERE genre_application.application_id = app.application_id) AS genres
FROM application AS app
WHERE app.application_id = 479516143;


-- Get list of title, description & supported languages for an app
SELECT title, description, language_code from application_detail WHERE application_id=479516143;


-- Get genres for an app
SELECT
  genre.genre_id AS id,
  name as genre
FROM
  genre,
  genre_application
WHERE
  genre.genre_id = genre_application.genre_id AND
  genre_application.application_id=479516143;


-- Get all categories an app ranks for
SELECT
	storefront_id,
	genre_id,
	application_rank
FROM free_application_popularity_per_genre
WHERE application_id=479516143;


-- Get all categories an app ranks for, on just ipad
SELECT
	storefront_id,
	genre_id,
	application_rank
FROM free_ipad_application_popularity_per_genre
WHERE application_id=479516143;


-- Get top 200 free mobile apps in US
SELECT
    a.title,
  pop.application_id,
  pop.application_rank
FROM free_application_popularity_per_genre pop, application a
WHERE genre_id=36 AND storefront_id=143441 AND a.application_id=pop.application_id ORDER BY application_rank;


-- Get list of "mobile" genres
SELECT
	genre_id,
	name
FROM genre
WHERE parent_id=36;


-- Get list of "desktop" genres
SELECT
	genre_id,
	name
FROM genre
WHERE parent_id=39;


-- Get prices for an app, on every storefront
SELECT
  application_price.retail_price,
  application_price.currency_code,
  application_price.storefront_id AS storefront
FROM
  application_price
WHERE
  application_price.application_id=479516143;


-- ADVANCED: create a seperate table with keywords scores

-- get all search-term words from title & description
-- title terms get a score of 1.2, description terms get 1.
-- if the word occurs more than once, it gets a score of half of that per instance, after the initial

CREATE TABLE search_keywords (
  word VARCHAR,
  application_id BIGINT,
  score DOUBLE,
  storefront_id INTEGER,
  PRIMARY KEY (word, application_id, storefront_id)
);

-- Insert the results into the search_keywords table
INSERT INTO search_keywords (word, application_id, score, storefront_id)
WITH
-- Get distinct popular applications by storefront
popular_apps AS (
  SELECT DISTINCT
    application_id,
    storefront_id
  FROM free_application_popularity_per_genre WHERE genre_id=36
),

-- Extract and score words from titles, only for popular apps
title_words AS (
  SELECT
    a.application_id,
    p.storefront_id,
    lower(trim(word)) AS word,
    1.2 AS score
  FROM application a
  JOIN popular_apps p ON a.application_id = p.application_id,
  unnest(string_split(regexp_replace(a.title, '[^\w\s]', ' '), ' ')) AS t(word)
  WHERE length(trim(word)) > 0
),

-- Extract and score words from descriptions, only for popular apps
desc_words AS (
  SELECT
    a.application_id,
    p.storefront_id,
    lower(trim(word)) AS word,
    1.0 AS base_score
  FROM application a
  JOIN popular_apps p ON a.application_id = p.application_id,
  unnest(string_split(regexp_replace(a.description, '[^\w\s]', ' '), ' ')) AS d(word)
  WHERE length(trim(word)) > 0
),

-- Count occurrences in descriptions for scoring
desc_word_counts AS (
  SELECT
    application_id,
    storefront_id,
    word,
    count(*) AS occurrences,
    CASE
      WHEN count(*) = 1 THEN 1.0
      ELSE 1.0 + (count(*) - 1) * 0.5
    END AS score
  FROM desc_words
  GROUP BY application_id, storefront_id, word
),

-- Combine both sources
all_words AS (
  SELECT application_id, storefront_id, word, score FROM title_words
  UNION ALL
  SELECT application_id, storefront_id, word, score FROM desc_word_counts
)

-- Final aggregation
SELECT
  word,
  application_id,
  sum(score) AS score,
  storefront_id
FROM all_words
GROUP BY word, application_id, storefront_id;
