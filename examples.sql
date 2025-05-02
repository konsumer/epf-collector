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


-- Get top free mobile apps in US
SELECT
	export_date,
	storefront_id,
	genre_id,
	application_id,
	application_rank
FROM free_application_popularity_per_genre
WHERE genre_id=36 AND storefront_id=143441 ORDER BY application_rank;


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
