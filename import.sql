CREATE TABLE application_price AS SELECT * FROM read_csv('data/csv/application_price.csv',columns={'export_date': 'BIGINT', 'application_id': 'BIGINT', 'retail_price': 'DECIMAL(9,3)', 'currency_code': 'VARCHAR(20)', 'storefront_id': 'INTEGER'});

CREATE TABLE storefront AS SELECT * FROM read_csv('data/csv/storefront.csv',columns={'export_date': 'BIGINT', 'storefront_id': 'INTEGER', 'country_code': 'VARCHAR(10)', 'name': 'VARCHAR(200)'});

CREATE TABLE key_value AS SELECT * FROM read_csv('data/csv/key_value.csv',columns={'export_date': 'BIGINT', 'key_': 'VARCHAR(100)', 'value_': 'VARCHAR(100)'});

CREATE TABLE genre_application AS SELECT * FROM read_csv('data/csv/genre_application.csv',columns={'export_date': 'BIGINT', 'genre_id': 'BIGINT', 'application_id': 'BIGINT', 'is_primary': 'INTEGER'});

CREATE TABLE genre AS SELECT * FROM read_csv('data/csv/genre.csv',columns={'export_date': 'BIGINT', 'genre_id': 'INTEGER', 'parent_id': 'INTEGER', 'name': 'VARCHAR(200)'});

CREATE TABLE device_type AS SELECT * FROM read_csv('data/csv/device_type.csv',columns={'export_date': 'BIGINT', 'device_type_id': 'INTEGER', 'name': 'VARCHAR(200)'});

CREATE TABLE artist_application AS SELECT * FROM read_csv('data/csv/artist_application.csv',columns={'export_date': 'BIGINT', 'artist_id': 'BIGINT', 'application_id': 'BIGINT'});

CREATE TABLE artist AS SELECT * FROM read_csv('data/csv/artist.csv',columns={'export_date': 'BIGINT', 'artist_id': 'BIGINT', 'name': 'VARCHAR(1000)', 'is_actual_artist': 'INTEGER', 'view_url': 'VARCHAR(1000)', 'artist_type_id': 'INTEGER'});

CREATE TABLE application_device_type AS SELECT * FROM read_csv('data/csv/application_device_type.csv',columns={'export_date': 'BIGINT', 'application_id': 'BIGINT', 'device_type_id': 'INTEGER'});

CREATE TABLE application_detail AS SELECT * FROM read_csv('data/csv/application_detail.csv',columns={'export_date': 'BIGINT', 'application_id': 'BIGINT', 'language_code': 'VARCHAR(20)', 'title': 'VARCHAR(1000)', 'description': 'TEXT', 'release_notes': 'TEXT', 'company_url': 'VARCHAR(1000)', 'support_url': 'VARCHAR(1000)', 'screenshot_url_1': 'VARCHAR(1000)', 'screenshot_url_2': 'VARCHAR(1000)', 'screenshot_url_3': 'VARCHAR(1000)', 'screenshot_url_4': 'VARCHAR(1000)', 'screenshot_width_height_1': 'VARCHAR(20)', 'screenshot_width_height_2': 'VARCHAR(20)', 'screenshot_width_height_3': 'VARCHAR(20)', 'screenshot_width_height_4': 'VARCHAR(20)', 'ipad_screenshot_url_1': 'VARCHAR(1000)', 'ipad_screenshot_url_2': 'VARCHAR(1000)', 'ipad_screenshot_url_3': 'VARCHAR(1000)', 'ipad_screenshot_url_4': 'VARCHAR(1000)', 'ipad_screenshot_width_height_1': 'VARCHAR(20)', 'ipad_screenshot_width_height_2': 'VARCHAR(20)', 'ipad_screenshot_width_height_3': 'VARCHAR(20)', 'ipad_screenshot_width_height_4': 'VARCHAR(20)'});

CREATE TABLE application AS SELECT * FROM read_csv('data/csv/application.csv',columns={'export_date': 'BIGINT', 'application_id': 'BIGINT', 'title': 'VARCHAR(1000)', 'recommended_age': 'VARCHAR(20)', 'artist_name': 'VARCHAR(1000)', 'seller_name': 'VARCHAR(1000)', 'company_url': 'VARCHAR(1000)', 'support_url': 'VARCHAR(1000)', 'view_url': 'VARCHAR(1000)', 'artwork_url_large': 'VARCHAR(1000)', 'artwork_url_small': 'VARCHAR(1000)', 'itunes_release_date': 'DATETIME', 'copyright': 'VARCHAR(4000)', 'description': 'TEXT', 'version': 'VARCHAR(100)', 'itunes_version': 'VARCHAR(100)', 'download_size': 'BIGINT'});

CREATE TABLE paid_ipad_application_popularity_per_genre AS SELECT * FROM read_csv('data/csv/paid_ipad_application_popularity_per_genre.csv',columns={'export_date': 'BIGINT', 'storefront_id': 'INTEGER', 'genre_id': 'INTEGER', 'application_id': 'BIGINT', 'application_rank': 'INTEGER'});

CREATE TABLE paid_application_popularity_per_genre AS SELECT * FROM read_csv('data/csv/paid_application_popularity_per_genre.csv',columns={'export_date': 'BIGINT', 'storefront_id': 'INTEGER', 'genre_id': 'INTEGER', 'application_id': 'BIGINT', 'application_rank': 'INTEGER'});

CREATE TABLE free_ipad_application_popularity_per_genre AS SELECT * FROM read_csv('data/csv/free_ipad_application_popularity_per_genre.csv',columns={'export_date': 'BIGINT', 'storefront_id': 'INTEGER', 'genre_id': 'INTEGER', 'application_id': 'BIGINT', 'application_rank': 'INTEGER'});

CREATE TABLE free_application_popularity_per_genre AS SELECT * FROM read_csv('data/csv/free_application_popularity_per_genre.csv',columns={'export_date': 'BIGINT', 'storefront_id': 'INTEGER', 'genre_id': 'INTEGER', 'application_id': 'BIGINT', 'application_rank': 'INTEGER'});
