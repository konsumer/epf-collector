// this was pulled form every EPF file, so I can lookup types (independant of file)
// it should not really be needed if you are streaming a file (the tables are in header)
// but it might help for reference, or cross-looking up types

export const application_price = {
  group: 'pricing',
  name: 'application_price',
  primaryKeys: ['application_id', 'storefront_id'],
  fields: {
    export_date: 'BIGINT',
    application_id: 'BIGINT',
    retail_price: 'DECIMAL(9,3)',
    currency_code: 'VARCHAR(20)',
    storefront_id: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const paid_ipad_application_popularity_per_genre = {
  group: 'popularity',
  name: 'paid_ipad_application_popularity_per_genre',
  primaryKeys: ['storefront_id', 'genre_id', 'application_id'],
  fields: {
    export_date: 'BIGINT',
    storefront_id: 'INTEGER',
    genre_id: 'INTEGER',
    application_id: 'BIGINT',
    application_rank: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const paid_application_popularity_per_genre = {
  group: 'popularity',
  name: 'paid_application_popularity_per_genre',
  primaryKeys: ['storefront_id', 'genre_id', 'application_id'],
  fields: {
    export_date: 'BIGINT',
    storefront_id: 'INTEGER',
    genre_id: 'INTEGER',
    application_id: 'BIGINT',
    application_rank: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const free_ipad_application_popularity_per_genre = {
  group: 'popularity',
  name: 'free_ipad_application_popularity_per_genre',
  primaryKeys: ['storefront_id', 'genre_id', 'application_id'],
  fields: {
    export_date: 'BIGINT',
    storefront_id: 'INTEGER',
    genre_id: 'INTEGER',
    application_id: 'BIGINT',
    application_rank: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const free_application_popularity_per_genre = {
  group: 'popularity',
  name: 'free_application_popularity_per_genre',
  primaryKeys: ['storefront_id', 'genre_id', 'application_id'],
  fields: {
    export_date: 'BIGINT',
    storefront_id: 'INTEGER',
    genre_id: 'INTEGER',
    application_id: 'BIGINT',
    application_rank: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const storefront = {
  group: 'itunes',
  name: 'storefront',
  primaryKeys: ['storefront_id'],
  fields: {
    export_date: 'BIGINT',
    storefront_id: 'INTEGER',
    country_code: 'VARCHAR(10)',
    name: 'VARCHAR(200)'
  },
  exportMode: 'FULL'
}
export const key_value = {
  group: 'itunes',
  name: 'key_value',
  primaryKeys: ['key_'],
  fields: {
    export_date: 'BIGINT',
    key_: 'VARCHAR(100)',
    value_: 'VARCHAR(100)'
  },
  exportMode: 'FULL'
}
export const genre_application = {
  group: 'itunes',
  name: 'genre_application',
  primaryKeys: ['genre_id', 'application_id'],
  fields: {
    export_date: 'BIGINT',
    genre_id: 'BIGINT',
    application_id: 'BIGINT',
    is_primary: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const genre = {
  group: 'itunes',
  name: 'genre',
  primaryKeys: ['device_type_id'],
  fields: {
    export_date: 'BIGINT',
    genre_id: 'INTEGER',
    parent_id: 'INTEGER',
    name: 'VARCHAR(200)'
  },
  exportMode: 'FULL'
}
export const device_type = {
  group: 'itunes',
  name: 'device_type',
  primaryKeys: ['device_type_id'],
  fields: {
    export_date: 'BIGINT',
    device_type_id: 'INTEGER',
    name: 'VARCHAR(200)'
  },
  exportMode: 'FULL'
}
export const artist_application = {
  group: 'itunes',
  name: 'artist_application',
  primaryKeys: ['artist_id', 'application_id'],
  fields: {
    export_date: 'BIGINT',
    artist_id: 'BIGINT',
    application_id: 'BIGINT'
  },
  exportMode: 'FULL'
}
export const artist = {
  group: 'itunes',
  name: 'artist',
  primaryKeys: ['artist_id'],
  fields: {
    export_date: 'BIGINT',
    artist_id: 'BIGINT',
    name: 'VARCHAR(1000)',
    is_actual_artist: 'INTEGER',
    view_url: 'VARCHAR(1000)',
    artist_type_id: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const application_device_type = {
  group: 'itunes',
  name: 'application_device_type',
  primaryKeys: ['application_id', 'device_type_id'],
  fields: {
    export_date: 'BIGINT',
    application_id: 'BIGINT',
    device_type_id: 'INTEGER'
  },
  exportMode: 'FULL'
}
export const application_detail = {
  group: 'itunes',
  name: 'application_detail',
  primaryKeys: ['application_id', 'language_code'],
  fields: {
    export_date: 'BIGINT',
    application_id: 'BIGINT',
    language_code: 'VARCHAR(20)',
    title: 'VARCHAR(1000)',
    description: 'LONGTEXT',
    release_notes: 'LONGTEXT',
    company_url: 'VARCHAR(1000)',
    support_url: 'VARCHAR(1000)',
    screenshot_url_1: 'VARCHAR(1000)',
    screenshot_url_2: 'VARCHAR(1000)',
    screenshot_url_3: 'VARCHAR(1000)',
    screenshot_url_4: 'VARCHAR(1000)',
    screenshot_width_height_1: 'VARCHAR(20)',
    screenshot_width_height_2: 'VARCHAR(20)',
    screenshot_width_height_3: 'VARCHAR(20)',
    screenshot_width_height_4: 'VARCHAR(20)',
    ipad_screenshot_url_1: 'VARCHAR(1000)',
    ipad_screenshot_url_2: 'VARCHAR(1000)',
    ipad_screenshot_url_3: 'VARCHAR(1000)',
    ipad_screenshot_url_4: 'VARCHAR(1000)',
    ipad_screenshot_width_height_1: 'VARCHAR(20)',
    ipad_screenshot_width_height_2: 'VARCHAR(20)',
    ipad_screenshot_width_height_3: 'VARCHAR(20)',
    ipad_screenshot_width_height_4: 'VARCHAR(20)'
  },
  exportMode: 'FULL'
}
export const application = {
  group: 'itunes',
  name: 'application',
  primaryKeys: ['application_id'],
  fields: {
    export_date: 'BIGINT',
    application_id: 'BIGINT',
    title: 'VARCHAR(1000)',
    recommended_age: 'VARCHAR(20)',
    artist_name: 'VARCHAR(1000)',
    seller_name: 'VARCHAR(1000)',
    company_url: 'VARCHAR(1000)',
    support_url: 'VARCHAR(1000)',
    view_url: 'VARCHAR(1000)',
    artwork_url_large: 'VARCHAR(1000)',
    artwork_url_small: 'VARCHAR(1000)',
    itunes_release_date: 'DATETIME',
    copyright: 'VARCHAR(4000)',
    description: 'LONGTEXT',
    version: 'VARCHAR(100)',
    itunes_version: 'VARCHAR(100)',
    download_size: 'BIGINT'
  },
  exportMode: 'FULL'
}
