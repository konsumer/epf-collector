// This is similar to tables.js, but with parquet-types, if you need that

export const application_price = {
  export_date: { type: "INT64", optional: true },
  application_id: { type: "INT64", optional: false },  // Primary key
  retail_price: { type: "DOUBLE", optional: true },
  currency_code: { type: "UTF8", optional: true },
  storefront_id: { type: "INT32", optional: false }    // Primary key
};

export const paid_ipad_application_popularity_per_genre = {
  export_date: { type: "INT64", optional: true },
  storefront_id: { type: "INT32", optional: false },   // Primary key
  genre_id: { type: "INT32", optional: false },        // Primary key
  application_id: { type: "INT64", optional: false },  // Primary key
  application_rank: { type: "INT32", optional: true }
};

export const paid_application_popularity_per_genre = {
  export_date: { type: "INT64", optional: true },
  storefront_id: { type: "INT32", optional: false },   // Primary key
  genre_id: { type: "INT32", optional: false },        // Primary key
  application_id: { type: "INT64", optional: false },  // Primary key
  application_rank: { type: "INT32", optional: true }
};

export const free_ipad_application_popularity_per_genre = {
  export_date: { type: "INT64", optional: true },
  storefront_id: { type: "INT32", optional: false },   // Primary key
  genre_id: { type: "INT32", optional: false },        // Primary key
  application_id: { type: "INT64", optional: false },  // Primary key
  application_rank: { type: "INT32", optional: true }
};

export const free_application_popularity_per_genre = {
  export_date: { type: "INT64", optional: true },
  storefront_id: { type: "INT32", optional: false },   // Primary key
  genre_id: { type: "INT32", optional: false },        // Primary key
  application_id: { type: "INT64", optional: false },  // Primary key
  application_rank: { type: "INT32", optional: true }
};

export const storefront = {
  export_date: { type: "INT64", optional: true },
  storefront_id: { type: "INT32", optional: false },   // Primary key
  country_code: { type: "UTF8", optional: true },
  name: { type: "UTF8", optional: true }
};

export const key_value = {
  export_date: { type: "INT64", optional: true },
  key_: { type: "UTF8", optional: false },            // Primary key
  value_: { type: "UTF8", optional: true }
};

export const genre_application = {
  export_date: { type: "INT64", optional: true },
  genre_id: { type: "INT64", optional: false },       // Primary key
  application_id: { type: "INT64", optional: false },  // Primary key
  is_primary: { type: "INT32", optional: true }
};

export const genre = {
  export_date: { type: "INT64", optional: true },
  genre_id: { type: "INT32", optional: false },       // Primary key indicated from both original schema
  parent_id: { type: "INT32", optional: true },
  name: { type: "UTF8", optional: true }
};

export const device_type = {
  export_date: { type: "INT64", optional: true },
  device_type_id: { type: "INT32", optional: false },  // Primary key
  name: { type: "UTF8", optional: true }
};

export const artist_application = {
  export_date: { type: "INT64", optional: true },
  artist_id: { type: "INT64", optional: false },      // Primary key
  application_id: { type: "INT64", optional: false }   // Primary key
};

export const artist = {
  export_date: { type: "INT64", optional: true },
  artist_id: { type: "INT64", optional: false },      // Primary key
  name: { type: "UTF8", optional: true },
  is_actual_artist: { type: "INT32", optional: true },
  view_url: { type: "UTF8", optional: true },
  artist_type_id: { type: "INT32", optional: true }
};

export const application_device_type = {
  export_date: { type: "INT64", optional: true },
  application_id: { type: "INT64", optional: false },  // Primary key
  device_type_id: { type: "INT32", optional: false }   // Primary key
};

export const application_detail = {
  export_date: { type: "INT64", optional: true },
  application_id: { type: "INT64", optional: false },  // Primary key
  language_code: { type: "UTF8", optional: false },    // Primary key
  title: { type: "UTF8", optional: true },
  description: { type: "UTF8", optional: true },
  release_notes: { type: "UTF8", optional: true },
  company_url: { type: "UTF8", optional: true },
  support_url: { type: "UTF8", optional: true },
  screenshot_url_1: { type: "UTF8", optional: true },
  screenshot_url_2: { type: "UTF8", optional: true },
  screenshot_url_3: { type: "UTF8", optional: true },
  screenshot_url_4: { type: "UTF8", optional: true },
  screenshot_width_height_1: { type: "UTF8", optional: true },
  screenshot_width_height_2: { type: "UTF8", optional: true },
  screenshot_width_height_3: { type: "UTF8", optional: true },
  screenshot_width_height_4: { type: "UTF8", optional: true },
  ipad_screenshot_url_1: { type: "UTF8", optional: true },
  ipad_screenshot_url_2: { type: "UTF8", optional: true },
  ipad_screenshot_url_3: { type: "UTF8", optional: true },
  ipad_screenshot_url_4: { type: "UTF8", optional: true },
  ipad_screenshot_width_height_1: { type: "UTF8", optional: true },
  ipad_screenshot_width_height_2: { type: "UTF8", optional: true },
  ipad_screenshot_width_height_3: { type: "UTF8", optional: true },
  ipad_screenshot_width_height_4: { type: "UTF8", optional: true }
};

export const application = {
  export_date: { type: "INT64", optional: true },
  application_id: { type: "INT64", optional: false },  // Primary key
  title: { type: "UTF8", optional: true },
  recommended_age: { type: "UTF8", optional: true },
  artist_name: { type: "UTF8", optional: true },
  seller_name: { type: "UTF8", optional: true },
  company_url: { type: "UTF8", optional: true },
  support_url: { type: "UTF8", optional: true },
  view_url: { type: "UTF8", optional: true },
  artwork_url_large: { type: "UTF8", optional: true },
  artwork_url_small: { type: "UTF8", optional: true },
  itunes_release_date: { type: "TIMESTAMP_MILLIS", optional: true },
  copyright: { type: "UTF8", optional: true },
  description: { type: "UTF8", optional: true },
  version: { type: "UTF8", optional: true },
  itunes_version: { type: "UTF8", optional: true },
  download_size: { type: "INT64", optional: true }
};
