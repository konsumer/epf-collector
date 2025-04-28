// these are the sqlite tables for EPF data
// this info is also in the actual EPF files, but setting it up first is much more efficient

export const tables = {
  video_price: {
    keys: ['video_id', 'storefront_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      retail_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      currency_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      availability_date: {
        sql: 'DATETIME',
        parquet: 'TIMESTAMP_MILLIS'
      },
      sd_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      hq_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      lc_rental_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      sd_rental_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      hd_rental_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      hd_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      }
    }
  },
  collection_price: {
    keys: ['collection_id', 'storefront_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      retail_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      currency_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      availability_date: {
        sql: 'DATETIME',
        parquet: 'TIMESTAMP_MILLIS'
      },
      hq_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      preorder_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      }
    }
  },
  application_price: {
    keys: ['application_id', 'storefront_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      retail_price: {
        sql: 'DECIMAL(9,3)',
        parquet: 'DOUBLE'
      },
      currency_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  paid_ipad_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      genre_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_rank: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  paid_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      genre_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_rank: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  free_ipad_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      genre_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_rank: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  free_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      genre_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_rank: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  video_match: {
    keys: ['video_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      upc: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      },
      isrc: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      },
      amg_video_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      isan: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  collection_match: {
    keys: ['collection_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      upc: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      },
      grid: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      },
      amg_album_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  artist_match: {
    keys: ['artist_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      amg_artist_id: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      amg_video_artist_id: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      }
    }
  },
  video_translation: {
    keys: ['video_id', 'language_code', 'translation_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      language_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      is_pronunciation: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      translation: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      translation_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  video: {
    keys: ['video_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      title_version: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      search_terms: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      parental_advisory_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      artist_display_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      collection_display_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      media_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      view_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      artwork_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      original_release_date: {
        sql: 'DATETIME',
        parquet: 'TIMESTAMP_MILLIS'
      },
      itunes_release_date: {
        sql: 'DATETIME',
        parquet: 'TIMESTAMP_MILLIS'
      },
      studio_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      network_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      content_provider_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      track_length: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      copyright: {
        sql: 'VARCHAR(4000)',
        parquet: 'UTF8'
      },
      p_line: {
        sql: 'VARCHAR(4000)',
        parquet: 'UTF8'
      },
      short_description: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      long_description: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      episode_production_number: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  translation_type: {
    keys: ['translation_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      translation_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  storefront: {
    keys: ['storefront_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      storefront_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      country_code: {
        sql: 'VARCHAR(10)',
        parquet: 'UTF8'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  role: {
    keys: ['role_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      role_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  parental_advisory: {
    keys: ['parental_advisory_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      parental_advisory_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  media_type: {
    keys: ['media_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      media_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  key_value: {
    keys: ['key_'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      key_: {
        sql: 'VARCHAR(100)',
        parquet: 'UTF8'
      },
      value_: {
        sql: 'VARCHAR(100)',
        parquet: 'UTF8'
      }
    }
  },
  genre_video: {
    keys: ['genre_id', 'video_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      genre_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      is_primary: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  genre_collection: {
    keys: ['genre_id', 'collection_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      genre_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      is_primary: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  genre_artist: {
    keys: ['genre_id', 'artist_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      genre_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      is_primary: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  genre_application: {
    keys: ['genre_id', 'application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      genre_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      is_primary: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  genre: {
    keys: ['genre_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      genre_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      parent_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  device_type: {
    keys: ['device_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      device_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  collection_video: {
    keys: ['collection_id', 'video_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      track_number: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      volume_number: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      preorder_only: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  collection_type: {
    keys: ['collection_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      }
    }
  },
  collection_translation: {
    keys: ['collection_id', 'language_code', 'translation_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      language_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      is_pronunciation: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      translation: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      translation_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  collection: {
    keys: ['collection_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      title_version: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      search_terms: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      parental_advisory_id: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      artist_display_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      view_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      artwork_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      original_release_date: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      itunes_release_date: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      label_studio: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      content_provider_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      copyright: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      p_line: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      media_type_id: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      is_compilation: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      collection_type_id: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      }
    }
  },
  artist_video: {
    keys: ['artist_id', 'video_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      video_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      is_primary_artist: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      role_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  artist_type: {
    keys: ['artist_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      name: {
        sql: 'VARCHAR(200)',
        parquet: 'UTF8'
      },
      primary_media_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  artist_translation: {
    keys: ['artist_id', 'language_code', 'translation_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      language_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      is_pronunciation: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      translation: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      translation_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  artist_collection: {
    keys: ['artist_id', 'collection_id', 'role_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      collection_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      is_primary_artist: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      role_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  artist_application: {
    keys: ['artist_id', 'application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      }
    }
  },
  artist: {
    keys: ['artist_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      artist_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      is_actual_artist: {
        sql: 'INTEGER',
        parquet: 'INT32'
      },
      view_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      artist_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  application_device_type: {
    keys: ['application_id', 'device_type_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      device_type_id: {
        sql: 'INTEGER',
        parquet: 'INT32'
      }
    }
  },
  application_detail: {
    keys: ['application_id', 'language_code'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      language_code: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      title: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      description: {
        sql: 'LONGTEXT',
        parquet: 'UTF8'
      },
      release_notes: {
        sql: 'LONGTEXT',
        parquet: 'UTF8'
      },
      company_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      support_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      screenshot_url_1: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      screenshot_url_2: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      screenshot_url_3: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      screenshot_url_4: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      screenshot_width_height_1: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      screenshot_width_height_2: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      screenshot_width_height_3: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      screenshot_width_height_4: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      ipad_screenshot_url_1: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      ipad_screenshot_url_2: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      ipad_screenshot_url_3: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      ipad_screenshot_url_4: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      ipad_screenshot_width_height_1: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      ipad_screenshot_width_height_2: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      ipad_screenshot_width_height_3: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      ipad_screenshot_width_height_4: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      }
    }
  },
  application: {
    keys: ['application_id'],
    types: {
      export_date: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      application_id: {
        sql: 'BIGINT',
        parquet: 'INT64'
      },
      title: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      recommended_age: {
        sql: 'VARCHAR(20)',
        parquet: 'UTF8'
      },
      artist_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      seller_name: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      company_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      support_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      view_url: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      artwork_url_large: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      artwork_url_small: {
        sql: 'VARCHAR(1000)',
        parquet: 'UTF8'
      },
      itunes_release_date: {
        sql: 'DATETIME',
        parquet: 'TIMESTAMP_MILLIS'
      },
      copyright: {
        sql: 'VARCHAR(4000)',
        parquet: 'UTF8'
      },
      description: {
        sql: 'LONGTEXT',
        parquet: 'UTF8'
      },
      version: {
        sql: 'VARCHAR(100)',
        parquet: 'UTF8'
      },
      itunes_version: {
        sql: 'VARCHAR(100)',
        parquet: 'UTF8'
      },
      download_size: {
        sql: 'BIGINT',
        parquet: 'INT64'
      }
    }
  }
}

// Maps an array of string values to a typed object based on the provided schema
export function mapRow(table, values) {
  const schema = tables[table]
  const result = {}
  let i = 0
  for (const fieldName in schema.types) {
    result[fieldName] = convertStringToType(values[i++], schema.types[fieldName].parquet)
  }
  return result
}

// Converts a string value to the appropriate JavaScript type based on the Parquet type
export function convertStringToType(value, parquetType) {
  switch (parquetType) {
    case 'INT32':
    case 'INT_32':
    case 'UINT_32':
      return parseInt(value || 0)

    case 'INT64':
    case 'INT_64':
    case 'UINT_64':
    case 'INT96':
      return BigInt(value || 0)

    case 'INT8':
    case 'INT_8':
    case 'UINT_8':
    case 'INT16':
    case 'INT_16':
    case 'UINT_16':
      return parseInt(value || 0)

    case 'FLOAT':
    case 'DOUBLE':
      return parseFloat(value || 0)

    case 'BOOLEAN':
      return value.toLowerCase() === 'true'

    case 'TIMESTAMP_MILLIS':
    case 'TIMESTAMP_MICROS':
    case 'TIME_MILLIS':
    case 'TIME_MICROS':
      if (!value) {
        return 0
      }
      if (isNaN(value)) {
        return BigInt(new Date(value).getTime())
      }
      return BigInt(value)

    case 'UTF8':
    case 'JSON':
    case 'BSON':
      return value

    case 'BYTE_ARRAY':
      return value

    default:
      return value
  }
}
