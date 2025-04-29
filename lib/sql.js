// Simple script to output table structure

export const tables = {
  video_price: {
    keys: ['video_id', 'storefront_id'],
    types: {
      export_date: 'BIGINT',
      video_id: 'BIGINT',
      retail_price: 'DECIMAL(9,3)',
      currency_code: 'VARCHAR(20)',
      storefront_id: 'INTEGER',
      availability_date: 'DATETIME',
      sd_price: 'DECIMAL(9,3)',
      hq_price: 'DECIMAL(9,3)',
      lc_rental_price: 'DECIMAL(9,3)',
      sd_rental_price: 'DECIMAL(9,3)',
      hd_rental_price: 'DECIMAL(9,3)',
      hd_price: 'DECIMAL(9,3)'
    }
  },
  collection_price: {
    keys: ['collection_id', 'storefront_id'],
    types: {
      export_date: 'BIGINT',
      collection_id: 'BIGINT',
      retail_price: 'DECIMAL(9,3)',
      currency_code: 'VARCHAR(20)',
      storefront_id: 'INTEGER',
      availability_date: 'DATETIME',
      hq_price: 'DECIMAL(9,3)',
      preorder_price: 'DECIMAL(9,3)'
    }
  },
  application_price: {
    keys: ['application_id', 'storefront_id'],
    types: {
      export_date: 'BIGINT',
      application_id: 'BIGINT',
      retail_price: 'DECIMAL(9,3)',
      currency_code: 'VARCHAR(20)',
      storefront_id: 'INTEGER'
    }
  },
  paid_ipad_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: 'BIGINT',
      storefront_id: 'INTEGER',
      genre_id: 'INTEGER',
      application_id: 'BIGINT',
      application_rank: 'INTEGER'
    }
  },
  paid_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: 'BIGINT',
      storefront_id: 'INTEGER',
      genre_id: 'INTEGER',
      application_id: 'BIGINT',
      application_rank: 'INTEGER'
    }
  },
  free_ipad_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: 'BIGINT',
      storefront_id: 'INTEGER',
      genre_id: 'INTEGER',
      application_id: 'BIGINT',
      application_rank: 'INTEGER'
    }
  },
  free_application_popularity_per_genre: {
    keys: ['storefront_id', 'genre_id', 'application_id'],
    types: {
      export_date: 'BIGINT',
      storefront_id: 'INTEGER',
      genre_id: 'INTEGER',
      application_id: 'BIGINT',
      application_rank: 'INTEGER'
    }
  },
  video_match: {
    keys: ['video_id'],
    types: {
      export_date: 'BIGINT',
      video_id: 'BIGINT',
      upc: 'VARCHAR(200)',
      isrc: 'VARCHAR(200)',
      amg_video_id: 'INTEGER',
      isan: 'VARCHAR(200)'
    }
  },
  collection_match: {
    keys: ['collection_id'],
    types: {
      export_date: 'BIGINT',
      collection_id: 'BIGINT',
      upc: 'VARCHAR(200)',
      grid: 'VARCHAR(200)',
      amg_album_id: 'INTEGER'
    }
  },
  artist_match: {
    keys: ['artist_id'],
    types: {
      export_date: 'BIGINT',
      artist_id: 'BIGINT',
      amg_artist_id: 'VARCHAR(20)',
      amg_video_artist_id: 'VARCHAR(20)'
    }
  },
  video_translation: {
    keys: ['video_id', 'language_code', 'translation_type_id'],
    types: {
      export_date: 'BIGINT',
      video_id: 'BIGINT',
      language_code: 'VARCHAR(20)',
      is_pronunciation: 'INTEGER',
      translation: 'VARCHAR(1000)',
      translation_type_id: 'INTEGER'
    }
  },
  video: {
    keys: ['video_id'],
    types: {
      export_date: 'BIGINT',
      video_id: 'BIGINT',
      name: 'VARCHAR(1000)',
      title_version: 'VARCHAR(1000)',
      search_terms: 'VARCHAR(1000)',
      parental_advisory_id: 'INTEGER',
      artist_display_name: 'VARCHAR(1000)',
      collection_display_name: 'VARCHAR(1000)',
      media_type_id: 'INTEGER',
      view_url: 'VARCHAR(1000)',
      artwork_url: 'VARCHAR(1000)',
      original_release_date: 'DATETIME',
      itunes_release_date: 'DATETIME',
      studio_name: 'VARCHAR(1000)',
      network_name: 'VARCHAR(1000)',
      content_provider_name: 'VARCHAR(1000)',
      track_length: 'BIGINT',
      copyright: 'VARCHAR(4000)',
      p_line: 'VARCHAR(4000)',
      short_description: 'VARCHAR(1000)',
      long_description: 'VARCHAR(1000)',
      episode_production_number: 'VARCHAR(200)'
    }
  },
  translation_type: {
    keys: ['translation_type_id'],
    types: {
      export_date: 'BIGINT',
      translation_type_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  storefront: {
    keys: ['storefront_id'],
    types: {
      export_date: 'BIGINT',
      storefront_id: 'INTEGER',
      country_code: 'VARCHAR(10)',
      name: 'VARCHAR(200)'
    }
  },
  role: {
    keys: ['role_id'],
    types: {
      export_date: 'BIGINT',
      role_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  parental_advisory: {
    keys: ['parental_advisory_id'],
    types: {
      export_date: 'BIGINT',
      parental_advisory_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  media_type: {
    keys: ['media_type_id'],
    types: {
      export_date: 'BIGINT',
      media_type_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  key_value: {
    keys: ['key_'],
    types: {
      export_date: 'BIGINT',
      key_: 'VARCHAR(100)',
      value_: 'VARCHAR(100)'
    }
  },
  genre_video: {
    keys: ['genre_id', 'video_id'],
    types: {
      export_date: 'BIGINT',
      genre_id: 'BIGINT',
      video_id: 'BIGINT',
      is_primary: 'INTEGER'
    }
  },
  genre_collection: {
    keys: ['genre_id', 'collection_id'],
    types: {
      export_date: 'BIGINT',
      genre_id: 'BIGINT',
      collection_id: 'BIGINT',
      is_primary: 'INTEGER'
    }
  },
  genre_artist: {
    keys: ['genre_id', 'artist_id'],
    types: {
      export_date: 'BIGINT',
      genre_id: 'BIGINT',
      artist_id: 'BIGINT',
      is_primary: 'INTEGER'
    }
  },
  genre_application: {
    keys: ['genre_id', 'application_id'],
    types: {
      export_date: 'BIGINT',
      genre_id: 'BIGINT',
      application_id: 'BIGINT',
      is_primary: 'INTEGER'
    }
  },
  genre: {
    keys: ['genre_id'],
    types: {
      export_date: 'BIGINT',
      genre_id: 'INTEGER',
      parent_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  device_type: {
    keys: ['device_type_id'],
    types: {
      export_date: 'BIGINT',
      device_type_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  collection_video: {
    keys: ['collection_id', 'video_id'],
    types: {
      export_date: 'BIGINT',
      collection_id: 'BIGINT',
      video_id: 'BIGINT',
      track_number: 'BIGINT',
      volume_number: 'BIGINT',
      preorder_only: 'INTEGER'
    }
  },
  collection_type: {
    keys: ['collection_type_id'],
    types: {
      export_date: 'BIGINT',
      collection_type_id: 'INTEGER',
      name: 'VARCHAR(200)'
    }
  },
  collection_translation: {
    keys: ['collection_id', 'language_code', 'translation_type_id'],
    types: {
      export_date: 'BIGINT',
      collection_id: 'BIGINT',
      language_code: 'VARCHAR(20)',
      is_pronunciation: 'INTEGER',
      translation: 'VARCHAR(1000)',
      translation_type_id: 'INTEGER'
    }
  },
  collection: {
    keys: ['collection_id'],
    types: {
      export_date: 'BIGINT',
      collection_id: 'VARCHAR(1000)',
      name: 'VARCHAR(1000)',
      title_version: 'VARCHAR(1000)',
      search_terms: 'VARCHAR(1000)',
      parental_advisory_id: 'VARCHAR(1000)',
      artist_display_name: 'VARCHAR(1000)',
      view_url: 'VARCHAR(1000)',
      artwork_url: 'VARCHAR(1000)',
      original_release_date: 'VARCHAR(1000)',
      itunes_release_date: 'VARCHAR(1000)',
      label_studio: 'VARCHAR(1000)',
      content_provider_name: 'VARCHAR(1000)',
      copyright: 'VARCHAR(1000)',
      p_line: 'VARCHAR(1000)',
      media_type_id: 'VARCHAR(1000)',
      is_compilation: 'INTEGER',
      collection_type_id: 'VARCHAR(1000)'
    }
  },
  artist_video: {
    keys: ['artist_id', 'video_id'],
    types: {
      export_date: 'BIGINT',
      artist_id: 'BIGINT',
      video_id: 'BIGINT',
      is_primary_artist: 'INTEGER',
      role_id: 'INTEGER'
    }
  },
  artist_type: {
    keys: ['artist_type_id'],
    types: {
      export_date: 'BIGINT',
      artist_type_id: 'INTEGER',
      name: 'VARCHAR(200)',
      primary_media_type_id: 'INTEGER'
    }
  },
  artist_translation: {
    keys: ['artist_id', 'language_code', 'translation_type_id'],
    types: {
      export_date: 'BIGINT',
      artist_id: 'BIGINT',
      language_code: 'VARCHAR(20)',
      is_pronunciation: 'INTEGER',
      translation: 'VARCHAR(1000)',
      translation_type_id: 'INTEGER'
    }
  },
  artist_collection: {
    keys: ['artist_id', 'collection_id', 'role_id'],
    types: {
      export_date: 'BIGINT',
      artist_id: 'BIGINT',
      collection_id: 'BIGINT',
      is_primary_artist: 'INTEGER',
      role_id: 'INTEGER'
    }
  },
  artist_application: {
    keys: ['artist_id', 'application_id'],
    types: {
      export_date: 'BIGINT',
      artist_id: 'BIGINT',
      application_id: 'BIGINT'
    }
  },
  artist: {
    keys: ['artist_id'],
    types: {
      export_date: 'BIGINT',
      artist_id: 'BIGINT',
      name: 'VARCHAR(1000)',
      is_actual_artist: 'INTEGER',
      view_url: 'VARCHAR(1000)',
      artist_type_id: 'INTEGER'
    }
  },
  application_device_type: {
    keys: ['application_id', 'device_type_id'],
    types: {
      export_date: 'BIGINT',
      application_id: 'BIGINT',
      device_type_id: 'INTEGER'
    }
  },
  application_detail: {
    keys: ['application_id', 'language_code'],
    types: {
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
    }
  },
  application: {
    keys: ['application_id'],
    types: {
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
    }
  }
}

const [, , table] = process.argv

if (!table || !Object.keys(tables).includes(table)) {
  console.error('Usage: sql.js <TABLE>')
  console.log('  ' + Object.keys(tables).join('\n  '))
  process.exit(1)
}

const fields = []
for (const [field, type] of Object.entries(tables[table].types)) {
  fields.push(`${field} ${type}`)
}
if (tables[table].keys?.length) {
  fields.push(`PRIMARY KEY(${tables[table].keys.join(', ')})`)
}

console.log(`CREATE OR REPLACE TABLE ${table} (${fields.join(', ')});`)
