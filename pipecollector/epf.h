// Function to convert a string to the appropriate value based on Arrow type
arrow::Status appendToBuilder(
    const std::string& value,
    std::shared_ptr<arrow::DataType> type,
    arrow::ArrayBuilder* builder) {

    // Handle null values
    if (value.empty()) {
        return builder->AppendNull();
    }

    switch (type->id()) {
        case arrow::Type::BOOL: {
            auto* bool_builder = static_cast<arrow::BooleanBuilder*>(builder);
            return bool_builder->Append(value == "true" || value == "1" || value == "TRUE");
        }
        case arrow::Type::INT8: {
            auto* int_builder = static_cast<arrow::Int8Builder*>(builder);
            return int_builder->Append(std::stoi(value));
        }
        case arrow::Type::INT16: {
            auto* int_builder = static_cast<arrow::Int16Builder*>(builder);
            return int_builder->Append(std::stoi(value));
        }
        case arrow::Type::INT32: {
            auto* int_builder = static_cast<arrow::Int32Builder*>(builder);
            return int_builder->Append(std::stoi(value));
        }
        case arrow::Type::INT64: {
            auto* int_builder = static_cast<arrow::Int64Builder*>(builder);
            return int_builder->Append(std::stoll(value));
        }
        case arrow::Type::UINT8: {
            auto* int_builder = static_cast<arrow::UInt8Builder*>(builder);
            return int_builder->Append(std::stoul(value));
        }
        case arrow::Type::UINT16: {
            auto* int_builder = static_cast<arrow::UInt16Builder*>(builder);
            return int_builder->Append(std::stoul(value));
        }
        case arrow::Type::UINT32: {
            auto* int_builder = static_cast<arrow::UInt32Builder*>(builder);
            return int_builder->Append(std::stoul(value));
        }
        case arrow::Type::UINT64: {
            auto* int_builder = static_cast<arrow::UInt64Builder*>(builder);
            return int_builder->Append(std::stoull(value));
        }
        case arrow::Type::FLOAT: {
            auto* float_builder = static_cast<arrow::FloatBuilder*>(builder);
            return float_builder->Append(std::stof(value));
        }
        case arrow::Type::DOUBLE: {
            auto* double_builder = static_cast<arrow::DoubleBuilder*>(builder);
            return double_builder->Append(std::stod(value));
        }
        case arrow::Type::STRING: {
            auto* string_builder = static_cast<arrow::StringBuilder*>(builder);
            return string_builder->Append(value);
        }
        case arrow::Type::BINARY: {
            auto* binary_builder = static_cast<arrow::BinaryBuilder*>(builder);
            return binary_builder->Append(value);
        }
        // Additional type conversions can be added as needed
        default:
            return arrow::Status::NotImplemented("Conversion not implemented for type: ",
                                                type->ToString());
    }
}

// Function to create an appropriate builder for an Arrow type
std::unique_ptr<arrow::ArrayBuilder> createBuilderForType(std::shared_ptr<arrow::DataType> type) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ArrayBuilder> builder;

    switch (type->id()) {
        case arrow::Type::BOOL:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::boolean(), &builder));
            break;
        case arrow::Type::INT8:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::int8(), &builder));
            break;
        case arrow::Type::INT16:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::int16(), &builder));
            break;
        case arrow::Type::INT32:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::int32(), &builder));
            break;
        case arrow::Type::INT64:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::int64(), &builder));
            break;
        case arrow::Type::UINT8:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::uint8(), &builder));
            break;
        case arrow::Type::UINT16:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::uint16(), &builder));
            break;
        case arrow::Type::UINT32:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::uint32(), &builder));
            break;
        case arrow::Type::UINT64:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::uint64(), &builder));
            break;
        case arrow::Type::FLOAT:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::float32(), &builder));
            break;
        case arrow::Type::DOUBLE:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::float64(), &builder));
            break;
        case arrow::Type::STRING:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::utf8(), &builder));
            break;
        case arrow::Type::BINARY:
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, arrow::binary(), &builder));
            break;
        // Add other type cases as needed
        default:
            throw std::runtime_error("Unsupported type: " + type->ToString());
    }

    return builder;
}

// Output a schema based on name
// output from genschema.js
arrow::FieldVector get_schema(const std::string& name) {
  if (name == "") {
    throw std::runtime_error("Schema name cannot be empty");
  } if (name == "video_price") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("retail_price", arrow::float64()),
      arrow::field("currency_code", arrow::utf8()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("availability_date", arrow::timestamp(arrow::TimeUnit::MILLI)),
      arrow::field("sd_price", arrow::float64()),
      arrow::field("hq_price", arrow::float64()),
      arrow::field("lc_rental_price", arrow::float64()),
      arrow::field("sd_rental_price", arrow::float64()),
      arrow::field("hd_rental_price", arrow::float64()),
      arrow::field("hd_price", arrow::float64()),
    };
    return fields;
  }
  else if (name == "collection_price") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("collection_id", arrow::int64()),
      arrow::field("retail_price", arrow::float64()),
      arrow::field("currency_code", arrow::utf8()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("availability_date", arrow::timestamp(arrow::TimeUnit::MILLI)),
      arrow::field("hq_price", arrow::float64()),
      arrow::field("preorder_price", arrow::float64()),
    };
    return fields;
  }
  else if (name == "application_price") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("retail_price", arrow::float64()),
      arrow::field("currency_code", arrow::utf8()),
      arrow::field("storefront_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "paid_ipad_application_popularity_per_genre") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("genre_id", arrow::int32()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("application_rank", arrow::int32()),
    };
    return fields;
  }
  else if (name == "paid_application_popularity_per_genre") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("genre_id", arrow::int32()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("application_rank", arrow::int32()),
    };
    return fields;
  }
  else if (name == "free_ipad_application_popularity_per_genre") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("genre_id", arrow::int32()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("application_rank", arrow::int32()),
    };
    return fields;
  }
  else if (name == "free_application_popularity_per_genre") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("genre_id", arrow::int32()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("application_rank", arrow::int32()),
    };
    return fields;
  }
  else if (name == "video_match") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("upc", arrow::utf8()),
      arrow::field("isrc", arrow::utf8()),
      arrow::field("amg_video_id", arrow::int32()),
      arrow::field("isan", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "collection_match") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("collection_id", arrow::int64()),
      arrow::field("upc", arrow::utf8()),
      arrow::field("grid", arrow::utf8()),
      arrow::field("amg_album_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "artist_match") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("amg_artist_id", arrow::utf8()),
      arrow::field("amg_video_artist_id", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "video_translation") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("language_code", arrow::utf8()),
      arrow::field("is_pronunciation", arrow::int32()),
      arrow::field("translation", arrow::utf8()),
      arrow::field("translation_type_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "video") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("name", arrow::utf8()),
      arrow::field("title_version", arrow::utf8()),
      arrow::field("search_terms", arrow::utf8()),
      arrow::field("parental_advisory_id", arrow::int32()),
      arrow::field("artist_display_name", arrow::utf8()),
      arrow::field("collection_display_name", arrow::utf8()),
      arrow::field("media_type_id", arrow::int32()),
      arrow::field("view_url", arrow::utf8()),
      arrow::field("artwork_url", arrow::utf8()),
      arrow::field("original_release_date", arrow::timestamp(arrow::TimeUnit::MILLI)),
      arrow::field("itunes_release_date", arrow::timestamp(arrow::TimeUnit::MILLI)),
      arrow::field("studio_name", arrow::utf8()),
      arrow::field("network_name", arrow::utf8()),
      arrow::field("content_provider_name", arrow::utf8()),
      arrow::field("track_length", arrow::int64()),
      arrow::field("copyright", arrow::utf8()),
      arrow::field("p_line", arrow::utf8()),
      arrow::field("short_description", arrow::utf8()),
      arrow::field("long_description", arrow::utf8()),
      arrow::field("episode_production_number", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "translation_type") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("translation_type_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "storefront") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("storefront_id", arrow::int32()),
      arrow::field("country_code", arrow::utf8()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "role") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("role_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "parental_advisory") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("parental_advisory_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "media_type") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("media_type_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "key_value") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("key_", arrow::utf8()),
      arrow::field("value_", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "genre_video") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("genre_id", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("is_primary", arrow::int32()),
    };
    return fields;
  }
  else if (name == "genre_collection") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("genre_id", arrow::int64()),
      arrow::field("collection_id", arrow::int64()),
      arrow::field("is_primary", arrow::int32()),
    };
    return fields;
  }
  else if (name == "genre_artist") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("genre_id", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("is_primary", arrow::int32()),
    };
    return fields;
  }
  else if (name == "genre_application") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("genre_id", arrow::int64()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("is_primary", arrow::int32()),
    };
    return fields;
  }
  else if (name == "genre") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("genre_id", arrow::int32()),
      arrow::field("parent_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "device_type") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("device_type_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "collection_video") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("collection_id", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("track_number", arrow::int64()),
      arrow::field("volume_number", arrow::int64()),
      arrow::field("preorder_only", arrow::int32()),
    };
    return fields;
  }
  else if (name == "collection_type") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("collection_type_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "collection_translation") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("collection_id", arrow::int64()),
      arrow::field("language_code", arrow::utf8()),
      arrow::field("is_pronunciation", arrow::int32()),
      arrow::field("translation", arrow::utf8()),
      arrow::field("translation_type_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "collection") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("collection_id", arrow::utf8()),
      arrow::field("name", arrow::utf8()),
      arrow::field("title_version", arrow::utf8()),
      arrow::field("search_terms", arrow::utf8()),
      arrow::field("parental_advisory_id", arrow::utf8()),
      arrow::field("artist_display_name", arrow::utf8()),
      arrow::field("view_url", arrow::utf8()),
      arrow::field("artwork_url", arrow::utf8()),
      arrow::field("original_release_date", arrow::utf8()),
      arrow::field("itunes_release_date", arrow::utf8()),
      arrow::field("label_studio", arrow::utf8()),
      arrow::field("content_provider_name", arrow::utf8()),
      arrow::field("copyright", arrow::utf8()),
      arrow::field("p_line", arrow::utf8()),
      arrow::field("media_type_id", arrow::utf8()),
      arrow::field("is_compilation", arrow::int32()),
      arrow::field("collection_type_id", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "artist_video") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("video_id", arrow::int64()),
      arrow::field("is_primary_artist", arrow::int32()),
      arrow::field("role_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "artist_type") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_type_id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
      arrow::field("primary_media_type_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "artist_translation") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("language_code", arrow::utf8()),
      arrow::field("is_pronunciation", arrow::int32()),
      arrow::field("translation", arrow::utf8()),
      arrow::field("translation_type_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "artist_collection") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("collection_id", arrow::int64()),
      arrow::field("is_primary_artist", arrow::int32()),
      arrow::field("role_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "artist_application") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("application_id", arrow::int64()),
    };
    return fields;
  }
  else if (name == "artist") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("artist_id", arrow::int64()),
      arrow::field("name", arrow::utf8()),
      arrow::field("is_actual_artist", arrow::int32()),
      arrow::field("view_url", arrow::utf8()),
      arrow::field("artist_type_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "application_device_type") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("device_type_id", arrow::int32()),
    };
    return fields;
  }
  else if (name == "application_detail") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("language_code", arrow::utf8()),
      arrow::field("title", arrow::utf8()),
      arrow::field("description", arrow::utf8()),
      arrow::field("release_notes", arrow::utf8()),
      arrow::field("company_url", arrow::utf8()),
      arrow::field("support_url", arrow::utf8()),
      arrow::field("screenshot_url_1", arrow::utf8()),
      arrow::field("screenshot_url_2", arrow::utf8()),
      arrow::field("screenshot_url_3", arrow::utf8()),
      arrow::field("screenshot_url_4", arrow::utf8()),
      arrow::field("screenshot_width_height_1", arrow::utf8()),
      arrow::field("screenshot_width_height_2", arrow::utf8()),
      arrow::field("screenshot_width_height_3", arrow::utf8()),
      arrow::field("screenshot_width_height_4", arrow::utf8()),
      arrow::field("ipad_screenshot_url_1", arrow::utf8()),
      arrow::field("ipad_screenshot_url_2", arrow::utf8()),
      arrow::field("ipad_screenshot_url_3", arrow::utf8()),
      arrow::field("ipad_screenshot_url_4", arrow::utf8()),
      arrow::field("ipad_screenshot_width_height_1", arrow::utf8()),
      arrow::field("ipad_screenshot_width_height_2", arrow::utf8()),
      arrow::field("ipad_screenshot_width_height_3", arrow::utf8()),
      arrow::field("ipad_screenshot_width_height_4", arrow::utf8()),
    };
    return fields;
  }
  else if (name == "application") {
    arrow::FieldVector fields = {
      arrow::field("export_date", arrow::int64()),
      arrow::field("application_id", arrow::int64()),
      arrow::field("title", arrow::utf8()),
      arrow::field("recommended_age", arrow::utf8()),
      arrow::field("artist_name", arrow::utf8()),
      arrow::field("seller_name", arrow::utf8()),
      arrow::field("company_url", arrow::utf8()),
      arrow::field("support_url", arrow::utf8()),
      arrow::field("view_url", arrow::utf8()),
      arrow::field("artwork_url_large", arrow::utf8()),
      arrow::field("artwork_url_small", arrow::utf8()),
      arrow::field("itunes_release_date", arrow::timestamp(arrow::TimeUnit::MILLI)),
      arrow::field("copyright", arrow::utf8()),
      arrow::field("description", arrow::utf8()),
      arrow::field("version", arrow::utf8()),
      arrow::field("itunes_version", arrow::utf8()),
      arrow::field("download_size", arrow::int64()),
    };
    return fields;
  }
  else {
    throw std::runtime_error("Unknown schema: " + name);
  }
}
