// BUILD: g++ -o epftoparquet epftoparquet.cpp $(pkg-config --cflags --libs arrow parquet) -std=c++17

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_map>

// Record separator: \x02\n
const char RECORD_SEP[] = {0x02, '\n'};
// Field separator: \x01
const char FIELD_SEP = 0x01;

// Define a function to split a string by separator
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to read data from stdin with the specified format
std::vector<std::vector<std::string>> readDataFromStdin() {
    std::vector<std::vector<std::string>> rows;
    std::string line;
    std::string buffer;
    char c;
    
    while (std::cin.get(c)) {
        buffer += c;
        
        // Check if we have a record separator
        if (buffer.size() >= 2 && 
            buffer[buffer.size() - 2] == RECORD_SEP[0] && 
            buffer[buffer.size() - 1] == RECORD_SEP[1]) {
            
            // Extract the line (without the separator)
            line = buffer.substr(0, buffer.size() - 2);
            buffer.clear();
            
            // Skip commented lines
            if (!line.empty() && line[0] == '#') {
                continue;
            }
            
            // Split the line into fields
            std::vector<std::string> fields = split(line, FIELD_SEP);
            if (!fields.empty()) {
                rows.push_back(fields);
            }
        }
    }
    
    // Handle any remaining data
    if (!buffer.empty()) {
        // Skip commented lines
        if (buffer[0] != '#') {
            std::vector<std::string> fields = split(buffer, FIELD_SEP);
            if (!fields.empty()) {
                rows.push_back(fields);
            }
        }
    }
    
    return rows;
}

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


int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <schema_name> <output_file.parquet>" << std::endl;
        return 1;
    }
    
    std::string schema_name = argv[1];
    std::string output_file = argv[2];
    
    try {
        // Get schema
        arrow::FieldVector fields = get_schema(schema_name);
        auto schema = std::make_shared<arrow::Schema>(fields);
        
        // Read and parse the data
        std::vector<std::vector<std::string>> rows = readDataFromStdin();
        
        if (rows.empty()) {
            std::cerr << "No data to write" << std::endl;
            return 1;
        }
        
        // Create builders for each column
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
        for (const auto& field : schema->fields()) {
            builders.push_back(createBuilderForType(field->type()));
        }
        
        // Parse each row
        for (const auto& row : rows) {
            // Ensure the row has the correct number of columns
            if (row.size() != fields.size()) {
                std::cerr << "Warning: Row has " << row.size() 
                          << " fields, but schema has " << fields.size() 
                          << " fields. Skipping row." << std::endl;
                continue;
            }
            
            // Add each value to the appropriate builder
            for (size_t i = 0; i < row.size(); i++) {
                PARQUET_THROW_NOT_OK(
                    appendToBuilder(row[i], fields[i]->type(), builders[i].get())
                );
            }
        }
        
        // Finish the arrays
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (auto& builder : builders) {
            std::shared_ptr<arrow::Array> array;
            PARQUET_THROW_NOT_OK(builder->Finish(&array));
            arrays.push_back(array);
        }
        
        // Convert to chunked arrays
        std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
        for (const auto& array : arrays) {
            chunked_arrays.push_back(std::make_shared<arrow::ChunkedArray>(array));
        }
        
        // Create table
        auto table = arrow::Table::Make(schema, chunked_arrays);
        
        // Open output file - without using the problematic macro
        auto result = arrow::io::FileOutputStream::Open(output_file);
        if (!result.ok()) {
            throw std::runtime_error("Failed to open output file: " + result.status().ToString());
        }
        std::shared_ptr<arrow::io::FileOutputStream> outfile = result.ValueOrDie();
        
        // Write the table to Parquet
        PARQUET_THROW_NOT_OK(
            parquet::arrow::WriteTable(
                *table, 
                arrow::default_memory_pool(), 
                outfile, 
                /*chunk_size=*/1024
            )
        );
        
        std::cout << "Successfully wrote " << rows.size() << " rows to " 
                  << output_file << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}