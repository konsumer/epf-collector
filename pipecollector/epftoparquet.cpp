// BUILD: g++ -o epftoparquet epftoparquet.cpp $(pkg-config --cflags --libs arrow parquet) -std=c++17

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/io/stdio.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <unistd.h>  // For STDOUT_FILENO

// Function to convert a string to the appropriate value based on Arrow type
arrow::Status appendToBuilder(const std::string& value, std::shared_ptr<arrow::DataType> type, arrow::ArrayBuilder* builder);

// Function to create an appropriate builder for an Arrow type
std::unique_ptr<arrow::ArrayBuilder> createBuilderForType(std::shared_ptr<arrow::DataType> type);

// Output a schema based on name
arrow::FieldVector get_schema(const std::string& name);

// the above functions are implemented
#include "./epf.h"


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




int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <schema_name> > <output_file.parquet>" << std::endl;
        return 1;
    }

    std::string schema_name = argv[1];

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

        // Open stdout for binary output
        auto result = arrow::io::FileOutputStream::Open(STDOUT_FILENO);
        if (!result.ok()) {
            throw std::runtime_error("Failed to open stdout: " + result.status().ToString());
        }
        std::shared_ptr<arrow::io::FileOutputStream> out_stream = result.ValueOrDie();

        // Write the table to Parquet
        PARQUET_THROW_NOT_OK(
            parquet::arrow::WriteTable(
                *table,
                arrow::default_memory_pool(),
                out_stream,
                /*chunk_size=*/1024
            )
        );

        std::cerr << "Successfully wrote " << rows.size() << " rows to stdout" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
