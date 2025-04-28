import {tables} from '../lib/schema.js'
function generateArrowSchemaCode(schemas) {
  let cppCode = `#include <arrow/api.h>
#include <string>
#include <stdexcept>

arrow::FieldVector get_schema(const std::string& name) {
  if (name == "") {
    throw std::runtime_error("Schema name cannot be empty");
  }
`;

  // Add each schema as a condition
  Object.keys(schemas).forEach(schemaName => {
    const schema = schemas[schemaName];
    const types = schema.types;
    
    cppCode += `  else if (name == "${schemaName}") {\n`;
    cppCode += `    arrow::FieldVector fields = {\n`;
    
    // Add each field in the schema
    Object.keys(types).forEach(fieldName => {
      const fieldType = types[fieldName];
      const parquetType = fieldType.parquet;
      
      // Map Parquet types to Arrow types
      let arrowType;
      switch (parquetType) {
        // Logical types
        case 'UTF8':
          arrowType = 'arrow::utf8()';
          break;
        case 'JSON':
          arrowType = 'arrow::utf8()';  // JSON is represented as string in Arrow
          break;
        case 'BSON':
          arrowType = 'arrow::binary()';  // BSON is binary in Arrow
          break;
        case 'BYTE_ARRAY':
          arrowType = 'arrow::binary()';
          break;
        case 'TIME_MILLIS':
          arrowType = 'arrow::time32(arrow::TimeUnit::MILLI)';
          break;
        case 'TIME_MICROS':
          arrowType = 'arrow::time64(arrow::TimeUnit::MICRO)';
          break;
        case 'TIMESTAMP_MILLIS':
          arrowType = 'arrow::timestamp(arrow::TimeUnit::MILLI)';
          break;
        case 'TIMESTAMP_MICROS':
          arrowType = 'arrow::timestamp(arrow::TimeUnit::MICRO)';
          break;
        case 'BOOLEAN':
          arrowType = 'arrow::boolean()';
          break;
        case 'FLOAT':
          arrowType = 'arrow::float32()';
          break;
        case 'DOUBLE':
          arrowType = 'arrow::float64()';
          break;
        case 'INT32':
          arrowType = 'arrow::int32()';
          break;
        case 'INT64':
          arrowType = 'arrow::int64()';
          break;
        case 'INT96':
          arrowType = 'arrow::timestamp(arrow::TimeUnit::NANO)';  // Best mapping for INT96
          break;
        case 'INT_8':
          arrowType = 'arrow::int8()';
          break;
        case 'INT_16':
          arrowType = 'arrow::int16()';
          break;
        case 'INT_32':
          arrowType = 'arrow::int32()';
          break;
        case 'INT_64':
          arrowType = 'arrow::int64()';
          break;
        case 'UINT_8':
          arrowType = 'arrow::uint8()';
          break;
        case 'UINT_16':
          arrowType = 'arrow::uint16()';
          break;
        case 'UINT_32':
          arrowType = 'arrow::uint32()';
          break;
        case 'UINT_64':
          arrowType = 'arrow::uint64()';
          break;
        default:
          arrowType = `arrow::utf8() /* Unmapped type: ${parquetType} */`;
          console.warn(`Warning: Unmapped Parquet type: ${parquetType}`);
      }
      
      cppCode += `      arrow::field("${fieldName}", ${arrowType}),\n`;
    });
    
    cppCode += `    };\n`;
    cppCode += `    return fields;\n`;
    cppCode += `  }\n`;
  });
  
  // Add the default case and closing brace
  cppCode += `  else {\n`;
  cppCode += `    throw std::runtime_error("Unknown schema: " + name);\n`;
  cppCode += `  }\n`;
  cppCode += `}\n`;
  
  return cppCode;
}

// Fix the else-if chain by removing the first 'else'
function fixElseIfChain(code) {
  return code.replace("  if (name == \"\") {\n    throw std::runtime_error(\"Schema name cannot be empty\");\n  }\n  else", 
                     "  if (name == \"\") {\n    throw std::runtime_error(\"Schema name cannot be empty\");\n  }");
}

const generatedCode = generateArrowSchemaCode(tables);
console.log(fixElseIfChain(generatedCode));