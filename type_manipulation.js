/**
 * Converts SQL-style types to Parquet schema types
 * @param {Object} sqlTypes - Object with field names as keys and SQL types as values
 * @param {string} libraryType - 'generic' or 'parquetjs' to specify output format
 * @returns {Object} Parquet schema definition
 */
export function sqlTypesToParquetSchema(sqlTypes, libraryType = 'generic') {
  const parquetSchema = {}

  for (const [field, sqlType] of Object.entries(sqlTypes)) {
    // Convert to uppercase and remove any parameters for comparison
    const baseType = sqlType.split('(')[0].toUpperCase()

    let parquetType

    if (baseType.includes('BIGINT')) {
      parquetSchema[field] = { type: 'INT64' }
      continue
    }

    if (baseType.includes('INT')) {
      parquetSchema[field] = { type: 'INT32' }
      continue
    }

    // Map SQL types to Parquet types
    switch (baseType) {
      case 'FLOAT':
      case 'REAL':
        parquetType = libraryType === 'parquetjs' ? { type: 'FLOAT' } : { type: 'FLOAT' }
        break

      case 'DOUBLE':
      case 'DECIMAL':
        parquetType = libraryType === 'parquetjs' ? { type: 'DOUBLE' } : { type: 'DOUBLE' }
        break

      case 'BIT':
      case 'BOOLEAN':
        parquetType = libraryType === 'parquetjs' ? { type: 'BOOLEAN' } : { type: 'BOOLEAN' }
        break

      case 'DATE':
        parquetType = libraryType === 'parquetjs' ? { type: 'DATE' } : { type: 'INT32', logicalType: 'DATE' }
        break

      case 'TIME':
        parquetType = libraryType === 'parquetjs' ? { type: 'TIME_MILLIS' } : { type: 'INT32', logicalType: 'TIME_MILLIS' }
        break

      case 'DATETIME':
      case 'TIMESTAMP':
        parquetType = libraryType === 'parquetjs' ? { type: 'TIMESTAMP_MILLIS' } : { type: 'INT64', logicalType: 'TIMESTAMP_MILLIS' }
        break

      case 'CHAR':
      case 'VARCHAR':
      case 'TEXT':
      case 'LONGTEXT':
      case 'TINYTEXT':
      case 'MEDIUMTEXT':
      case 'STRING':
        parquetType = libraryType === 'parquetjs' ? { type: 'UTF8' } : { type: 'BYTE_ARRAY', logicalType: 'UTF8' }
        break

      case 'BINARY':
      case 'VARBINARY':
      case 'BLOB':
      case 'LONGBLOB':
      case 'TINYBLOB':
      case 'MEDIUMBLOB':
        parquetType = libraryType === 'parquetjs' ? { type: 'BYTE_ARRAY' } : { type: 'BYTE_ARRAY' }
        break

      default:
        // Default to string for unknown types
        console.warn(`Unknown SQL type: ${sqlType} for field: ${field}, defaulting to string`)
        parquetType = libraryType === 'parquetjs' ? { type: 'UTF8' } : { type: 'BYTE_ARRAY', logicalType: 'UTF8' }
    }

    parquetSchema[field] = parquetType
  }

  // For parquetjs, we need to flatten the schema structure
  // if (libraryType === 'parquetjs') {
  //   for (const field in parquetSchema) {
  //     parquetSchema[field] = parquetSchema[field].type
  //   }
  // }

  return parquetSchema
}

const convertValue = (value, type) => {
  switch (type.split('(')[0]) {
    case 'LONGTEXT':
    case 'VARCHAR':
      return value
    case 'INTEGER':
    case 'INT32':
      return parseInt(value || 0)
    case 'BIGINT':
    case 'INT64':
      return BigInt(value || 0n)
    case 'DECIMAL':
    case 'FLOAT':
    case 'DOUBLE':
      return parseFloat(value || 0)
    case 'BOOLEAN':
      return value === 'true'
    case 'DATETIME':
    case 'DATE':
    case 'TIME_MILLIS':
    case 'TIMESTAMP_MILLIS':
      return new Date(value || 0)
    case 'BYTE_ARRAY':
      return Buffer.from(value, 'base64')
    default:
      throw new Error(`Unsupported type: ${type}`)
  }
}

// Converts string values to appropriate JavaScript types based on SQL type definitions and schema
export const mapValues = (row, types) => Object.entries(types).reduce((all, [field, type], index) => ({ ...all, [field]: convertValue(row[index], type) }), {})
