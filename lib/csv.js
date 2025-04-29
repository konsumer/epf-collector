// this is a stream that outputs well-formed CSV that duck is happy with

import { Transform } from 'node:stream'

export default function createCSVStream(options = {}) {
  const {
    headers = true,
    delimiter = ',',
    columns = null
  } = options;

  let headerWritten = false;
  let knownColumns = columns ? [...columns] : [];

  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      try {
        // If columns weren't specified, extract them from the first object
        if (!knownColumns.length && chunk) {
          knownColumns = Object.keys(chunk);
        }

        // Write headers if needed
        if (headers && !headerWritten && knownColumns.length) {
          const headerRow = knownColumns
            .map(column => escapeCSVField(column, delimiter))
            .join(delimiter);
          this.push(headerRow + '\n');
          headerWritten = true;
        }

        // Convert object to CSV row
        const row = knownColumns
          .map(column => {
            // Use empty string for undefined/null values
            const value = chunk[column] ?? '';
            return escapeCSVField(value, delimiter);
          })
          .join(delimiter);

        this.push(row + '\n');
        callback();
      } catch (error) {
        callback(error);
      }
    }
  });
}

function escapeCSVField(field, delimiter) {
  // Return empty string for null/undefined
  if (field === null || field === undefined) {
    return '';
  }

  const stringField = String(field);

  // Check if the field needs to be quoted
  if (
    stringField.includes(delimiter) ||
    stringField.includes('"') ||
    stringField.includes('\n') ||
    stringField.includes('\r')
  ) {
    // Double up any quotes and wrap in quotes
    return '"' + stringField.replace(/"/g, '""') + '"';
  }

  return stringField;
}
