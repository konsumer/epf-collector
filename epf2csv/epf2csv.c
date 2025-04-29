#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define MAX_LINE_SIZE 100000
#define MAX_FIELDS 1000
#define FIELD_SEPARATOR 0x01
#define ROW_DELIMITER_1 0x02
#define ROW_DELIMITER_2 '\n'

void output_csv_field(const char *field) {
    bool needs_quotes = false;
    size_t len = strlen(field);
    const char *p;

    // Check if field needs quotes (contains comma, quote or newline)
    for (p = field; *p; p++) {
        if (*p == ',' || *p == '"' || *p == '\n' || *p == '\r') {
            needs_quotes = true;
            break;
        }
    }

    if (needs_quotes) {
        putchar('"');
        for (p = field; *p; p++) {
            if (*p == '"') {
                // Double up quotes within quoted fields
                putchar('"');
                putchar('"');
            } else {
                putchar(*p);
            }
        }
        putchar('"');
    } else {
        printf("%s", field);
    }
}

int main() {
    char buffer[MAX_LINE_SIZE];
    char *fields[MAX_FIELDS];
    int max_fields = 0;
    int pos = 0;
    int ch;
    bool prev_was_delimiter1 = false;
    bool first_comment_processed = false;

    while ((ch = getchar()) != EOF) {
        // Check for row delimiter sequence (\x02\n)
        if (prev_was_delimiter1 && ch == ROW_DELIMITER_2) {
            // We've reached end of row, terminate the string
            // Note: we've already added \x02 to the buffer, so remove it
            if (pos > 0) pos--; // Remove the \x02 character
            buffer[pos] = '\0';

            // Handle comment lines
            if (buffer[0] == '#') {
                // Process only the first comment line as header
                if (!first_comment_processed) {
                    first_comment_processed = true;

                    // Skip the '#' character and any spaces after it
                    char *header_start = buffer + 1;
                    while (*header_start == ' ' || *header_start == '\t') {
                        header_start++;
                    }

                    // Parse header fields
                    int field_count = 0;
                    fields[field_count++] = header_start;

                    for (int i = 0; header_start[i]; i++) {
                        if (header_start[i] == FIELD_SEPARATOR) {
                            header_start[i] = '\0';
                            fields[field_count++] = &header_start[i + 1];
                            if (field_count >= MAX_FIELDS) {
                                fprintf(stderr, "Too many fields in header\n");
                                break;
                            }
                        }
                    }

                    // Track maximum number of fields
                    if (field_count > max_fields) {
                        max_fields = field_count;
                    }

                    // Output headers as CSV
                    for (int i = 0; i < field_count; i++) {
                        if (i > 0) {
                            putchar(',');
                        }
                        output_csv_field(fields[i]);
                    }
                    putchar('\n');
                }

                pos = 0;
                prev_was_delimiter1 = false;
                continue;
            }

            // Parse fields
            int field_count = 0;
            fields[field_count++] = buffer;

            for (int i = 0; buffer[i]; i++) {
                if (buffer[i] == FIELD_SEPARATOR) {
                    buffer[i] = '\0';
                    fields[field_count++] = &buffer[i + 1];
                    if (field_count >= MAX_FIELDS) {
                        fprintf(stderr, "Too many fields in row\n");
                        break;
                    }
                }
            }

            // Track maximum number of fields
            if (field_count > max_fields) {
                max_fields = field_count;
            }

            // Output as CSV
            for (int i = 0; i < field_count; i++) {
                if (i > 0) {
                    putchar(',');
                }
                output_csv_field(fields[i]);
            }

            // Add columns if needed to maintain consistent column count
            for (int i = field_count; i < max_fields; i++) {
                putchar(',');
            }

            // End the CSV row
            putchar('\n');

            // Reset buffer position for next row
            pos = 0;
            prev_was_delimiter1 = false;
        } else {
            if (ch == ROW_DELIMITER_1) {
                prev_was_delimiter1 = true;
            } else {
                prev_was_delimiter1 = false;
            }

            // Add character to buffer
            if (pos < MAX_LINE_SIZE - 1) {
                buffer[pos++] = ch;
            } else {
                if (!prev_was_delimiter1) {
                    // Only print the error once per line
                    fprintf(stderr, "Line too long, truncating\n");
                }
                // Continue collecting characters but don't store them
            }
        }
    }

    return 0;
}
