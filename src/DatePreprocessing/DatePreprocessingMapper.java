package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Converts inconsistent date formats in weather data to a single standardized format.
 * This is a MAP-ONLY transformation job where the mapper does all the work.
 */
public class DatePreprocessingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final Logger LOGGER = Logger.getLogger(DatePreprocessingMapper.class.getName());

    private final LongWritable outKey = new LongWritable();
    private final Text outValue = new Text();

    /**
     * MAP METHOD - Processes each line and standardizes date format
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Skip the header line in CSV
        if (key.get() == 0 && line.toLowerCase().startsWith("location_id")) {
            outKey.set(Long.MIN_VALUE);  // Guaranteed to sort before all other offsets
            outValue.set(line);
            context.write(outKey, outValue);
            LOGGER.info("Header line detected and written: " + line);
            return;
        }

        // Skip duplicate headers that may appear in middle of file
        if (line.toLowerCase().startsWith("location_id")) {
            LOGGER.info("Skipping duplicate header at offset " + key.get());
            return;
        }

        // Parse CSV line into fields and -1 parameter preserves trailing empty fields
        String[] parts = line.split(",", -1);

        // Debug logging for first few lines
        if (key.get() < 500) {
            LOGGER.info("Line structure at offset " + key.get() + ": Total columns=" + parts.length +
                    ", Column[0]=" + (parts.length > 0 ? parts[0] : "N/A") +
                    ", Column[1]=" + (parts.length > 1 ? parts[1] : "N/A") +
                    ", Column[2]=" + (parts.length > 2 ? parts[2] : "N/A"));
        }

        // Validate minimum field count is more than 2
        if (parts.length < 2) {
            LOGGER.warning("Skipping line with insufficient columns at offset " + key.get() + ": " + line);
            return;
        }

        try {
            // Extract date from column 1 (index 1)
            String originalDate = parts[1].trim();

            if (!originalDate.isEmpty() && !originalDate.equalsIgnoreCase("date")) {
                String standardizedDate = parseDate(originalDate);
                parts[1] = standardizedDate;  // Replace original date with standardized date
            }

            //Set original file offset as key (preserves order)
            outKey.set(key.get());
            LOGGER.info("Date converted: '" + originalDate + "' -> '" + Arrays.toString(parts) + "' at offset " + key.get());

            outValue.set(String.join(",", parts));
            context.write(outKey, outValue);

        } catch (Exception e) {
            // If date cannot be parsed, keep original line, and preserve data with bad date than lose entire record
            LOGGER.severe("ERROR: Could not parse date '" + parts[1] + "' at line offset " + key.get() + " - " + e.getMessage());
            LOGGER.severe("  Full line: " + line);
            outKey.set(key.get());
            outValue.set(line);
            context.write(outKey, outValue);
        }
    }

    private String parseDate(String input) throws Exception {
        // standardized output format for all dates
        SimpleDateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");

        // Input file possible input formats
        String[] formats = {
                "dd-MM-yyyy",   // Already in correct format
                "d-M-yyyy",     // Single digit with dash
                "yyyy-MM-dd",   // ISO format
                "M/d/yyyy",     // PRIMARY: mm/dd/yyyy format (1/1/2010, 12/31/2010)
                "MM/dd/yyyy",   // Double digit mm/dd/yyyy
                "d/M/yyyy",     // Single digit dd/mm/yyyy (fallback)
                "dd/MM/yyyy",   // Double digit dd/mm/yyyy (fallback)
                "dd-M-yyyy",    // Mixed dash formats
                "d-MM-yyyy",
                "yyyy/MM/dd",
                "d/MM/yyyy",
                "dd/M/yyyy"
        };

        /**
         * this for loop designed to as in the csv we don't know whether we have same date format.
         * Those dates might have multiple date format. If incorrect format found in this line "Date date = sdf.parse(input);"
         * it will call the catch block and ignored it and go to next line. And finally the exact dateformat found,
         * get that date and conver that into correct format we need.
         */
        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                // Rejects invalid dates
                // Ensures data quality
                sdf.setLenient(false);

                Date date = sdf.parse(input);
                return outputFormat.format(date);

            } catch (Exception ignored) {
                //Silent failure, will try next format
            }
        }
        throw new Exception("Unparseable date: " + input);
    }
}