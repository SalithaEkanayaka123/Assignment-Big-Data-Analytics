package Task_2_Question_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
/**
 * Mapper Class: HighestPrecipitationMapper
 *
 * PURPOSE:
 * Processes weather observation records and groups precipitation data by Year-Month.
 * This is the first phase of finding the month with highest total precipitation.
 *
 * OUTPUT KEY: Text (format: "Year-Month", e.g., "2023-07")
 * OUTPUT VALUE: DoubleWritable (precipitation value, e.g., 45.2)
 *
 */
public class HighestPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Reusable objects to minimize object creation overhead
    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    // Tracking variables for monitoring and debugging
    private boolean headerSkipped = false;
    private int processedLines = 0;
    private int skippedLines = 0;

    /**
     * MAP METHOD - Processes each weather observation record
     *
     * CSV STRUCTURE (cleaned_weather.csv):
     * Field[1] = date (dd-MM-yyyy format)
     * Field[11] = precipitation_sum (mm)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context){

        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header (first line encountered)
        if (!headerSkipped) {
            System.err.println("Skipping header: " + line.substring(0, Math.min(100, line.length())));
            headerSkipped = true;
            return;
        }

        // Parse CSV line into fields
        String[] fields = line.split(",");

        // Debug logging for first few lines
        if (processedLines < 3) {
            System.err.println("=== Line " + processedLines + " ===");
            System.err.println("Total fields: " + fields.length);
            if (fields.length > 1) System.err.println("Field[1] (date): " + fields[1]);
            if (fields.length > 11) System.err.println("Field[11] (precip_sum): " + fields[11]);
        }

        // Validate minimum field count (need field 0-11, so at least 12 fields)
        if (fields.length < 12) {
            skippedLines++;
            if (skippedLines <= 5) {
                System.err.println("SKIPPED - Not enough fields: " + fields.length);
            }
            return;
        }

        try {
            // Extract relevant fields from CSV
            String date = fields[1].trim();            // date field (dd-MM-yyyy)
            String precipStr = fields[11].trim();      // precipitation_sum (mm)

            // Parse date - expected format: dd-MM-yyyy
            String[] dateParts = date.split("-");
            if (dateParts.length != 3) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid date format: " + date);
                }
                return;
            }

            // Extract month and year from dd-MM-yyyy format
            // dateParts[0] = day, dateParts[1] = month, dateParts[2] = year
            String month = dateParts[1];
            String year = dateParts[2];

            // Parse precipitation value
            double precipitation = parseDouble(precipStr);

            if (Double.isNaN(precipitation)) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid precipitation: " + precipStr);
                }
                return;
            }

            // set outputkey as this Format: "Year-Month" (e.g., "2023-07" for July 2023)
            // All daily observations from July 2023 will have the same key
            String keyStr = year + "-" + month;
            outputKey.set(keyStr);

            // USE DoubleWritable for VALUE - More efficient than Text, reducer can directly call value.get() without parsing
            outputValue.set(precipitation);

            // All observations from same month will be grouped together by Hadoop
            context.write(outputKey, outputValue);
            processedLines++;

        } catch (Exception e) {
            skippedLines++;
            if (skippedLines <= 10) {
                System.err.println("ERROR: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * CLEANUP METHOD - Called once after all records processed
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER CLEANUP ===");
        System.err.println("Total lines processed: " + processedLines);
        System.err.println("Total lines skipped: " + skippedLines);
    }

    /**
     * HELPER METHOD - Safe double parsing
     */
    private double parseDouble(String str) {
        try {
            if (str == null || str.isEmpty() || str.equalsIgnoreCase("null")) {
                return Double.NaN;
            }
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }
}
