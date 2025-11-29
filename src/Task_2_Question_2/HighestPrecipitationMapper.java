package Task_2_Question_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HighestPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();
    private boolean headerSkipped = false;
    private int processedLines = 0;
    private int skippedLines = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context){

        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        // Skip header line
        if (!headerSkipped) {
            System.err.println("Skipping header: " + line.substring(0, Math.min(100, line.length())));
            headerSkipped = true;
            return;
        }

        String[] fields = line.split(",");

        if (processedLines < 3) {
            System.err.println("=== Line " + processedLines + " ===");
            System.err.println("Total fields: " + fields.length);
            if (fields.length > 1) System.err.println("Field[1] (date): " + fields[1]);
            if (fields.length > 11) System.err.println("Field[11] (precip_sum): " + fields[11]);
        }

        // Need at least 12 fields (0-11)
        if (fields.length < 12) {
            skippedLines++;
            if (skippedLines <= 5) {
                System.err.println("SKIPPED - Not enough fields: " + fields.length);
            }
            return;
        }

        try {
            String date = fields[1].trim();            // date
            String precipStr = fields[11].trim();      // precipitation_sum (mm)

            // Parse date (format: dd-MM-yyyy)
            String[] dateParts = date.split("-");
            if (dateParts.length != 3) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid date format: " + date);
                }
                return;
            }

            // Format is dd-MM-yyyy
            String month = dateParts[1];
            String year = dateParts[2];

            // Parse precipitation
            double precipitation = parseDouble(precipStr);

            if (Double.isNaN(precipitation)) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid precipitation: " + precipStr);
                }
                return;
            }

            // Create key: Year-Month
            String keyStr = year + "-" + month;
            outputKey.set(keyStr);
            outputValue.set(precipitation);

            if (processedLines < 3) {
                System.err.println("EMITTING - Key: " + keyStr + ", Value: " + precipitation);
            }

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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER CLEANUP ===");
        System.err.println("Total lines processed: " + processedLines);
        System.err.println("Total lines skipped: " + skippedLines);
    }

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
