package Task_2_Question_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Reducer Class: DistrictMonthlyStatsReducer
 *
 * PURPOSE:
 * Aggregates all temperature and precipitation values for each unique District-Year-Month
 * combination and calculates summary statistics (total precipitation, mean temperature).
 *
 * INPUT KEY: Text (format: "District-Year-Month", e.g., "Mumbai-2023-07")
 * INPUT VALUES: Iterable<Text> (multiple "temperature,precipitation" strings)
 *
 * OUTPUT KEY: Text (empty string for cleaner output)
 * OUTPUT VALUE: Text (human-readable formatted statistics)
 */
public class DistrictMonthlyStatsReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();
    private int keysProcessed = 0;

    /**
     * REDUCE METHOD - Called once for each unique key (District-Year-Month combination)
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (keysProcessed < 5) {
            System.err.println("=== REDUCER processing key: " + key.toString() + " ===");
        }

        // Accumulator variables for aggregation
        double tempSum = 0.0;      // Sum of all temperatures
        double precipSum = 0.0;    // Sum of all precipitation values
        int count = 0;             // Number of records for this key

        //  Process all values for this key
        for (Text value : values) {
            String[] parts = value.toString().split(",");

            if (keysProcessed < 3 && count < 3) {
                System.err.println("  Value " + count + ": " + value.toString());
            }

            // Parse the "temperature,precipitation" format
            if (parts.length == 2) {
                try {
                    double temp = Double.parseDouble(parts[0]);
                    double precip = Double.parseDouble(parts[1]);

                    // Accumulate sums for later averaging
                    tempSum += temp;
                    precipSum += precip;
                    count++;
                } catch (NumberFormatException e) {
                    System.err.println("  ERROR parsing: " + value.toString());
                    continue;  // Skip invalid values
                }
            }
        }

        // Calculate final statistics if we have valid data
        if (count > 0) {
            // MEAN TEMPERATURE calculation
            // Mean = Sum of all temperatures / Number of observations
            double meanTemp = tempSum / count;

            // Parse the composite key to extract components
            String[] keyParts = key.toString().split("-");
            if (keyParts.length >= 3) {
                String district = keyParts[0];
                String year = keyParts[1];
                String month = keyParts[2];

                // FORMAT OUTPUT - Create human-readable result
                // Shows: District, Total Precipitation, Mean Temperature, Month, Year
                String output = String.format("%s had a total precipitation of %.2f mm with a mean temperature of %.2f°C for month %s in year %s",
                        district, precipSum, meanTemp, month, year);

                // Use the original key (District-Year-Month) as output key
                outputValue.set(output);

                if (keysProcessed < 5) {
                    System.err.println("  Writing output: " + output);
                }

                // EMIT FINAL OUTPUT
                // Value contains the complete formatted message
                context.write(new Text(""), outputValue);  // Empty key for cleaner output
                keysProcessed++;
            }
        }
    }

    /**
     * CLEANUP METHOD - Called once per reducer task after processing all keys
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("=== REDUCER CLEANUP ===");
        System.err.println("Total keys processed: " + keysProcessed);
    }
}