package Task_2_Question_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Reducer Class: HighestPrecipitationReducer
 *
 * PURPOSE:
 * This reducer performs a TWO-PHASE operation:
 * PHASE 1 (reduce()): Calculate total precipitation for each month-year
 * PHASE 2 (cleanup()): Find the month with MAXIMUM total precipitation
 *
 * INPUT KEY: Text (format: "Year-Month", e.g., "2023-07")
 * INPUT VALUES: Iterable<DoubleWritable> (multiple precipitation values)
 *
 * OUTPUT KEY: Text ("Result")
 * OUTPUT VALUE: Text (formatted message about the highest precipitation month)
 *
 */
public class HighestPrecipitationReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    // Instance variables to track the GLOBAL maximum across ALL months
    // These persist across all reduce() calls within this reducer instance
    private String maxMonthYear = "";                    // Stores "Year-Month" of maximum
    private double maxPrecipitation = Double.MIN_VALUE;  // Stores the maximum precipitation value
    private int keysProcessed = 0;                       // Tracks how many months processed

    /**
     * REDUCE METHOD - Called once for each unique Year-Month combination
     */
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        // Debug logging for first 5 keys
        if (keysProcessed < 5) {
            System.err.println("=== REDUCER processing key: " + key.toString() + " ===");
        }

        double totalPrecipitation = 0.0;
        int count = 0;

        // AGGREGATION LOOP - Sum all precipitation values for this month
        // Values come from multiple daily observations (or multiple mappers)
        for (DoubleWritable value : values) {
            totalPrecipitation += value.get();  // DoubleWritable.get() returns double
            count++;
        }

        // Debug logging
        if (keysProcessed < 5) {
            System.err.println("  Total precipitation: " + totalPrecipitation + " (from " + count + " records)");
        }

        // COMPARISON - Check if this month has the highest precipitation so far
        // This is the "SELECTION" part of the algorithm
        if (totalPrecipitation > maxPrecipitation) {
            // Found a new maximum!
            maxPrecipitation = totalPrecipitation;
            maxMonthYear = key.toString();

            System.err.println("*** NEW MAX FOUND: " + maxMonthYear + " with " + maxPrecipitation + " mm ***");
        }

        keysProcessed++;
    }

    /**
     * CLEANUP METHOD - Called ONCE after all reduce() calls complete
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("\n=== REDUCER CLEANUP ===");
        System.err.println("Total keys processed: " + keysProcessed);
        System.err.println("Maximum precipitation: " + maxPrecipitation + " mm");
        System.err.println("Month-Year: " + maxMonthYear);

        // Emit the final result only if we found valid data
        if (!maxMonthYear.isEmpty()) {
            // Parse the Year-Month key
            String[] parts = maxMonthYear.split("-");
            if (parts.length == 2) {
                String year = parts[0];
                String month = parts[1];

                //FORMAT OUTPUT - Create human-readable result
                // Example: "7th month in 2023 had the highest total precipitation of 1234.56 mm"
                String output = String.format("%s month in %s had the highest total precipitation of %.2f mm",
                        getMonthName(month), year, maxPrecipitation);

                // EMIT FINAL RESULT
                // Key: "Result" (just a label)
                // Value: Formatted message
                context.write(new Text("Result"), new Text(output));

                System.err.println("\n=== FINAL ANSWER ===");
                System.err.println(output);
            }
        }
    }

    /**
     * HELPER METHOD - Convert month number to ordinal format
     *
     * POINTS:
     * - Converts "07" to "7th", "01" to "1st", etc.
     * - Makes output more readable for business users
     * - Handles English ordinal suffixes (st, nd, rd, th)
     *
     * EXAMPLES:
     * 1 -> "1st", 2 -> "2nd", 3 -> "3rd", 4 -> "4th"
     * 11 -> "11th", 12 -> "12th", 21 -> "21st"
     */
    private String getMonthName(String monthNum) {
        // Return ordinal format (1st, 2nd, 3rd, etc.)
        int month = Integer.parseInt(monthNum);

        // Special case for 11th, 12th, 13th (all use "th")
        if (month >= 11 && month <= 13) {
            return month + "th";
        }

        // Check last digit for suffix
        switch (month % 10) {
            case 1: return month + "st";  // 1st, 21st, 31st
            case 2: return month + "nd";  // 2nd, 22nd
            case 3: return month + "rd";  // 3rd, 23rd
            default: return month + "th"; // 4th, 5th, 6th, etc.
        }
    }
}