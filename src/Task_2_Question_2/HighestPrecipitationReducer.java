package Task_2_Question_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class HighestPrecipitationReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private String maxMonthYear = "";
    private double maxPrecipitation = Double.MIN_VALUE;
    private int keysProcessed = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        if (keysProcessed < 5) {
            System.err.println("=== REDUCER processing key: " + key.toString() + " ===");
        }

        double totalPrecipitation = 0.0;
        int count = 0;

        for (DoubleWritable value : values) {
            totalPrecipitation += value.get();
            count++;
        }

        if (keysProcessed < 5) {
            System.err.println("  Total precipitation: " + totalPrecipitation + " (from " + count + " records)");
        }

        // Track the maximum
        if (totalPrecipitation > maxPrecipitation) {
            maxPrecipitation = totalPrecipitation;
            maxMonthYear = key.toString();

            System.err.println("*** NEW MAX FOUND: " + maxMonthYear + " with " + maxPrecipitation + " mm ***");
        }

        keysProcessed++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("\n=== REDUCER CLEANUP ===");
        System.err.println("Total keys processed: " + keysProcessed);
        System.err.println("Maximum precipitation: " + maxPrecipitation + " mm");
        System.err.println("Month-Year: " + maxMonthYear);

        if (!maxMonthYear.isEmpty()) {
            String[] parts = maxMonthYear.split("-");
            if (parts.length == 2) {
                String year = parts[0];
                String month = parts[1];

                // Format output as required
                String output = String.format("%s month in %s had the highest total precipitation of %.2f mm",
                        getMonthName(month), year, maxPrecipitation);

                context.write(new Text("Result"), new Text(output));

                System.err.println("\n=== FINAL ANSWER ===");
                System.err.println(output);
            }
        }
    }

    private String getMonthName(String monthNum) {
        // Return ordinal format (1st, 2nd, 3rd, etc.)
        int month = Integer.parseInt(monthNum);

        if (month >= 11 && month <= 13) {
            return month + "th";
        }

        switch (month % 10) {
            case 1: return month + "st";
            case 2: return month + "nd";
            case 3: return month + "rd";
            default: return month + "th";
        }
    }
}