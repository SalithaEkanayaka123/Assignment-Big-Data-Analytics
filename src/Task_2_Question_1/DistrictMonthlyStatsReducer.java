package Task_2_Question_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DistrictMonthlyStatsReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private int keysProcessed = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (keysProcessed < 5) {
            System.err.println("=== REDUCER processing key: " + key.toString() + " ===");
        }

        double tempSum = 0.0;
        double precipSum = 0.0;
        int count = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(",");

            if (keysProcessed < 3 && count < 3) {
                System.err.println("  Value " + count + ": " + value.toString());
            }

            if (parts.length == 2) {
                try {
                    double temp = Double.parseDouble(parts[0]);
                    double precip = Double.parseDouble(parts[1]);

                    tempSum += temp;
                    precipSum += precip;
                    count++;
                } catch (NumberFormatException e) {
                    System.err.println("  ERROR parsing: " + value.toString());
                    continue;
                }
            }
        }

        if (count > 0) {
            double meanTemp = tempSum / count;

            String[] keyParts = key.toString().split("-");
            if (keyParts.length >= 3) {
                String district = keyParts[0];
                String year = keyParts[1];
                String month = keyParts[2];

                // Create human-readable output as required
                String output = String.format("%s had a total precipitation of %.2f mm with a mean temperature of %.2f°C for month %s in year %s",
                        district, precipSum, meanTemp, month, year);

                // Use the original key (District-Year-Month) as output key
                outputValue.set(output);

                if (keysProcessed < 5) {
                    System.err.println("  Writing output: " + output);
                }

                context.write(new Text(""), outputValue);  // Empty key for cleaner output
                keysProcessed++;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("=== REDUCER CLEANUP ===");
        System.err.println("Total keys processed: " + keysProcessed);
    }
}