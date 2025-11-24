import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DistrictMonthlyStatsReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double tempSum = 0.0;
        double precipSum = 0.0;
        int count = 0;

        // Aggregate temperature and precipitation values
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 2) {
                try {
                    double temp = Double.parseDouble(parts[0]);
                    double precip = Double.parseDouble(parts[1]);

                    tempSum += temp;
                    precipSum += precip;
                    count++;
                } catch (NumberFormatException e) {
                    // Skip invalid values
                    continue;
                }
            }
        }

        if (count > 0) {
            // Calculate mean temperature
            double meanTemp = tempSum / count;

            // Parse key to get district, year, month
            String[] keyParts = key.toString().split("-");
            if (keyParts.length >= 3) {
                String district = keyParts[0];
                String year = keyParts[1];
                String month = keyParts[2];

                // Format output
                String output = String.format(
                        "%s had a total precipitation of %.2f hours with a mean temperature of %.2f for %s month in %s",
                        district, precipSum, meanTemp, month, year
                );

                outputValue.set(output);
                context.write(key, outputValue);
            }
        }
    }
}