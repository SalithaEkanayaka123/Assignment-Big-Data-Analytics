package Task_2_Question_1;

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

                // Output format: total_precip,mean_temp
                String output = String.format("%.2f,%.2f", precipSum, meanTemp);
                outputValue.set(output);
                context.write(key, outputValue);
            }
        }
    }
}