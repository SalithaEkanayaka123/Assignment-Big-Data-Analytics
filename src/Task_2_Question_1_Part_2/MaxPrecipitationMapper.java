package Task_2_Question_1_Part_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.startsWith("date") || line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 4) {
            return;
        }

        try {
            String date = fields[0].trim();
            String precipStr = fields[3].trim();

            String[] dateParts = date.split("/");
            if (dateParts.length != 3) {
                return;
            }

            String month = dateParts[0];
            String year = dateParts[2];

            double precipitation = parseDouble(precipStr);
            if (Double.isNaN(precipitation)) {
                return;
            }

            // Key: Year-Month
            String keyStr = year + "-" + month;
            outputKey.set(keyStr);
            outputValue.set(String.valueOf(precipitation));

            context.write(outputKey, outputValue);

        } catch (Exception e) {
            System.err.println("Error processing line: " + line);
        }
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