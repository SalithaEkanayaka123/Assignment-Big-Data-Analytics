package Task_2_Question_1_Part_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MaxPrecipitationReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecip = 0.0;

        for (Text value : values) {
            try {
                double precip = Double.parseDouble(value.toString());
                totalPrecip += precip;
            } catch (NumberFormatException e) {
                continue;
            }
        }

        outputValue.set(String.valueOf(totalPrecip));
        context.write(key, outputValue);
    }
}