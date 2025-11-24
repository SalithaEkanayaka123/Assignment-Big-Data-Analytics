package DatePreprocessing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DatePreprocessingReducer extends Reducer<Text, Text, NullWritable, Text> {

    private NullWritable outputKey = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Simply output the corrected line
        for (Text value : values) {
            context.write(outputKey, value);
        }
    }
}