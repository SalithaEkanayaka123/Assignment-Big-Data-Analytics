package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DatePreprocessingReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

    private final NullWritable outKey = NullWritable.get();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(outKey, value);
        }
    }
}