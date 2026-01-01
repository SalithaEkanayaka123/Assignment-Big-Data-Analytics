package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Class: DatePreprocessingReducer
 *
 * This Reducer receives cleaned CSV lines from the mapper and
 * writes them to the output file without the byte offset keys. From Hadoop's
 * shuffle & sort phase to automatically order lines by their original file position.
 * Using a single reducer ensures all data goes to one clean output file in the correct
 * order, with NullWritable keys producing clean CSV format without number prefixes
 */
public class DatePreprocessingReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

    // This represents "no key" or "empty key" in Hadoop
    private final NullWritable outKey = NullWritable.get();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // Example: "location_id,15-07-2023,temperature,..." (clean CSV)
            // NOT: "12345	location_id,15-07-2023,..." (would have offset)
            context.write(outKey, value);
        }
    }
}