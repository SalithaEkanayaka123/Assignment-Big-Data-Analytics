package DatePreprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver Class: DatePreprocessingDriver
 *
 * PURPOSE: Job setup and submitission process happen in this class.
 *
 * - Reads raw weather CSV data with inconsistent date formats
 * - Normalizes ALL dates to a single standard format: dd-MM-yyyy
 */
public class DatePreprocessingDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DatePreprocessingDriver <input path> <output path>");
            System.exit(-1);
        }

        // job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Date Normalization");
        job.setJarByClass(DatePreprocessingDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(DatePreprocessingMapper.class);
        job.setReducerClass(DatePreprocessingReducer.class);

        // preserves original line order
        // Value: CSV line with normalized date
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Value: Text (cleaned CSV line)
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Use single reducer to maintain original file order since Single reducer guarantees output preserves input sequence, Multiple reducers would scatter lines across files (lose order)
        job.setNumReduceTasks(1);

        // Set input and output paths
        //hadoop jar clean_weather.jar DatePreprocessing.DatePreprocessingDriver /user/hduser/input1/weather.csv /user/hduser/input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}