package Task_2_Question_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver Class: HighestPrecipitationDriver
 *
 * PURPOSE: Job setup and submitission process happen in this class.
 *
 * Finds the month-year combination with the HIGHEST TOTAL precipitation across all weather data.
 * This is a "Global Maximum" finding job that requires a single reducer.
 *
 *  OUTPUT:
 *  Single line showing which month-year had the highest total precipitation:
 *  "7th month in 2023 had the highest total precipitation of 1234.56 mm"
 */
public class HighestPrecipitationDriver {

    public static void main(String[] args) throws Exception {

        // Validate command line arguments
        //hadoop jar highest_precipitation.jar Task_2_Question_2.HighestPrecipitationDriver /user/hduser/input/cleaned_weather.csv /output/highest_total_precipitation
        if (args.length != 2) {
            System.err.println("Usage: Task_2_Question_2.HighestPrecipitationDriver <weather data path> <output path>");
            System.exit(-1);
        }

        // Create job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Highest Precipitation Month-Year Job");

        // Set job classes
        job.setJarByClass(HighestPrecipitationDriver.class);
        job.setMapperClass(HighestPrecipitationMapper.class);
        job.setReducerClass(HighestPrecipitationReducer.class);

        // Mapper output types
        // Mapper emits: Key = Year-Month (Text), Value = precipitation (DoubleWritable)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Final output types
        // Reducer emits: Key = "Result" (Text), Value = formatted message (Text)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Use single reducer to maintain original file order since Single reducer guarantees
        // output preserves input sequence, Multiple reducers would scatter lines across files (lose order)
        job.setNumReduceTasks(1);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
