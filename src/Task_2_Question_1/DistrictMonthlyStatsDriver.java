package Task_2_Question_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

/**
 * Driver Class: DistrictMonthlyStatsDriver
 *
 * PURPOSE:
 * This is the main entry point for the MapReduce job that calculates monthly weather statistics
 * (total precipitation and mean temperature) for each district.
 *
 * INPUTS:
 * 1. Weather data CSV file (cleaned_weather2.csv) - contains weather observations
 * 2. Location data CSV file (locationData.csv) - maps location IDs to district names
 * 3. Output directory path - where results will be stored
 *
 * OUTPUT:
 * Human-readable monthly statistics per district in format:
 * "{District} had a total precipitation of {X} mm with a mean temperature of {Y}°C for month {M} in year {YYYY}"
 *
 * POINTS:
 * - This class configures the entire MapReduce job
 * - Uses Distributed Cache to share location data across all mappers efficiently
 * - Driver is responsible for setting up job parameters before execution
 */
public class DistrictMonthlyStatsDriver {

    public static void main(String[] args) throws Exception {

        // Argument validation ensures the job receives all required inputs
        // args[0] = weather data path, args[1] = location data path, args[2] = output path
        if (args.length != 3) {
            System.err.println("Usage: Task_2_Question_1.DistrictMonthlyStatsDriver <weather data path> <location file path> <output path>");
            System.exit(-1);
        }

        // Configuration object holds cluster-specific settings and custom parameters
        Configuration conf = new Configuration();

        // Job instance represents the entire MapReduce job with a descriptive name
        Job job = Job.getInstance(conf, "District Monthly Statistics Job");

        // Set the JAR file class - tells Hadoop which JAR contains the job classes
        job.setJarByClass(DistrictMonthlyStatsDriver.class);

        // Specify which Mapper and Reducer classes to use for processing
        job.setMapperClass(DistrictMonthlyStatsMapper.class);
        job.setReducerClass(DistrictMonthlyStatsReducer.class);

        // Define the data types for Mapper output (Key-Value pairs)
        // Mapper emits: Key = "District-Year-Month" (Text), Value = "temp,precip" (Text)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Define the data types for final Reducer output
        // Reducer emits: Key = "" (empty Text), Value = formatted statistics (Text)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Distributed Cache - Critical concept for optimization
        // Location file is cached on all nodes so every mapper can access it locally
        // This avoids reading the location file multiple times from HDFS
        // Benefits: Reduces network I/O, faster lookup, efficient memory usage
        job.addCacheFile(new URI(args[1]));

        // Set input path for the main weather data (split across mappers)
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set output directory (must not exist before job runs)
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Submit job and wait for completion
        // Returns 0 for success, 1 for failure
        // waitForCompletion(true) = verbose output showing progress
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
