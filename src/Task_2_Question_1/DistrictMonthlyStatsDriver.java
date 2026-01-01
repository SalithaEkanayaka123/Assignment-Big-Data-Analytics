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
 * PURPOSE: Job setup and submission process happen in this class.
 *
 * - Calculates monthly weather statistics (total precipitation and mean temperature) for each district
 * - Joins weather data with location data using Distributed Cache
 * - Configures mapper and reducer classes for processing
 * - Sets up input/output paths and data types
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
        Job job = Job.getInstance(conf, "District Monthly Statistics Job");

        // tells Hadoop which JAR contains the job classes
        job.setJarByClass(DistrictMonthlyStatsDriver.class);

        // Specify which Mapper and Reducer classes to use for processing
        job.setMapperClass(DistrictMonthlyStatsMapper.class);
        job.setReducerClass(DistrictMonthlyStatsReducer.class);

        // Mapper output key and value denotes (data type)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer output key and value denotes (data type)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // define locationData path
        job.addCacheFile(new URI(args[1]));

        // Set input path for the main weather data (split across mappers)
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set output directory (must not exist before job runs)
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Submit job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
