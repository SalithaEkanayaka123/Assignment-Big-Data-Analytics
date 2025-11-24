package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatePreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (line.startsWith("location_id") || line.isEmpty()) return;

        String[] parts = line.split(",", -1);
        if (parts.length < 2) return;

        try {
            String originalDate = parts[1].trim();
            String standardizedDate = parseDate(originalDate);
            parts[1] = standardizedDate;

            outKey.set(String.valueOf(key.get()));
            outValue.set(String.join(",", parts));
            context.write(outKey, outValue);
        } catch (Exception e) {
            // Skip lines with unparseable dates
        }
    }

    private String parseDate(String input) throws Exception {
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
        String[] formats = {
                "d/M/yyyy", "M/d/yyyy", "dd/MM/yyyy", "MM/dd/yyyy", "yyyy-MM-dd", "yyyy/MM/dd"
        };

        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                sdf.setLenient(false);
                Date date = sdf.parse(input);
                return outputFormat.format(date);
            } catch (Exception e) {
                // Try next
            }
        }

        throw new Exception("Unparseable date: " + input);
    }
}