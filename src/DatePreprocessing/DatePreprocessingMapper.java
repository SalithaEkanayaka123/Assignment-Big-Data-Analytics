package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatePreprocessingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private final LongWritable outKey = new LongWritable();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        if (key.get() == 0 && line.toLowerCase().startsWith("location_id")) {
            outKey.set(Long.MIN_VALUE); // ensure header sorts before any real offset
            outValue.set(line);
            context.write(outKey, outValue);
            return; // don't attempt to parse header as data
        }

        // Skip other header-like lines that accidentally appear in later splits
        if (line.toLowerCase().startsWith("location_id")) {
            // ignore (since header has been emitted by offset 0)
            return;
        }

        String[] parts = line.split(",", -1);
        if (parts.length < 2) {
            // invalid row — skip
            return;
        }

        try {
            String originalDate = parts[1].trim();
            String standardizedDate = parseDate(originalDate);
            parts[1] = standardizedDate;
            outKey.set(key.get()); // file offset: preserves original line order when sorted
            outValue.set(String.join(",", parts));
            context.write(outKey, outValue);
        } catch (Exception e) {
            // Could not parse date — skip or optionally emit original line
            // For now skip unparseable lines to match earlier behavior
        }
    }

    private String parseDate(String input) throws Exception {
        SimpleDateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");
        String[] formats = {
                "dd-MM-yyyy", "d-M-yyyy", "d/M/yyyy", "M/d/yyyy", 
                "dd/MM/yyyy", "MM/dd/yyyy", "yyyy-MM-dd", "yyyy/MM/dd"
        };
        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                sdf.setLenient(false);
                Date date = sdf.parse(input);
                return outputFormat.format(date);
            } catch (Exception ignored) {
                // try next format
            }
        }
        throw new Exception("Unparseable date: " + input);
    }
}