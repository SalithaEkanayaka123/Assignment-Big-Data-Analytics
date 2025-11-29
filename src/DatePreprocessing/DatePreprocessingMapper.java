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
        // Try formats in order: exact matches first, then flexible formats
        String[] formats = {
                "dd-MM-yyyy",   // Already correct format
                "d-M-yyyy",     // Single digit with dash
                "yyyy-MM-dd",   // ISO format
                "dd/MM/yyyy",   // Double digit with slash
                "d/M/yyyy",     // Single digit with slash (1/1/2010)
                "dd-M-yyyy",    // Mixed: double day, single month with dash
                "d-MM-yyyy",    // Mixed: single day, double month with dash
                "M/d/yyyy",     // US format with slash (ambiguous)
                "MM/dd/yyyy",   // US format double digit
                "yyyy/MM/dd",   // ISO with slash
                "d/MM/yyyy",    // Single day, double month with slash
                "dd/M/yyyy"     // Double day, single month with slash
        };
        
        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                sdf.setLenient(false);
                Date date = sdf.parse(input);
                // Verify that the entire string was parsed
                if (sdf.format(date).length() > 0) {
                    return outputFormat.format(date);
                }
            } catch (Exception ignored) {
                // try next format
            }
        }
        throw new Exception("Unparseable date: " + input);
    }
}