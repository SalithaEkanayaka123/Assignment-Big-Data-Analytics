package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

public class DatePreprocessingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final Logger LOGGER = Logger.getLogger(DatePreprocessingMapper.class.getName());
    private final LongWritable outKey = new LongWritable();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        if (key.get() == 0 && line.toLowerCase().startsWith("location_id")) {
            outKey.set(Long.MIN_VALUE);
            outValue.set(line);
            context.write(outKey, outValue);
            LOGGER.info("Header line detected and written: " + line);
            return;
        }

        if (line.toLowerCase().startsWith("location_id")) {
            LOGGER.info("Skipping duplicate header at offset " + key.get());
            return;
        }

        String[] parts = line.split(",", -1);

        if (key.get() < 500) {
            LOGGER.info("Line structure at offset " + key.get() + ": Total columns=" + parts.length +
                    ", Column[0]=" + (parts.length > 0 ? parts[0] : "N/A") +
                    ", Column[1]=" + (parts.length > 1 ? parts[1] : "N/A") +
                    ", Column[2]=" + (parts.length > 2 ? parts[2] : "N/A"));
        }

        if (parts.length < 2) {
            LOGGER.warning("Skipping line with insufficient columns at offset " + key.get() + ": " + line);
            return;
        }

        try {
            String originalDate = parts[1].trim();
            if (!originalDate.isEmpty() && !originalDate.equalsIgnoreCase("date")) {
                String standardizedDate = parseDate(originalDate);
                parts[1] = standardizedDate;
                LOGGER.info("Date converted: '" + originalDate + "' -> '" + standardizedDate + "' at offset " + key.get());
            }
            outKey.set(key.get());
            outValue.set(String.join(",", parts));
            context.write(outKey, outValue);
        } catch (Exception e) {
            LOGGER.severe("ERROR: Could not parse date '" + parts[1] + "' at line offset " + key.get() + " - " + e.getMessage());
            LOGGER.severe("  Full line: " + line);
            outKey.set(key.get());
            outValue.set(line);
            context.write(outKey, outValue);
        }
    }

    private String parseDate(String input) throws Exception {
        SimpleDateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");
        String[] formats = {
                "dd-MM-yyyy",
                "d-M-yyyy",
                "yyyy-MM-dd",
                "dd/MM/yyyy",
                "d/M/yyyy",
                "M/d/yyyy",
                "dd-M-yyyy",
                "d-MM-yyyy",
                "MM/dd/yyyy",
                "yyyy/MM/dd",
                "d/MM/yyyy",
                "dd/M/yyyy"
        };

        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                sdf.setLenient(false);
                Date date = sdf.parse(input);
                return outputFormat.format(date);
            } catch (Exception ignored) { }
        }
        throw new Exception("Unparseable date: " + input);
    }
}