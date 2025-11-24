package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatePreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip header
        if (line.startsWith("location_id") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 2) return;

        try {
            String originalDate = fields[1].trim(); // date is the 2nd column
            String standardizedDate = standardizeDate(originalDate);
            fields[1] = standardizedDate;

            outputKey.set(String.valueOf(key.get()));
            outputValue.set(String.join(",", fields));
            context.write(outputKey, outputValue);
        } catch (Exception e) {
            System.err.println("Error processing line: " + line);
        }
    }

    private String standardizeDate(String dateStr) throws Exception {
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
        outputFormat.setLenient(false);

        String[] formats = {
                "d/M/yyyy", "M/d/yyyy", "dd/MM/yyyy", "MM/dd/yyyy", "yyyy-MM-dd", "yyyy/MM/dd"
        };

        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                sdf.setLenient(false);
                Date date = sdf.parse(dateStr);
                if (isValidDate(date)) return outputFormat.format(date);
            } catch (Exception ignored) {}
        }

        return intelligentDateParse(dateStr);
    }

    private boolean isValidDate(Date date) {
        int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(date));
        return year >= 2010 && year <= 2024;
    }

    private String intelligentDateParse(String dateStr) throws Exception {
        String[] parts = dateStr.split("[/\\-]");
        if (parts.length != 3) throw new Exception("Invalid date format");

        int part1 = Integer.parseInt(parts[0]);
        int part2 = Integer.parseInt(parts[1]);
        int part3 = Integer.parseInt(parts[2]);

        int year, month, day;
        if (part1 > 31 || parts[0].length() == 4) {
            year = part1;
            if (part2 > 12) { day = part2; month = part3; }
            else { month = part2; day = part3; }
        } else if (part3 > 31 || parts[2].length() == 4) {
            year = part3;
            if (part1 > 12) { day = part1; month = part2; }
            else { month = part1; day = part2; }
        } else {
            throw new Exception("Unrecognized date: " + dateStr);
        }

        return String.format("%04d-%02d-%02d", year, month, day);
    }
}