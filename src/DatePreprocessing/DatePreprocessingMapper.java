package DatePreprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatePreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip header
        if (line.startsWith("date") || line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length < 4) {
            return; // Skip malformed lines
        }

        try {
            String originalDate = fields[0].trim();
            String standardizedDate = standardizeDate(originalDate);

            // Replace the date field with standardized format
            fields[0] = standardizedDate;

            // Output: key=line_number, value=corrected_line
            outputKey.set(String.valueOf(key.get()));
            outputValue.set(String.join(",", fields));
            context.write(outputKey, outputValue);

        } catch (Exception e) {
            // Log problematic line but continue processing
            System.err.println("Error processing line: " + line);
        }
    }

    private String standardizeDate(String dateStr) throws Exception {
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;

        // Try different date formats
        String[] formats = {
                "d/M/yyyy",    // 1/5/2020
                "M/d/yyyy",    // 5/1/2020
                "dd/MM/yyyy",  // 01/05/2020
                "MM/dd/yyyy",  // 05/01/2020
                "yyyy-MM-dd",  // 2020-05-01
                "yyyy/MM/dd"   // 2020/05/01
        };

        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                sdf.setLenient(false);
                date = sdf.parse(dateStr);

                // Additional validation: check if parsed date makes sense
                if (isValidDate(date)) {
                    return outputFormat.format(date);
                }
            } catch (Exception e) {
                // Try next format
                continue;
            }
        }

        // If no format works, try intelligent parsing
        return intelligentDateParse(dateStr);
    }

    private boolean isValidDate(Date date) {
        // Check if date is within reasonable range (2010-2024)
        SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
        int year = Integer.parseInt(yearFormat.format(date));
        return year >= 2010 && year <= 2024;
    }

    private String intelligentDateParse(String dateStr) throws Exception {
        // Split by common delimiters
        String[] parts = dateStr.split("[/\\-]");

        if (parts.length != 3) {
            throw new Exception("Invalid date format");
        }

        int part1 = Integer.parseInt(parts[0]);
        int part2 = Integer.parseInt(parts[1]);
        int part3 = Integer.parseInt(parts[2]);

        int year, month, day;

        // Determine which part is year (typically 4 digits or > 31)
        if (part1 > 31 || parts[0].length() == 4) {
            year = part1;
            // Determine month and day
            if (part2 > 12) {
                day = part2;
                month = part3;
            } else if (part3 > 12) {
                month = part2;
                day = part3;
            } else {
                // Ambiguous case - assume MM/DD format
                month = part2;
                day = part3;
            }
        } else if (part3 > 31 || parts[2].length() == 4) {
            year = part3;
            // Determine month and day
            if (part1 > 12) {
                day = part1;
                month = part2;
            } else if (part2 > 12) {
                month = part1;
                day = part2;
            } else {
                // Ambiguous case - assume D/M/YYYY format
                day = part1;
                month = part2;
            }
        } else {
            throw new Exception("Cannot determine year");
        }

        // Adjust 2-digit years
        if (year < 100) {
            year += (year < 50) ? 2000 : 1900;
        }

        return String.format("%04d-%02d-%02d", year, month, day);
    }
}
