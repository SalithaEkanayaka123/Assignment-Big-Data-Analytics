package Task_2_Question_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.File;

public class DistrictMonthlyStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private Map<String, String> locationMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            java.net.URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Get the actual file path
                File locationFile = new File(cacheFiles[0].getPath());
                String fileName = locationFile.getName();

                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;

                // Skip header
                reader.readLine();

                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length >= 2) {
                        String cityId = fields[0].trim();
                        String cityName = fields[1].trim();
                        locationMap.put(cityId, cityName);
                    }
                }
                reader.close();
            }
        } catch (Exception e) {
            System.err.println("Error loading location data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip header and empty lines
        if (line.startsWith("date") || line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length < 4) {
            return;
        }

        try {
            String date = fields[0].trim();
            String cityId = fields[1].trim();
            String tempStr = fields[2].trim();
            String precipStr = fields[3].trim();

            // Parse date (format: MM/dd/yyyy)
            String[] dateParts = date.split("/");
            if (dateParts.length != 3) {
                return;
            }

            String month = dateParts[0];  // Month is first
            String year = dateParts[2];   // Year is third

            // Get district name from location map
            String district = locationMap.getOrDefault(cityId, "Unknown");

            if (district.equals("Unknown")) {
                return;
            }

            double temperature = parseDouble(tempStr);
            double precipitation = parseDouble(precipStr);

            if (Double.isNaN(temperature) || Double.isNaN(precipitation)) {
                return;
            }

            // Key: District-Year-Month
            String keyStr = district + "-" + year + "-" + month;
            outputKey.set(keyStr);

            // Value: temperature,precipitation
            String valueStr = temperature + "," + precipitation;
            outputValue.set(valueStr);

            context.write(outputKey, outputValue);

        } catch (Exception e) {
            System.err.println("Error processing line: " + line);
        }
    }

    private double parseDouble(String str) {
        try {
            if (str == null || str.isEmpty() || str.equalsIgnoreCase("null")) {
                return Double.NaN;
            }
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }
}