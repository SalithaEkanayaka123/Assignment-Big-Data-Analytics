
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DistrictMonthlyStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private Map<String, String> locationMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load location data from distributed cache
        // Assuming location.csv is added to distributed cache
        try {
            java.net.URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                String line;
                java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.FileReader(cacheFiles[0].getPath()));

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

        // Expected format: date,city_id,temperature_2m_mean,precipitation_sum,...
        // Adjust indices based on your actual CSV structure
        if (fields.length < 4) {
            return;
        }

        try {
            String date = fields[0].trim();
            String cityId = fields[1].trim();
            String tempStr = fields[2].trim();
            String precipStr = fields[3].trim();

            // Parse date to extract year and month
            String[] dateParts = date.split("-");
            if (dateParts.length != 3) {
                return;
            }

            String year = dateParts[0];
            String month = dateParts[1];

            // Get district name from location map
            String district = locationMap.getOrDefault(cityId, "Unknown");

            // Skip if we couldn't find the district
            if (district.equals("Unknown")) {
                return;
            }

            // Parse numerical values
            double temperature = parseDouble(tempStr);
            double precipitation = parseDouble(precipStr);

            // Skip if values are invalid
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