package Task_2_Question_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DistrictMonthlyStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private Map<String, String> locationMap = new HashMap<>();
    private int processedLines = 0;
    private int skippedLines = 0;
    private boolean headerSkipped = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER SETUP STARTED ===");
        try {
            java.net.URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                String path = cacheFiles[0].getPath();
                String fileName = new java.io.File(path).getName();

                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;

                // Skip header
                String header = reader.readLine();
                System.err.println("Location file header: " + header);

                int locationCount = 0;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    // location_id is at index 0, city_name is at index 7
                    if (fields.length >= 8) {
                        String locationId = fields[0].trim();
                        String cityName = fields[7].trim();
                        locationMap.put(locationId, cityName);
                        locationCount++;
                        if (locationCount <= 5) {
                            System.err.println("Loaded: " + locationId + " -> " + cityName);
                        }
                    }
                }
                reader.close();
                System.err.println("Total locations loaded: " + locationCount);
            }
        } catch (Exception e) {
            System.err.println("ERROR in setup: " + e.getMessage());
            e.printStackTrace();
        }
        System.err.println("=== MAPPER SETUP COMPLETED ===");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        // Skip header line (first line)
        if (!headerSkipped) {
            System.err.println("Skipping header: " + line.substring(0, Math.min(100, line.length())));
            headerSkipped = true;
            return;
        }

        String[] fields = line.split(",");

        if (processedLines < 3) {
            System.err.println("=== Line " + processedLines + " ===");
            System.err.println("Total fields: " + fields.length);
            if (fields.length > 0) System.err.println("Field[0] (location_id): " + fields[0]);
            if (fields.length > 1) System.err.println("Field[1] (date): " + fields[1]);
            if (fields.length > 5) System.err.println("Field[5] (temp_mean): " + fields[5]);
            if (fields.length > 11) System.err.println("Field[11] (precip_sum): " + fields[11]);
        }

        // Need at least 12 fields (0-11)
        if (fields.length < 12) {
            skippedLines++;
            if (skippedLines <= 5) {
                System.err.println("SKIPPED - Not enough fields: " + fields.length);
            }
            return;
        }

        try {
            // Field indices based on your CSV structure
            String locationId = fields[0].trim();      // location_id
            String date = fields[1].trim();            // date
            String tempStr = fields[5].trim();         // temperature_2m_mean (°C)
            String precipStr = fields[11].trim();      // precipitation_sum (mm)

            // Parse date (format: dd-MM-yyyy)
            String[] dateParts = date.split("-");
            if (dateParts.length != 3) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid date format: " + date);
                }
                return;
            }

            // Format is dd-MM-yyyy
            String day = dateParts[0];
            String month = dateParts[1];
            String year = dateParts[2];

            // Get district name
            String district = locationMap.getOrDefault(locationId, "Unknown");

            if (district.equals("Unknown")) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Unknown location: " + locationId);
                }
                return;
            }

            // Parse numbers
            double temperature = parseDouble(tempStr);
            double precipitation = parseDouble(precipStr);

            if (Double.isNaN(temperature) || Double.isNaN(precipitation)) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid numbers. Temp: " + tempStr + ", Precip: " + precipStr);
                }
                return;
            }

            // Create key: District-Year-Month
            String keyStr = district + "-" + year + "-" + month;
            outputKey.set(keyStr);

            // Create value: temperature,precipitation
            String valueStr = temperature + "," + precipitation;
            outputValue.set(valueStr);

            if (processedLines < 3) {
                System.err.println("EMITTING - Key: " + keyStr + ", Value: " + valueStr);
            }

            context.write(outputKey, outputValue);
            processedLines++;

        } catch (Exception e) {
            skippedLines++;
            if (skippedLines <= 10) {
                System.err.println("ERROR: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER CLEANUP ===");
        System.err.println("Total lines processed: " + processedLines);
        System.err.println("Total lines skipped: " + skippedLines);
        System.err.println("Location map size: " + locationMap.size());
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