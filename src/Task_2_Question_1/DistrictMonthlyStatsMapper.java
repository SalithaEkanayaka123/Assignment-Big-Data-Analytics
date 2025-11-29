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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER SETUP STARTED ===");
        try {
            java.net.URI[] cacheFiles = context.getCacheFiles();
            System.err.println("Cache files count: " + (cacheFiles != null ? cacheFiles.length : 0));

            if (cacheFiles != null && cacheFiles.length > 0) {
                System.err.println("Cache file URI: " + cacheFiles[0].toString());

                // Try different approaches to read the file
                String path = cacheFiles[0].getPath();
                String fileName = new java.io.File(path).getName();
                System.err.println("File name: " + fileName);

                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;

                // Skip header
                String header = reader.readLine();
                System.err.println("Location file header: " + header);

                int locationCount = 0;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length >= 2) {
                        String cityId = fields[0].trim();
                        String cityName = fields[1].trim();
                        locationMap.put(cityId, cityName);
                        locationCount++;
                        if (locationCount <= 5) {
                            System.err.println("Loaded location: " + cityId + " -> " + cityName);
                        }
                    }
                }
                reader.close();
                System.err.println("Total locations loaded: " + locationCount);
            } else {
                System.err.println("ERROR: No cache files found!");
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

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Skip header
        if (line.startsWith("date") || line.toLowerCase().startsWith("date")) {
            System.err.println("Skipping header: " + line);
            return;
        }

        String[] fields = line.split(",");

        // Debug: Print first few lines
        if (processedLines < 5) {
            System.err.println("Processing line " + processedLines + ": " + line);
            System.err.println("Fields count: " + fields.length);
            for (int i = 0; i < Math.min(fields.length, 5); i++) {
                System.err.println("  Field[" + i + "]: '" + fields[i] + "'");
            }
        }

        if (fields.length < 4) {
            skippedLines++;
            if (skippedLines <= 5) {
                System.err.println("SKIPPED - Not enough fields: " + line);
            }
            return;
        }

        try {
            String date = fields[0].trim();
            String cityId = fields[1].trim();
            String tempStr = fields[2].trim();
            String precipStr = fields[3].trim();

            if (processedLines < 5) {
                System.err.println("Parsed - Date: " + date + ", CityID: " + cityId +
                        ", Temp: " + tempStr + ", Precip: " + precipStr);
            }

            // Parse date (format: MM/dd/yyyy)
            String[] dateParts = date.split("/");
            if (dateParts.length != 3) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid date format: " + date);
                }
                return;
            }

            String month = dateParts[0];
            String year = dateParts[2];

            // Get district name from location map
            String district = locationMap.getOrDefault(cityId, "Unknown");

            if (processedLines < 5) {
                System.err.println("CityID: " + cityId + " -> District: " + district);
            }

            if (district.equals("Unknown")) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Unknown district for cityId: " + cityId);
                }
                return;
            }

            double temperature = parseDouble(tempStr);
            double precipitation = parseDouble(precipStr);

            if (Double.isNaN(temperature) || Double.isNaN(precipitation)) {
                skippedLines++;
                if (skippedLines <= 5) {
                    System.err.println("SKIPPED - Invalid numbers. Temp: " + tempStr + ", Precip: " + precipStr);
                }
                return;
            }

            // Key: District-Year-Month
            String keyStr = district + "-" + year + "-" + month;
            outputKey.set(keyStr);

            // Value: temperature,precipitation
            String valueStr = temperature + "," + precipitation;
            outputValue.set(valueStr);

            if (processedLines < 5) {
                System.err.println("EMITTING - Key: " + keyStr + ", Value: " + valueStr);
            }

            context.write(outputKey, outputValue);
            processedLines++;

        } catch (Exception e) {
            System.err.println("ERROR processing line: " + line);
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace();
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