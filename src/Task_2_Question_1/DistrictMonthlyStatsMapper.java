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
    private int totalLinesReceived = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER SETUP STARTED ===");
        try {
            java.net.URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                String path = cacheFiles[0].getPath();
                String fileName = new java.io.File(path).getName();

                System.err.println("Cache file path: " + path);
                System.err.println("Cache file name: " + fileName);

                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;

                // Skip header
                String header = reader.readLine();
                System.err.println("Location file header: " + header);

                int locationCount = 0;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");

                    if (locationCount < 3) {
                        System.err.println("Location line " + locationCount + ": " + line);
                        System.err.println("  Fields count: " + fields.length);
                    }

                    // location_id is at index 0, city_name is at index 7
                    if (fields.length >= 8) {
                        String locationId = fields[0].trim();
                        String cityName = fields[7].trim();
                        locationMap.put(locationId, cityName);
                        locationCount++;
                        if (locationCount <= 5) {
                            System.err.println("  Loaded: " + locationId + " -> " + cityName);
                        }
                    }
                }
                reader.close();
                System.err.println("Total locations loaded: " + locationCount);
                System.err.println("Location map sample keys: " + locationMap.keySet().stream().limit(5).toArray());
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

        totalLinesReceived++;
        String line = value.toString().trim();

        if (totalLinesReceived <= 5) {
            System.err.println("\n=== RAW LINE " + totalLinesReceived + " ===");
            System.err.println("Line content: [" + line.substring(0, Math.min(200, line.length())) + "]");
        }

        if (line.isEmpty()) {
            System.err.println("EMPTY LINE at position " + totalLinesReceived);
            return;
        }

        // Skip header line (first line)
        if (!headerSkipped) {
            System.err.println("=== FOUND HEADER ===");
            System.err.println("Header content: " + line.substring(0, Math.min(200, line.length())));
            headerSkipped = true;
            return;
        }

        String[] fields = line.split(",");

        if (totalLinesReceived <= 5) {
            System.err.println("Processing line " + totalLinesReceived);
            System.err.println("Total fields: " + fields.length);
            for (int i = 0; i < Math.min(12, fields.length); i++) {
                System.err.println("  Field[" + i + "]: [" + fields[i] + "]");
            }
        }

        // Need at least 12 fields (0-11)
        if (fields.length < 12) {
            skippedLines++;
            if (skippedLines <= 10) {
                System.err.println("SKIPPED - Not enough fields. Got " + fields.length + ", need 12");
                System.err.println("  Line preview: " + line.substring(0, Math.min(100, line.length())));
            }
            return;
        }

        try {
            // Field indices based on your CSV structure
            String locationId = fields[0].trim();
            String date = fields[1].trim();
            String tempStr = fields[5].trim();
            String precipStr = fields[11].trim();

            if (totalLinesReceived <= 5) {
                System.err.println("Extracted values:");
                System.err.println("  locationId: [" + locationId + "]");
                System.err.println("  date: [" + date + "]");
                System.err.println("  tempStr: [" + tempStr + "]");
                System.err.println("  precipStr: [" + precipStr + "]");
            }

            // Parse date - try multiple formats
            String[] dateParts;
            String day, month, year;

            if (date.contains("-")) {
                dateParts = date.split("-");
                if (totalLinesReceived <= 5) {
                    System.err.println("  Date format: dash-separated");
                }
            } else if (date.contains("/")) {
                dateParts = date.split("/");
                if (totalLinesReceived <= 5) {
                    System.err.println("  Date format: slash-separated");
                }
            } else {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Date has no separator: [" + date + "]");
                }
                return;
            }

            if (dateParts.length != 3) {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Invalid date format: [" + date + "], parts: " + dateParts.length);
                }
                return;
            }

            // Assume dd-MM-yyyy or dd/MM/yyyy
            day = dateParts[0];
            month = dateParts[1];
            year = dateParts[2];

            if (totalLinesReceived <= 5) {
                System.err.println("  Parsed date: day=" + day + ", month=" + month + ", year=" + year);
            }

            // Get district name
            String district = locationMap.getOrDefault(locationId, "Unknown");

            if (totalLinesReceived <= 5) {
                System.err.println("  District lookup: " + locationId + " -> " + district);
            }

            if (district.equals("Unknown")) {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Unknown location: [" + locationId + "]");
                    System.err.println("  Available keys sample: " + locationMap.keySet().stream().limit(3).toArray());
                }
                return;
            }

            // Parse numbers
            double temperature = parseDouble(tempStr);
            double precipitation = parseDouble(precipStr);

            if (totalLinesReceived <= 5) {
                System.err.println("  Parsed numbers: temp=" + temperature + ", precip=" + precipitation);
            }

            if (Double.isNaN(temperature) || Double.isNaN(precipitation)) {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Invalid numbers. Temp: [" + tempStr + "], Precip: [" + precipStr + "]");
                }
                return;
            }

            // Create key: District-Year-Month
            String keyStr = district + "-" + year + "-" + month;
            outputKey.set(keyStr);

            // Create value: temperature,precipitation
            String valueStr = temperature + "," + precipitation;
            outputValue.set(valueStr);

            if (processedLines < 10) {
                System.err.println("✓ EMITTING - Key: [" + keyStr + "], Value: [" + valueStr + "]");
            }

            context.write(outputKey, outputValue);
            processedLines++;

        } catch (Exception e) {
            skippedLines++;
            if (skippedLines <= 10) {
                System.err.println("ERROR processing line " + totalLinesReceived + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("\n=== MAPPER CLEANUP ===");
        System.err.println("Total lines received: " + totalLinesReceived);
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