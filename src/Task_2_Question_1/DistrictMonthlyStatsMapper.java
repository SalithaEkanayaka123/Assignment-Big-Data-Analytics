package Task_2_Question_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper Class: DistrictMonthlyStatsMapper
 *
 * PURPOSE:
 * Processes each weather record and emits key-value pairs grouping data by District-Year-Month.
 * This mapper performs a MAP-SIDE JOIN by loading location data into memory.
 *
 * INPUT KEY: LongWritable (byte offset of line in file)
 * INPUT VALUE: Text (CSV line from weather data)
 *
 * OUTPUT KEY: Text (format: "District-Year-Month", e.g., "Mumbai-2023-07")
 * OUTPUT VALUE: Text (format: "temperature,precipitation", e.g., "28.5,45.2")
 *
 * POINTS:
 * - Uses Distributed Cache for efficient lookup of location data
 * - Implements data enrichment (adds district name to weather records)
 * - Demonstrates MapReduce data preprocessing and validation
 * - Uses setup() method to initialize resources once per mapper task
 */
public class DistrictMonthlyStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Reusable objects to avoid object creation overhead in map() method
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    // HashMap for O(1) lookup of district by location ID (in-memory join)
    private Map<String, String> locationMap = new HashMap<>();

    // Statistics tracking for debugging and monitoring
    private int processedLines = 0;
    private int skippedLines = 0;
    private boolean headerSkipped = false;
    private int totalLinesReceived = 0;

    /**
     * SETUP METHOD - Called once per mapper task before processing any records
     *
     * POINTS:
     * - This runs ONCE per mapper, not once per record (efficient initialization)
     * - Loads location data from Distributed Cache into memory
     * - Creates a HashMap for fast lookups: locationId -> districtName
     * - This is an example of MAP-SIDE JOIN optimization technique
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.err.println("=== MAPPER SETUP STARTED ===");
        try {
            // getCacheFiles() retrieves files added via addCacheFile() in Driver
            // Files are automatically copied to each node's local filesystem
            java.net.URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                String path = cacheFiles[0].getPath();
                String fileName = new java.io.File(path).getName();

                System.err.println("Cache file path: " + path);
                System.err.println("Cache file name: " + fileName);

                // Read from local file system (not HDFS) - this is fast!
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;

                // Skip CSV header line
                String header = reader.readLine();
                System.err.println("Location file header: " + header);

                int locationCount = 0;
                // Build the lookup map - locationId -> cityName
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");

                    if (locationCount < 3) {
                        System.err.println("Location line " + locationCount + ": " + line);
                        System.err.println("  Fields count: " + fields.length);
                    }

                    // CSV structure - location_id is at index 0, city_name is at index 7
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

    /**
     * MAP METHOD - Called once for each input record (each line of weather data)
     *
     * POINTS:
     * - This is the core mapper logic executed for every input record
     * - Performs data validation, parsing, and transformation
     * - Enriches weather data with district information (JOIN operation)
     * - Emits intermediate key-value pairs for the reducer to aggregate
     * - Implements error handling to skip malformed records
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        totalLinesReceived++;
        String line = value.toString().trim();

        // Detailed logging for first few records (useful for debugging)
        if (totalLinesReceived <= 5) {
            System.err.println("\n=== RAW LINE " + totalLinesReceived + " ===");
            System.err.println("Line content: [" + line.substring(0, Math.min(200, line.length())) + "]");
        }

        // Data validation - skip empty lines
        if (line.isEmpty()) {
            System.err.println("EMPTY LINE at position " + totalLinesReceived);
            return;
        }

        // Skip CSV header line (only first line encountered by this mapper)
        if (!headerSkipped) {
            System.err.println("=== FOUND HEADER ===");
            System.err.println("Header content: " + line.substring(0, Math.min(200, line.length())));
            headerSkipped = true;
            return;
        }

        //Parse CSV line into fields
        String[] fields = line.split(",");

        if (totalLinesReceived <= 5) {
            System.err.println("Processing line " + totalLinesReceived);
            System.err.println("Total fields: " + fields.length);
            for (int i = 0; i < Math.min(12, fields.length); i++) {
                System.err.println("  Field[" + i + "]: [" + fields[i] + "]");
            }
        }

        // Data quality check - ensure we have all required fields
        // Need at least 12 fields (0-11) based on CSV structure
        if (fields.length < 12) {
            skippedLines++;
            if (skippedLines <= 10) {
                System.err.println("SKIPPED - Not enough fields. Got " + fields.length + ", need 12");
                System.err.println("  Line preview: " + line.substring(0, Math.min(100, line.length())));
            }
            return;
        }

        try {
            // Extract relevant fields from weather CSV
            // Field indices based on CSV structure:
            String locationId = fields[0].trim();      // Location identifier
            String date = fields[1].trim();            // Date of observation
            String tempStr = fields[5].trim();         // Temperature reading
            String precipStr = fields[11].trim();      // Precipitation amount

            if (totalLinesReceived <= 5) {
                System.err.println("Extracted values:");
                System.err.println("  locationId: [" + locationId + "]");
                System.err.println("  date: [" + date + "]");
                System.err.println("  tempStr: [" + tempStr + "]");
                System.err.println("  precipStr: [" + precipStr + "]");
            }

            // Date parsing - handle multiple date formats (dd-MM-yyyy or dd/MM/yyyy)
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

            // Validate date format has 3 parts (day, month, year)
            if (dateParts.length != 3) {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Invalid date format: [" + date + "], parts: " + dateParts.length);
                }
                return;
            }

            // Extract date components (assuming dd-MM-yyyy or dd/MM/yyyy format)
            day = dateParts[0];
            month = dateParts[1];
            year = dateParts[2];

            if (totalLinesReceived <= 5) {
                System.err.println("  Parsed date: day=" + day + ", month=" + month + ", year=" + year);
            }

            // LOOKUP OPERATION - This is the JOIN step!
            // Use locationId to find district name from our in-memory map
            // getOrDefault returns "Unknown" if locationId not found
            String district = locationMap.getOrDefault(locationId, "Unknown");

            if (totalLinesReceived <= 5) {
                System.err.println("  District lookup: " + locationId + " -> " + district);
            }

            // Skip records with unknown locations (data quality)
            if (district.equals("Unknown")) {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Unknown location: [" + locationId + "]");
                    System.err.println("  Available keys sample: " + locationMap.keySet().stream().limit(3).toArray());
                }
                return;
            }

            // Parse numeric values with error handling
            double temperature = parseDouble(tempStr);
            double precipitation = parseDouble(precipStr);

            if (totalLinesReceived <= 5) {
                System.err.println("  Parsed numbers: temp=" + temperature + ", precip=" + precipitation);
            }

            // Validate numeric values are valid (not NaN)
            if (Double.isNaN(temperature) || Double.isNaN(precipitation)) {
                skippedLines++;
                if (skippedLines <= 10) {
                    System.err.println("SKIPPED - Invalid numbers. Temp: [" + tempStr + "], Precip: [" + precipStr + "]");
                }
                return;
            }

            // BUILD COMPOSITE KEY - This determines how data will be grouped
            // Format: "District-Year-Month" (e.g., "Mumbai-2023-07")
            // All records with same district, year, and month will go to same reducer
            String keyStr = district + "-" + year + "-" + month;
            outputKey.set(keyStr);

            // BUILD VALUE - Contains the data to be aggregated
            // Format: "temperature,precipitation" (e.g., "28.5,45.2")
            // Reducer will sum/average these values
            String valueStr = temperature + "," + precipitation;
            outputValue.set(valueStr);

            if (processedLines < 10) {
                System.err.println("✓ EMITTING - Key: [" + keyStr + "], Value: [" + valueStr + "]");
            }

            //EMIT KEY-VALUE PAIR - This is the mapper output
            // context.write() sends data to the shuffle and sort phase
            // Hadoop will automatically group all values with same key together
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

    /**
     * CLEANUP METHOD - Called once per mapper task after all records are processed
     *
     * POINTS:
     * - Used for cleanup and logging final statistics
     * - Runs once per mapper, useful for closing resources or reporting metrics
     * - Helps monitor data quality and processing efficiency
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("\n=== MAPPER CLEANUP ===");
        System.err.println("Total lines received: " + totalLinesReceived);
        System.err.println("Total lines processed: " + processedLines);
        System.err.println("Total lines skipped: " + skippedLines);
        System.err.println("Location map size: " + locationMap.size());
    }

    /**
     * HELPER METHOD - Safely parse string to double
     *
     * POINTS:
     * - Handles null, empty, and invalid numeric values gracefully
     * - Returns NaN (Not a Number) for invalid inputs instead of throwing exception
     * - Example of defensive programming in data processing
     */
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