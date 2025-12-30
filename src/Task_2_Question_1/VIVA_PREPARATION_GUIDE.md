# Task 2 Question 1
## District Monthly Statistics MapReduce Job

---

### 1. **What does this MapReduce job do?**
**Answer:** This job calculates monthly weather statistics (total precipitation and mean temperature) for each district by joining weather observation data with location data, then aggregating the results by district, year, and month.

---

### 2. **Explain the role of each class**

**Driver Class (DistrictMonthlyStatsDriver):**
- Entry point for the MapReduce job
- Configures job parameters (mapper, reducer, input/output types)
- Adds location file to Distributed Cache
- Submits the job to Hadoop cluster

**Mapper Class (DistrictMonthlyStatsMapper):**
- Processes each weather record line by line
- Performs map-side join using location data from Distributed Cache
- Emits key-value pairs: Key = "District-Year-Month", Value = "temp,precip"

**Reducer Class (DistrictMonthlyStatsReducer):**
- Receives grouped data for each District-Year-Month
- Aggregates: sums precipitation, averages temperature
- Produces human-readable formatted output

---

### 3. **What is Distributed Cache and why is it used here?**

**Answer:** 
- Distributed Cache is a Hadoop feature that copies small files to all worker nodes
- **Purpose in this job:** Share the location data (locationData.csv) with all mappers
- **Benefits:**
  - Avoids reading location file from HDFS repeatedly
  - Enables efficient map-side join
  - Faster lookup (local file system vs HDFS)
  - Reduced network I/O

**Code location:** 
```java
job.addCacheFile(new URI(args[1]));  // Driver adds file to cache
context.getCacheFiles();              // Mapper retrieves cached file
```

---

### 4. **Explain the map() method workflow**

**Answer:**
1. **Validate input:** Skip empty lines and header
2. **Parse CSV:** Split line into fields
3. **Extract fields:** locationId, date, temperature, precipitation
4. **Parse date:** Extract day, month, year
5. **JOIN operation:** Lookup district name using locationId
6. **Validate data:** Check for valid numeric values
7. **Build composite key:** "District-Year-Month"
8. **Emit key-value pair:** (District-Year-Month, temp,precip)

---

### 5. **Explain the reduce() method workflow**

**Answer:**
1. **Receive grouped data:** All values for one District-Year-Month key
2. **Initialize accumulators:** tempSum, precipSum, count
3. **Iterate through values:** Parse each "temp,precip" pair
4. **Accumulate:** Sum temperatures, sum precipitation, count records
5. **Calculate mean:** meanTemp = tempSum / count
6. **Format output:** Create human-readable string
7. **Emit result:** Write formatted statistics

---

### 6. **What is the input and output of Mapper?**

**Mapper Input:**
- Key: LongWritable (byte offset in file)
- Value: Text (one line of CSV)

**Mapper Output:**
- Key: Text ("District-Year-Month", e.g., "Mumbai-2023-07")
- Value: Text ("temperature,precipitation", e.g., "28.5,45.2")

---

### 7. **What is the input and output of Reducer?**

**Reducer Input:**
- Key: Text ("District-Year-Month")
- Values: Iterable<Text> (multiple "temp,precip" strings)

**Reducer Output:**
- Key: Text (empty string "")
- Value: Text (formatted message like "Mumbai had a total precipitation of 234.50 mm...")

---

### 8. **Why use setup() and cleanup() methods?**

**setup():**
- Called ONCE per mapper task (not per record)
- Used to initialize resources (load location data)
- More efficient than loading data for each record

**cleanup():**
- Called ONCE per task after all records processed
- Used for resource cleanup and final logging
- Reports statistics (lines processed, skipped)

---

### 9. **What is a Map-Side Join?**

**Answer:** 
A join operation performed in the mapper by loading one dataset (location data) into memory and using it to enrich the main dataset (weather data).

**In this job:**
- Location data loaded into HashMap during setup()
- Each weather record joined with location data using locationId lookup
- More efficient than reduce-side join for small reference data

---

### 10. **How does Hadoop group data between Map and Reduce phases?**

**Answer:**
1. **Mapper emits:** Multiple key-value pairs
2. **Shuffle & Sort phase:** Hadoop automatically:
   - Sorts all mapper outputs by key
   - Groups all values with same key together
   - Distributes keys across reducers
3. **Reducer receives:** One key with all its values in an Iterable

**Example:**
```
Mapper1 emits: (Mumbai-2023-07, "28.5,45.2")
Mapper2 emits: (Mumbai-2023-07, "29.0,50.0")
Mapper3 emits: (Delhi-2023-07, "35.0,20.0")

After Shuffle & Sort:
Reducer1 receives: Mumbai-2023-07 → ["28.5,45.2", "29.0,50.0"]
Reducer2 receives: Delhi-2023-07 → ["35.0,20.0"]
```

---

### 11. **What is the composite key pattern?**

**Answer:** 
Combining multiple attributes into a single key to control grouping.

**In this job:**
- Key format: "District-Year-Month"
- Example: "Mumbai-2023-07"
- **Purpose:** Groups all weather records from same district and month together
- Enables aggregation at the desired granularity level

---

### 12. **Why use Text instead of String in Hadoop?**

**Answer:**
- Text is Hadoop's serializable String wrapper
- Implements Writable interface (Hadoop serialization)
- More efficient for network transfer and disk I/O
- Required for mapper/reducer input/output types

---

### 13. **How is data quality handled?**

**Answer:**
1. **Validation checks:**
   - Skip empty lines
   - Skip header lines
   - Verify minimum number of fields
   - Check date format validity
   
2. **Error handling:**
   - parseDouble() returns NaN for invalid numbers
   - Skip records with unknown locations
   - Skip records with invalid data
   - Log errors for debugging

3. **Statistics tracking:**
   - Count processed lines
   - Count skipped lines
   - Report in cleanup phase

---

### 14. **What are the command line arguments?**

```bash
hadoop jar district_monthly_stats.jar \
  Task_2_Question_1.DistrictMonthlyStatsDriver \
  /user/hduser/input/cleaned_weather2.csv \      # Arg 0: Weather data
  /user/hduser/input/locationData.csv \          # Arg 1: Location data
  /output/district_monthly_stats                  # Arg 2: Output directory
```

---

### 15. **What optimizations are used in this job?**

**Answer:**
1. **Distributed Cache:** Avoid repeated HDFS reads
2. **Object reuse:** Reuse outputKey and outputValue objects (avoid GC overhead)
3. **Map-side join:** More efficient than reduce-side join for small datasets
4. **Early validation:** Skip invalid records early to save processing
5. **Composite key:** Single shuffle instead of multiple grouping operations

---

### 16. **How would you explain the complete data flow?**

**Answer:**
```
1. INPUT PHASE:
   - Weather CSV split across multiple mappers
   - Location CSV cached on all nodes

2. MAP PHASE (per mapper):
   - setup(): Load location HashMap
   - map(): Process each weather record
     → Extract data
     → Join with location data
     → Emit (District-Year-Month, temp,precip)
   - cleanup(): Log statistics

3. SHUFFLE & SORT PHASE (Hadoop automatic):
   - Sort all mapper outputs by key
   - Group values by key
   - Distribute to reducers

4. REDUCE PHASE (per reducer):
   - reduce(): For each unique District-Year-Month
     → Sum all temperatures and precipitations
     → Calculate mean temperature
     → Format output message
     → Emit result
   - cleanup(): Log statistics

5. OUTPUT PHASE:
   - Write results to HDFS output directory
   - part-r-00000 contains formatted statistics
```

---

### 17. **What would happen if location data was NOT in Distributed Cache?**

**Answer:**
- Each mapper would need to read location file from HDFS
- Multiple network reads (inefficient)
- Higher latency and resource consumption
- Possible performance bottleneck
- Map-side join would be impractical

**Alternative:** Would need to use reduce-side join (less efficient)

---

### 18. **Expected output format example?**

**Answer:**
```
Mumbai had a total precipitation of 234.50 mm with a mean temperature of 28.75°C for month 07 in year 2023
Delhi had a total precipitation of 125.30 mm with a mean temperature of 35.20°C for month 06 in year 2023
Bangalore had a total precipitation of 180.00 mm with a mean temperature of 24.50°C for month 08 in year 2023
```

---

### 19. **What data structures are used and why?**

**Answer:**
1. **HashMap<String, String>:** Location lookup (O(1) access time)
2. **Text objects:** Hadoop serialization format
3. **Iterable<Text>:** Efficient iteration over grouped values (memory-friendly)
4. **Primitive doubles:** Numeric calculations (temperature, precipitation)

---

### 20. **How would you test this job?**

**Answer:**
1. **Unit testing:**
   - Test parseDouble() with various inputs
   - Test date parsing logic
   - Test output formatting

2. **Integration testing:**
   - Run with small sample datasets
   - Verify location lookup works
   - Check output format

3. **Validation:**
   - Check log messages for skipped lines
   - Verify all districts appear in output
   - Manually verify calculations for sample month
   - Ensure no duplicate entries

4. **Performance testing:**
   - Monitor mapper/reducer task counts
   - Check distributed cache loading
   - Verify no data skew (balanced processing)

---

## IMPORTANT CONCEPTS TO REMEMBER

1. **MapReduce paradigm:** Divide, Process, Combine
2. **Distributed Cache:** Share small files across cluster
3. **Map-side join:** Efficient for small reference data
4. **Composite keys:** Control data grouping
5. **setup/cleanup:** Resource initialization and cleanup
6. **Shuffle & Sort:** Automatic grouping by Hadoop
7. **Data quality:** Validation and error handling
8. **Aggregation:** Sum and average calculations

---

