import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransAnalysis6 {

    /**
     * Mapper:
     *  - Loads cust.txt from Distributed Cache in setup().
     *  - For each record in trans.txt, emits (gameType, "playerName,age \t amount").
     */
    public static class TransMapper extends Mapper<Object, Text, Text, Text> {

        // Maps custID -> "firstName,age"
        private Map<String, String> custMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Retrieve the cached file specified by job.addCacheFile(...)
            URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // We assume it's localized as 'cust.txt'
                String localCustFile = "./cust.txt";  // after the '#' in job.addCacheFile()

                try (BufferedReader br = new BufferedReader(new FileReader(localCustFile))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Example line in cust.txt: 4000001,Kristina,Chung,55,Pilot
                        String[] parts = line.trim().split(",");
                        // We need at least 4 columns: custID, firstName, lastName, age
                        if (parts.length >= 4) {
                            String custID    = parts[0];
                            String firstName = parts[1];
                            String age       = parts[3];
                            // Combine firstName and age => e.g. "Kristina,55"
                            String nameWithAge = firstName + "," + age;
                            custMap.put(custID, nameWithAge);
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String record = value.toString().trim();
            if (record.isEmpty()) {
                return;
            }

            // trans.txt example: transID, ???, userID, amount, gameType
            String[] parts = record.split(",");
            if (parts.length < 5) {
                // Skip malformed lines
                return;
            }

            // Adjust these indices to match your actual trans.txt
            // Suppose userID is at index 2, amount at index 3, gameType at index 4
            String userID    = parts[2];
            String amountStr = parts[3];
            String gameType  = parts[4];

            // Lookup player's "firstName,age" in custMap, or "Unknown" if missing
            String nameWithAge = custMap.getOrDefault(userID, "Unknown,0");

            // Emit: (gameType, "name,age \t amount")
            // Using '\t' as a delimiter between "name,age" and "amount"
            context.write(new Text(gameType), new Text(nameWithAge + "\t" + amountStr));
        }
    }

    /**
     * Reducer:
     *  - For each gameType, gather distinct (name,age) pairs and sum amounts.
     *  - Output: gameType    [name,age, name2,age2, ...]    totalAmount
     */
    public static class TransReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double total = 0.0;
            // Use a Set to ensure distinct "name,age" entries
            Set<String> distinctPlayers = new HashSet<>();

            for (Text t : values) {
                // t is "name,age \t amount"
                String[] parts = t.toString().split("\t");
                if (parts.length < 2) {
                    continue;  // skip malformed
                }

                String nameWithAge = parts[0];   // e.g. "Karen,74"

                distinctPlayers.add(nameWithAge);
                total += Float.parseFloat(parts[1]); 
                
            }

            // Join distinct "name,age" pairs with commas
            String joinedPlayers = String.join(", ", distinctPlayers);
            // Wrap them in brackets
            String bracketedPlayers = "[" + joinedPlayers + "]";

            // Output format: (gameType, "[Karen,74, Dolores,60]" + "\t" + total)
            context.write(key, new Text(bracketedPlayers + "\t" + total));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TransAnalysis6 <trans_input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trans analysis 6");
        job.setJarByClass(TransAnalysis6.class);

        // Mapper & Reducer
        job.setMapperClass(TransMapper.class);
        job.setReducerClass(TransReducer.class);

        // Mapper output key/value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output key/value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add cust.txt to the distributed cache
        // The '#cust.txt' means it will be localized as 'cust.txt' in each mapper's working directory
        job.addCacheFile(new URI("/user/hien2706/input/cust.txt#cust.txt"));

        // Input / Output paths for trans.txt
        FileInputFormat.addInputPath(job, new Path(args[0]));   // e.g. /user/hien2706/input/trans.txt
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // e.g. /user/hien2706/output6

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
