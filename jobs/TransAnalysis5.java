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

public class TransAnalysis5 {

    /**
     * Mapper:
     *  - Loads cust.txt from Distributed Cache in setup().
     *  - For each record in trans.txt, emits (gameType, "playerName,amount").
     */
    public static class TransMapper extends Mapper<Object, Text, Text, Text> {

        // Maps custID -> "FirstName LastName"
        private Map<String, String> custMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get the cached files (cust.txt) specified by job.addCacheFile(...)
            URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // We assume the file is localized as 'cust.txt'
                String localCustFile = "./cust.txt";  // after the # in job.addCacheFile()

                try (BufferedReader br = new BufferedReader(new FileReader(localCustFile))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // e.g. 4000001,Kristina,Chung,55,Pilot
                        String[] parts = line.trim().split(",");
                        if (parts.length >= 3) {
                            String custID    = parts[0];
                            String name = parts[1];
                            custMap.put(custID, name);
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

            // trans.txt example: transID, userID, ???, amount, gameType
            String[] parts = record.split(",");
            if (parts.length < 5) {
                return; // skip malformed lines
            }

            String userID    = parts[2];
            String amountStr = parts[3];
            String gameType  = parts[4];

            // Lookup player's full name from custMap, default to "Unknown" if not found
            String playerName = custMap.getOrDefault(userID, "Unknown");

            // Emit: (gameType, "playerName,amount")
            context.write(new Text(gameType), new Text(playerName + "," + amountStr));
        }
    }

    /**
     * Reducer:
     *  - For each gameType, gathers distinct player names and sums amounts.
     *  - Output: (gameType, "name1,name2,...\t totalAmount")
     */
    public static class TransReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double total = 0.0;
            Set<String> distinctNames = new HashSet<>();

            for (Text t : values) {
                // "playerName,amount"
                String[] parts = t.toString().split(",");
                if (parts.length < 2) {
                    continue;
                }
                String name     = parts[0];
                String amountStr = parts[1];

                distinctNames.add(name);
                total += Float.parseFloat(parts[1]); 
                
            }

            // Combine distinct player names into a comma-separated list
            String joinedNames = String.join(",", distinctNames);

            // Final output: gameType, "name1,name2,...\t totalAmount"
            context.write(key, new Text("[" + joinedNames + "]" + "\t" + total));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TransAnalysis5 <trans_input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trans analysis 5");
        job.setJarByClass(TransAnalysis5.class);

        // Set Mapper & Reducer
        job.setMapperClass(TransMapper.class);
        job.setReducerClass(TransReducer.class);

        // Mapper output key/value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output key/value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add cust.txt to distributed cache.
        // The '#cust.txt' part means the file will be localized under the name 'cust.txt' in each mapper's working dir.
        job.addCacheFile(new URI("/user/hien2706/input/cust.txt#cust.txt"));

        // Set input/output paths for trans.txt data
        FileInputFormat.addInputPath(job, new Path(args[0]));   // e.g. /user/hien2706/input/trans.txt
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // e.g. /user/hien2706/output5

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
