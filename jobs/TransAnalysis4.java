import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransAnalysis4 {

    // Mapper: outputs (game_type, "ID,amount")
    public static class TransMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String record = value.toString().trim();
            // Skip empty lines
            if (record.isEmpty()) {
                return;
            }

            // Split the CSV line
            String[] parts = record.split(",");
            // Example CSV structure: transID, userID, something, amount, gameType
            // Adjust indices as needed to match your data
            if (parts.length < 5) {
                // Not enough columns, skip
                return;
            }

            // According to the screenshot, gameType is at index 4, ID at index 2, amount at index 3
            String gameType = parts[4];
            String id       = parts[2];
            String amount   = parts[3];

            // Emit key = gameType, value = "ID,amount"
            context.write(new Text(gameType), new Text(id + " " + amount));
        }
    }

    // Reducer: collects all IDs and sums amounts for each game_type
    public static class TransReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double total = 0.0;
            Set<String> distinctIDs = new HashSet<>();

            for (Text t : values) 
            { 
                String[] parts = t.toString().trim().split(" "); 

                total += Float.parseFloat(parts[1]); 
                // IDlist += parts[0]+ ","; 
                distinctIDs.add(parts[0]);
            }
           
            context.write(key, new Text( distinctIDs.size() + "   " + Double.toString(total)));
        
        }
    }

    // 3. Driver
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TransAnalysis4 <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trans analysis 4");

        job.setJarByClass(TransAnalysis4.class);
        job.setMapperClass(TransMapper.class);
        job.setReducerClass(TransReducer.class);

        // Output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
