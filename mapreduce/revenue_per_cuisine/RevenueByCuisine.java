import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce Job 2: Total Revenue Per Cuisine
 * 
 * Sum aggregation job that computes total and average revenue per cuisine type.
 * Uses net_revenue (order_value - discount) for accurate calculation.
 * 
 * Input:  Clean CSV from /fooddelivery/clean
 * Output: Cuisine -> Total Revenue, Avg Revenue, Order Count
 * 
 * Usage:
 *   hadoop jar RevenueByCuisine.jar RevenueByCuisine /fooddelivery/clean /fooddelivery/output/revenue_per_cuisine
 */
public class RevenueByCuisine {

    /**
     * Mapper: Emits (cuisine, net_revenue) for each delivered order
     */
    public static class RevenueMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        private Text cuisineKey = new Text();
        private DoubleWritable revenueValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            if (line.startsWith("order_id")) return;
            
            String[] fields = line.split(",");
            if (fields.length < 19) return;
            
            String cuisine = fields[3].trim();
            String status = fields[18].trim();
            
            // Only count delivered orders for revenue
            if (!status.equalsIgnoreCase("Delivered")) return;
            if (cuisine.isEmpty()) return;
            
            try {
                // Use net_revenue (column 12) = order_value - discount
                double netRevenue = Double.parseDouble(fields[12].trim());
                
                cuisineKey.set(cuisine);
                revenueValue.set(netRevenue);
                context.write(cuisineKey, revenueValue);
                
            } catch (NumberFormatException e) {
                // Skip malformed records
                context.getCounter("CUSTOM", "MALFORMED_REVENUE").increment(1);
            }
        }
    }

    /*
     * NOTE: No combiner is used here because the reducer counts orders
     * by iterating over individual values. A combiner would pre-sum
     * revenues, making each combined value appear as 1 order instead
     * of many — corrupting order_count and avg_revenue.
     *
     * For a correct combiner pattern with averages, see
     * AvgDeliveryPerHour.java which uses a custom SumCountWritable.
     */

    /**
     * Reducer: Computes total revenue and count per cuisine
     * Output format: cuisine \t total_revenue \t order_count \t avg_revenue
     */
    public static class RevenueReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double totalRevenue = 0;
            int count = 0;
            
            for (DoubleWritable val : values) {
                totalRevenue += val.get();
                count++;
            }
            
            double avgRevenue = (count > 0) ? totalRevenue / count : 0;
            
            result.set(String.format("%.2f\t%d\t%.2f", totalRevenue, count, avgRevenue));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: RevenueByCuisine <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Revenue Per Cuisine");
        
        job.setJarByClass(RevenueByCuisine.class);
        job.setMapperClass(RevenueMapper.class);
        // No combiner — see comment above RevenueCombiner for rationale
        job.setReducerClass(RevenueReducer.class);
        
        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        // Reducer output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
