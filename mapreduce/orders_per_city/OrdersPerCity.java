import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce Job 1: Orders Per City/Area
 * 
 * A word-count style job that counts the number of orders per city
 * and per city-area combination.
 * 
 * Input:  Clean CSV from /fooddelivery/clean
 * Output: City/Area -> Order Count
 * 
 * Usage:
 *   hadoop jar OrdersPerCity.jar OrdersPerCity /fooddelivery/clean /fooddelivery/output/orders_per_city
 */
public class OrdersPerCity {

    /**
     * Mapper: Emits (city, 1) and (city-area, 1) for each order
     * 
     * Input CSV columns (after Pig cleaning):
     * 0:order_id, 1:customer_id, 2:restaurant_id, 3:cuisine, 4:city, 5:area,
     * 6:order_time, 7:delivery_time, 8:delivery_duration_min, 9:distance_km,
     * 10:order_value, 11:discount, 12:net_revenue, 13:payment_method,
     * 14:delivery_partner_id, 15:partner_rating, 16:weather, 17:traffic_level,
     * 18:status, 19:customer_rating, 20:order_hour, 21:order_day_of_week,
     * 22:is_weekend, 23:is_delayed, 24:delivery_speed_kmph,
     * 25:order_value_bucket, 26:distance_bucket, 27:time_of_day
     */
    public static class OrderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text cityKey = new Text();
        private Text cityAreaKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            // Skip header if present
            if (line.startsWith("order_id")) return;
            
            String[] fields = line.split(",");
            
            // Validate we have enough fields
            if (fields.length < 6) return;
            
            String city = fields[4].trim();
            String area = fields[5].trim();
            
            // Skip empty values
            if (city.isEmpty() || area.isEmpty()) return;
            
            // Emit city count
            cityKey.set("CITY:" + city);
            context.write(cityKey, one);
            
            // Emit city-area count
            cityAreaKey.set("AREA:" + city + "-" + area);
            context.write(cityAreaKey, one);
        }
    }

    /**
     * Reducer: Sums up all counts for each key
     */
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Combiner: Same as reducer for count operations (associative + commutative)
     * This optimizes the shuffle phase by pre-aggregating on the mapper side.
     */
    // The SumReducer can be reused as Combiner since addition is associative

    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: OrdersPerCity <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Orders Per City/Area");
        
        job.setJarByClass(OrdersPerCity.class);
        job.setMapperClass(OrderMapper.class);
        job.setCombinerClass(SumReducer.class);  // Combiner for optimization
        job.setReducerClass(SumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
