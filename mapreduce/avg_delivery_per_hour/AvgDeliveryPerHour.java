import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce Job 3: Average Delivery Time Per Hour of Day
 * 
 * Demonstrates the use of a COMBINER with a custom Writable for computing
 * averages. The combiner emits partial sums and counts, which the reducer
 * aggregates to compute the final average.
 * 
 * This is a classic example of why combiners for averages need special handling
 * (you can't just average the averages).
 * 
 * Input:  Clean CSV from /fooddelivery/clean
 * Output: Hour -> Avg Delivery Time, Order Count, Min Time, Max Time
 * 
 * Usage:
 *   hadoop jar AvgDeliveryPerHour.jar AvgDeliveryPerHour /fooddelivery/clean /fooddelivery/output/avg_delivery_per_hour
 */
public class AvgDeliveryPerHour {

    /**
     * Custom Writable to hold sum and count for average computation.
     * This allows the combiner to correctly aggregate partial results.
     */
    public static class SumCountWritable implements Writable {
        
        private double sum;
        private int count;
        private double min;
        private double max;

        public SumCountWritable() {
            this.sum = 0;
            this.count = 0;
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
        }

        public SumCountWritable(double sum, int count, double min, double max) {
            this.sum = sum;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public void set(double sum, int count, double min, double max) {
            this.sum = sum;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public double getSum() { return sum; }
        public int getCount() { return count; }
        public double getMin() { return min; }
        public double getMax() { return max; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeInt(count);
            out.writeDouble(min);
            out.writeDouble(max);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readInt();
            min = in.readDouble();
            max = in.readDouble();
        }

        @Override
        public String toString() {
            return String.format("%.2f\t%d\t%.2f\t%.2f", sum, count, min, max);
        }
    }

    /**
     * Mapper: Emits (hour, SumCountWritable(duration, 1, duration, duration))
     * for each delivered order
     */
    public static class DeliveryTimeMapper 
            extends Mapper<LongWritable, Text, IntWritable, SumCountWritable> {
        
        private IntWritable hourKey = new IntWritable();
        private SumCountWritable valueOut = new SumCountWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            if (line.startsWith("order_id")) return;
            
            String[] fields = line.split(",");
            if (fields.length < 21) return;
            
            String status = fields[18].trim();
            
            // Only consider delivered orders
            if (!status.equalsIgnoreCase("Delivered")) return;
            
            try {
                float duration = Float.parseFloat(fields[8].trim());  // delivery_duration_min
                int hour = Integer.parseInt(fields[20].trim());       // order_hour
                
                // Validate
                if (duration <= 0 || hour < 0 || hour > 23) return;
                
                hourKey.set(hour);
                valueOut.set(duration, 1, duration, duration);
                context.write(hourKey, valueOut);
                
            } catch (NumberFormatException e) {
                context.getCounter("CUSTOM", "MALFORMED_RECORDS").increment(1);
            }
        }
    }

    /**
     * Combiner: Aggregates partial sums and counts on mapper side.
     * This is the KEY optimization - instead of sending individual records,
     * we send aggregated (sum, count, min, max) per hour per mapper.
     */
    public static class DeliveryTimeCombiner 
            extends Reducer<IntWritable, SumCountWritable, IntWritable, SumCountWritable> {
        
        private SumCountWritable partialResult = new SumCountWritable();

        @Override
        public void reduce(IntWritable key, Iterable<SumCountWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double sum = 0;
            int count = 0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            
            for (SumCountWritable val : values) {
                sum += val.getSum();
                count += val.getCount();
                min = Math.min(min, val.getMin());
                max = Math.max(max, val.getMax());
            }
            
            partialResult.set(sum, count, min, max);
            context.write(key, partialResult);
        }
    }

    /**
     * Reducer: Computes final average from aggregated sums and counts
     * Output: hour \t avg_time \t order_count \t min_time \t max_time
     */
    public static class DeliveryTimeReducer 
            extends Reducer<IntWritable, SumCountWritable, Text, Text> {
        
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<SumCountWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double totalSum = 0;
            int totalCount = 0;
            double globalMin = Double.MAX_VALUE;
            double globalMax = Double.MIN_VALUE;
            
            for (SumCountWritable val : values) {
                totalSum += val.getSum();
                totalCount += val.getCount();
                globalMin = Math.min(globalMin, val.getMin());
                globalMax = Math.max(globalMax, val.getMax());
            }
            
            double average = (totalCount > 0) ? totalSum / totalCount : 0;
            
            // Format hour as HH:00
            outputKey.set(String.format("%02d:00", key.get()));
            outputValue.set(String.format("%.2f\t%d\t%.2f\t%.2f", 
                average, totalCount, globalMin, globalMax));
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: AvgDeliveryPerHour <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Delivery Time Per Hour");
        
        job.setJarByClass(AvgDeliveryPerHour.class);
        job.setMapperClass(DeliveryTimeMapper.class);
        job.setCombinerClass(DeliveryTimeCombiner.class);
        job.setReducerClass(DeliveryTimeReducer.class);
        
        // Map output types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        
        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
