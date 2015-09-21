
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        
    	Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setJarByClass(OrphanPages.class);
        
        return job.waitForCompletion(true) ? 0 : 1; 
        
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
        	String line = value.toString();
        	
        	StringTokenizer tokenizer = new StringTokenizer(line, ": "); // Delims are : and SPACE!!
        	
        	Boolean first = true; //First time round setfirst value to true
        	//This loop handles the first token which is the source page. 
        	while (tokenizer.hasMoreTokens()) {
        		String next = tokenizer.nextToken().trim();
        		
        		Integer page = Integer.parseInt(next);
        		
        		if (first == true) {
        			
        			context.write(new IntWritable(page), new IntWritable(0)); // Write the source page and set it to count 0 as it has no link yet..
        			first = false; // We have been through at least once so set false. 
        			
        			continue; //go back to ensure we are not somehow on the first again. 
        		}
        		
        		context.write(new IntWritable(page), new IntWritable(1)); //This isn't the first token so it is a link so it gets a 1. 
        		
        	}
        	
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; 
            
            for (IntWritable val : values) {
            	sum += val.get();
            }
            
            if (sum == 0) {
            	context.write(key, NullWritable.get());
            }
        }
    }
}