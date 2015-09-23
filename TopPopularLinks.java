
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
    	
    	//Two jobs needed, one to create the popular pages, another to sort the output to N most pop
    	
    	Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job firstJob = Job.getInstance(conf, "Link Count");
        
        firstJob.setOutputKeyClass(IntWritable.class);
        firstJob.setOutputValueClass(IntWritable.class);

        firstJob.setMapperClass(LinkCountMap.class);
        firstJob.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, tmpPath);

        firstJob.setJarByClass(TopPopularLinks.class);
        firstJob.waitForCompletion(true);

        Job secondJob = Job.getInstance(conf, "Top Links");
        
        secondJob.setOutputKeyClass(IntWritable.class);
        secondJob.setOutputValueClass(IntWritable.class);

        secondJob.setMapOutputKeyClass(NullWritable.class);
        secondJob.setMapOutputValueClass(IntArrayWritable.class);

        secondJob.setMapperClass(TopLinksMap.class);
        secondJob.setReducerClass(TopLinksReduce.class);
        secondJob.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(secondJob, tmpPath);
        FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));

        secondJob.setInputFormatClass(KeyValueTextInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);

        secondJob.setJarByClass(TopPopularLinks.class);
        
        
        return secondJob.waitForCompletion(true) ? 0 : 1;
    
    }

	public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line, ": "); 

			Boolean first = true; // First time round setfirst value to true
			// This loop handles the first token which is the source page.
			while (tokenizer.hasMoreTokens()) {
				String next = tokenizer.nextToken().trim();

				Integer page = Integer.parseInt(next);

				if (first == true) {

					context.write(new IntWritable(page), new IntWritable(0));
					// Write the source page and set it to count 0 as it has no
					// link yet..
					first = false;
					// We have been through at least once so set false.

					continue;
					// go back to ensure we are not somehow on the first again.
				}

				context.write(new IntWritable(page), new IntWritable(1));
				// This isn't the first token so it is a link so it gets a 1.

			}
       
    }
}

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					
					sum += val.get();
				}
				
				context.write(key, new IntWritable(sum));
				//Unlike last time we write out ALL values. 
			}
		}

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;

        private TreeSet<Pair<Integer, Integer>> titleCountMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
				
        	Integer count = Integer.parseInt(value.toString());
				Integer page  = Integer.parseInt(key.toString());
				
				titleCountMap.add(new Pair<Integer, Integer>(count, page));
				
				if (titleCountMap.size() > this.N) {
					titleCountMap.remove(titleCountMap.first());
			   }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> item : titleCountMap) {
               

					Integer[] numbers = {item.second, item.first};
					
					IntArrayWritable val = new IntArrayWritable(numbers);
					
					context.write(NullWritable.get(), val);
				}
    }
}
    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        
        private TreeSet<Pair<Integer, Integer>> titleCountMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
				
        	for (IntArrayWritable val: values) {
					IntWritable []pair = (IntWritable []) val.toArray();
					Integer page = pair[0].get();
					Integer count = pair[1].get();
					
					titleCountMap.add(new Pair<Integer, Integer>(count, page));

					if (titleCountMap.size() > this.N) {
						titleCountMap.remove(titleCountMap.first());
					}
				}

				for (Pair<Integer, Integer> item: titleCountMap) {
					IntWritable page = new IntWritable(item.second);
					IntWritable count = new IntWritable(item.first);
					context.write(page, count); //Again use the null write done!!!
				}
        }
    }
}
	
// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change