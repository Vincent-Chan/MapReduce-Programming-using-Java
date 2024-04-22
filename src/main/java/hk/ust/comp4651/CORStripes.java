package hk.ust.comp4651;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Compute the bigram count using "pairs" approach
 */
public class CORStripes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CORStripes.class);

	/*
	 * TODO: write your first-pass Mapper here.
	 */
	private static class CORMapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> word_set = new HashMap<String, Integer>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String clean_doc = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizer = new StringTokenizer(clean_doc);
			/*
			 * TODO: Your implementation goes here.
			 */

			Set<String> real_word_set = new HashSet<String>() ;
			String word_token = new String() ;
			Text KEY = new Text() ;
			IntWritable ONE = new IntWritable(1) ;

			boolean add_success ;

			while (doc_tokenizer.hasMoreTokens() == true)
			{
				word_token = doc_tokenizer.nextToken() ;

				add_success = real_word_set.add(word_token) ;

				KEY.set(word_token) ;
				context.write(KEY, ONE) ;

				// if (real_word_set.add(word_token) == true)
				// {
				// 	KEY.set(word_token) ;
				// 	context.write(KEY, ONE) ;
				// }
				// // newly added for debugging
				// else
				// {
				// 	KEY.set(word_token) ;
				// 	context.write(KEY, ONE) ;
				// }
			}
			
		}
	}

	/*
	 * TODO: Write your first-pass reducer here.
	 */
	private static class CORReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */

			int s = 0 ;

			for (IntWritable v: values)
			{
				s += v.get() ;
			}

			context.write(key, new IntWritable(s)) ;
			
		}
	}

	/*
	 * TODO: Write your second-pass Mapper here.
	 */
	public static class CORStripesMapper2 extends Mapper<LongWritable, Text, Text, MapWritable> {
				
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> sorted_word_set = new TreeSet<String>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String doc_clean = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizers = new StringTokenizer(doc_clean);
			while (doc_tokenizers.hasMoreTokens()) {
				sorted_word_set.add(doc_tokenizers.nextToken());
			}
			/*
			 * TODO: Your implementation goes here.
			 */

			Text KEY = new Text() ;
			MapWritable VALUE = new MapWritable() ;
			IntWritable ONE = new IntWritable(1) ;

			while (doc_tokenizers.hasMoreTokens() == true)
			{
				sorted_word_set.add(doc_tokenizers.nextToken()) ;
			}

			String[] word_token_list = new String[sorted_word_set.size()] ;
			sorted_word_set.toArray(word_token_list) ;

			for (int i = 0; i < word_token_list.length; ++i)
			{
				KEY.set(word_token_list[i]) ;
				// VALUE = new MapWritable() ;

				for (int j = i+1; j < word_token_list.length; ++j)
				{
					VALUE.put(new Text(word_token_list[j]), ONE) ;
				}

				context.write(KEY, VALUE) ;
				VALUE.clear() ;
			}

			// Reference

			/*

			Text KEY = new Text() ;
			MapWritable VALUE = new MapWritable() ;
			IntWritable ONE = new IntWritable(1) ;

			while (doc_tokenizers.hasMoreTokens() == true)
			{
				sorted_word_set.add(doc_tokenizers.nextToken()) ;
			}

			String[] word_list = new String[sorted_word_set.size()] ;
			sorted_word_set.toArray(word_list) ;

			for (int i = 0; i < word_list.length; ++i)
			{
				KEY.set(word_list[i]) ;
				// VALUE = new MapWritable() ;

				for (int j = i+1; j < word_list.length; ++j)
				{
					VALUE.put(new Text(word_list[j]), ONE) ;
				}

				context.write(KEY, VALUE) ;
				VALUE.clear() ;
			}	

			*/
			
		}
	}

	/*
	 * TODO: Write your second-pass Combiner here.
	 */
	public static class CORStripesCombiner2 extends Reducer<Text, MapWritable, Text, MapWritable> {
		static IntWritable ZERO = new IntWritable(0);

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */

			MapWritable stripe_map_writable = new MapWritable() ;
			Text temp_word_token = new Text() ;
			IntWritable temp_count = new IntWritable() ;
			IntWritable total_count = new IntWritable() ;

			for (MapWritable v: values)
			{
				for (Map.Entry<Writable, Writable> entry: v.entrySet())
				{
					temp_word_token = (Text) entry.getKey() ;
					temp_count = (IntWritable) entry.getValue() ;
					
					// total_count = (IntWritable) stripe_map_writable.getOrDefault(temp_word_token, ZERO) ;
					
					if (stripe_map_writable.containsKey(temp_word_token) == true)
					{
						total_count = (IntWritable) stripe_map_writable.get(temp_word_token) ;
					}
					else
					{
						total_count = (IntWritable) ZERO ;
					}

					stripe_map_writable.put(temp_word_token, new IntWritable(total_count.get() + temp_count.get())) ;
				}
			}

			context.write(key, stripe_map_writable) ;

			// Reference

			/*

			MapWritable stripe_map = new MapWritable() ;
			Text temp_text = new Text() ;
			IntWritable temp_count = new IntWritable() ;
			IntWritable total_count = new IntWritable() ;

			for (MapWritable v: values)
			{
				for (Map.Entry<Writable, Writable> entry: v.entrySet())
				{
					temp_text = (Text) entry.getKey() ;
					temp_count = (IntWritable) entry.getValue() ;
					
					// total_count = (IntWritable) stripe_map.getOrDefault(temp_text, ZERO) ;
					
					if (stripe_map.containsKey(temp_text) == true)
					{
						total_count = (IntWritable) stripe_map.get(temp_text) ;
					}
					else
					{
						total_count = (IntWritable) ZERO ;
					}

					stripe_map.put(temp_text, new IntWritable(total_count.get() + temp_count.get())) ;
				}
			}

			context.write(key, stripe_map) ;

			*/
			
		}
	}

	/*
	 * TODO: Write your second-pass Reducer here.
	 */
	public static class CORStripesReducer2 extends Reducer<Text, MapWritable, PairOfStrings, DoubleWritable> {
		private static Map<String, Integer> word_total_map = new HashMap<String, Integer>();
		private static IntWritable ZERO = new IntWritable(0);

		/*
		 * Preload the middle result file.
		 * In the middle result file, each line contains a word and its frequency Freq(A), seperated by "\t"
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path middle_result_path = new Path("mid/part-r-00000");
			Configuration middle_conf = new Configuration();
			try {
				FileSystem fs = FileSystem.get(URI.create(middle_result_path.toString()), middle_conf);

				if (!fs.exists(middle_result_path)) {
					throw new IOException(middle_result_path.toString() + "not exist!");
				}

				FSDataInputStream in = fs.open(middle_result_path);
				InputStreamReader inStream = new InputStreamReader(in);
				BufferedReader reader = new BufferedReader(inStream);

				LOG.info("reading...");
				String line = reader.readLine();
				String[] line_terms;
				while (line != null) {
					line_terms = line.split("\t");
					word_total_map.put(line_terms[0], Integer.valueOf(line_terms[1]));
					LOG.info("read one line!");
					line = reader.readLine();
				}
				reader.close();
				LOG.info("finished！");
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

		/*
		 * TODO: Write your second-pass Reducer here.
		 */
		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */

			MapWritable stripe_map_writable = new MapWritable() ;
			Text right_word_token = new Text() ;
			IntWritable left_and_right_freq = new IntWritable() ;
			IntWritable total_count = new IntWritable() ;

			PairOfStrings KEY = new PairOfStrings() ;

			double cor_value ;

			for (MapWritable v: values)
			{
				for (Map.Entry<Writable, Writable> entry: v.entrySet())
				{
					right_word_token = (Text) entry.getKey() ;
					left_and_right_freq = (IntWritable) entry.getValue() ;
					
					// total_count = (IntWritable) stripe_map_writable.getOrDefault(right_word_token, ZERO) ;

					if (stripe_map_writable.containsKey(right_word_token) == true)
					{
						total_count = (IntWritable) stripe_map_writable.get(right_word_token) ;
					}
					else
					{
						total_count = (IntWritable) ZERO ;
					}

					stripe_map_writable.put(right_word_token, new IntWritable(total_count.get() + left_and_right_freq.get())) ;
				}
			}

			for (Map.Entry<Writable, Writable> entry: stripe_map_writable.entrySet())
			{
				right_word_token = (Text) entry.getKey() ;
				left_and_right_freq = (IntWritable) entry.getValue() ;

				cor_value = (double) left_and_right_freq.get() / (word_total_map.get(key.toString()) * word_total_map.get(right_word_token.toString())) ;

				KEY.set(key.toString(), right_word_token.toString()) ;

				context.write(KEY, new DoubleWritable(cor_value)) ;
			}

			// Reference

			/*

			MapWritable stripe_map = new MapWritable() ;
			Text temp_text = new Text() ;
			IntWritable temp_count = new IntWritable() ;
			IntWritable total_count = new IntWritable() ;

			PairOfStrings KEY = new PairOfStrings() ;

			double cor_value ;

			for (MapWritable v: values)
			{
				for (Map.Entry<Writable, Writable> entry: v.entrySet())
				{
					temp_text = (Text) entry.getKey() ;
					temp_count = (IntWritable) entry.getValue() ;
					
					// total_count = (IntWritable) stripe_map.getOrDefault(temp_text, ZERO) ;

					if (stripe_map.containsKey(temp_text) == true)
					{
						total_count = (IntWritable) stripe_map.get(temp_text) ;
					}
					else
					{
						total_count = (IntWritable) ZERO ;
					}

					stripe_map.put(temp_text, new IntWritable(total_count.get() + temp_count.get())) ;
				}
			}

			for (Map.Entry<Writable, Writable> entry: stripe_map.entrySet())
			{
				temp_text = (Text) entry.getKey() ;
				temp_count = (IntWritable) entry.getValue() ;

				cor_value = (double) temp_count.get() / (word_total_map.get(key.toString()) * word_total_map.get(temp_text.toString())) ;

				KEY.set(key.toString(), temp_text.toString()) ;

				context.write(KEY, new DoubleWritable(cor_value)) ;
			}

			*/
			
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public CORStripes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(NUM_REDUCERS));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		// Lack of arguments
		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String middlePath = "mid";
		String outputPath = cmdline.getOptionValue(OUTPUT);

		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + CORStripes.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - middle path: " + middlePath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Setup for the first-pass MapReduce
		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "Firstpass");

		job1.setJarByClass(CORStripes.class);
		job1.setMapperClass(CORMapper1.class);
		job1.setReducerClass(CORReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(middlePath));

		// Delete the output directory if it exists already.
		Path middleDir = new Path(middlePath);
		FileSystem.get(conf1).delete(middleDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		// Setup for the second-pass MapReduce

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf1).delete(outputDir, true);


		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Secondpass");

		job2.setJarByClass(CORStripes.class);
		job2.setMapperClass(CORStripesMapper2.class);
		job2.setCombinerClass(CORStripesCombiner2.class);
		job2.setReducerClass(CORStripesReducer2.class);

		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));

		// Time the program
		startTime = System.currentTimeMillis();
		job2.waitForCompletion(true);
		LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CORStripes(), args);
	}
}