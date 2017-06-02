/*
 * 		Big Data Class
 *		20113337 Choi Young Keun 
 *		Hadoop Map Reduce Project #1
 *
 */

package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;

import kr.ac.kookmin.cs.bigdata.ReviewScore.Map;
import kr.ac.kookmin.cs.bigdata.ReviewScore.Reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;

public class ReviewScore extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new ReviewScore(), args);
      
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(ReviewScore.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
      
        return 0;
    }
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
        private Text word = new Text();
       
		@Override   // 코드 들여쓰기
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
			try{
				JSONObject jsn = new JSONObject(value.toString());   // 변수명 jsn?
				String asinValue = (String)jsn.get("asin");
				String overall = jsn.get("overall").toString();
				FloatWritable overallValue = new FloatWritable(Float.parseFloat(overall));
				word.set(asinValue);
				context.write(word, overallValue);
				
			}catch(JSONException e){
				e.printStackTrace();
			}
		}
    }

	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            context.write(key, new FloatWritable(sum/count));
        }
	}
}
