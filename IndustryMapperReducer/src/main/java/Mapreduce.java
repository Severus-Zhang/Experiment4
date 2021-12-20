import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mapreduce
{
    private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line[] = value.toString().split(",");
            // 提取出 Industry 列
            context.write(new Text(line[10]), one);
        }
    }

    private static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException
        {
            String line[] = value.toString().split("\t");
            context.write(new IntWritable(Integer.parseInt(line[1])),new Text(line[0]));
        }
    }

    // 对输出结果中的词频进行降序排列
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator
    {
        public int compare(WritableComparable a, WritableComparable b)
        {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Path tempDir=new Path("industry-count-temp-");
        Job job = new Job(conf, "word count");
        job.setJarByClass(Mapreduce.class);
        try {
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            // 先将词频统计的任务的输出结果写到临时目录中
            FileOutputFormat.setOutputPath(job, tempDir);
            // 下一个排序的任务以临时目录为输入目录
//            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if (job.waitForCompletion(true)) {
                Job sortJob = new Job(conf, "sort");
                sortJob.setJarByClass(Mapreduce.class);
                sortJob.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.addInputPath(sortJob, tempDir);

                sortJob.setMapperClass(SortMapper.class);
                //将 Reducer 的个数限定为1个，最终输出的结果文件就是一个
                sortJob.setNumReduceTasks(1);
                FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排序。 因此我们实现了一个
                 * IntWritableDecreasingComparator 类，并指定使用这个自定义的 Comparator 类，对输出
                 * 结果中的 key（词频）进行排序
                 */
                sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

            }

        } finally {
            FileSystem.get(conf).deleteOnExit(tempDir);
        }
    }
}