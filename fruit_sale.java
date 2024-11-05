import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class fruit_sale {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "fruit_sale");
        job.setJarByClass(fuirt_sale.class);
        job.setMapperClass(fuirt_sale.TokenizerMapper.class);
        job.setReducerClass(fuirt_sale.IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.103.111:9000/user/root/sales_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.103.111:9000/user/root/result7"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable quantity = new IntWritable();
        private Text fruit = new Text();

        public TokenizerMapper() {
        }

        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            for (String token : tokens) {
                // 设置单词为Mapper的输出键
                fruit.set(token);
                // 设置数量为1（因为我们在这个阶段只关心单词是否出现，而不是它的总计数）
                quantity.set(1);
                // 写出键值对到上下文，这将由Hadoop框架进一步处理（比如排序和分区）
                context.write(fruit, quantity);
            }
        }
    }

        public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();

            public IntSumReducer() {
            }

            public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
                int sum = 0;
                IntWritable val;
                for (Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                    val = (IntWritable) i$.next();
                }
                this.result.set(sum);
                context.write(key, this.result);
            }
        }
    }
