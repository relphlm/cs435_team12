import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class Count {

    public static class CountEntriesMapper extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valuesSplit = value.toString().split(" ");
            Text mapperKey = new Text(valuesSplit[0].split(":")[0]);
            IntWritable mapperVal = new IntWritable(1);
            context.write(mapperKey, mapperVal);
        }
    }

    public static class CountEntriesReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v :
                    values) {
                count ++;
            }
            IntWritable finalCount = new IntWritable(count);
            Text modifiedKey = new Text(key + ":");
            context.write(modifiedKey, finalCount);
        }
    }

    ///Job 2

    public static class DescendingMapper extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valuesSplit = value.toString().split(":");
            Text mapperKey = new Text(valuesSplit[0].trim());
            IntWritable mapperValue = new IntWritable(Integer.parseInt(valuesSplit[1].trim()));
            context.write(mapperKey, mapperValue);
        }
    }

    public static class DescendingReducer extends Reducer <Text, IntWritable, Text, IntWritable>{
            private ArrayList<Pair<String, Integer>> stateCount;
            @Override
            protected void setup(Context context) {
                stateCount = new ArrayList<>();
            }

            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
                for (IntWritable edge : values) {
        //                System.out.println("Reducer PUT: " + key.toString() + ", " +  edge.get());
                    Pair tempPair = new Pair(key.toString(), edge.get());
                    stateCount.add(tempPair);
                }
            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {
                stateCount.sort((o1, o2) ->
                        o2.getValue() - o1.getValue());

                for (int i = 0; i < stateCount.size(); i++) {
                    Text nodeID = new Text(stateCount.get(i).getKey());
                    IntWritable edgeCount = new IntWritable(stateCount.get(i).getValue());
                    context.write(nodeID, edgeCount);
                }
            }

    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Count state entries");
        job1.setJarByClass(Count.class);
        job1.setMapperClass(CountEntriesMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(CountEntriesReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(3);
        FileInputFormat.addInputPath(job1, new Path(args[0] + "/SelectTargetLocations/0School0Work"));
        FileOutputFormat.setOutputPath(job1, new Path(args[0] + "/SelectTargetLocations/countTempBottomFive"));
        job1.waitForCompletion(true);

        /// Job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort by descending");
        job2.setJarByClass(Count.class);
        job2.setMapperClass(DescendingMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setReducerClass(DescendingReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[0] + "/SelectTargetLocations/countTempBottomFive"));
        FileOutputFormat.setOutputPath(job2, new Path(args[0] + "/SelectTargetLocations/countFinalBottomFive"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
