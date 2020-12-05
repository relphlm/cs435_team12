import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The purpose of this class is to parse through the Covid_government_response dataset, and filter through states that have had a higher number of
 * imposed restrictions. This will allow us to narrow our focused locations within our ar quality datasets
 */

public class SelectOnlyUSLocations {

    public static class SelectTargetLocationsMapper extends Mapper<Object, Text, Text, NullWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String CSVLine = value.toString();
            System.out.println(CSVLine);
            String countryName = CSVLine.split(",")[0];
            if (countryName.equals("United States")){
                Text countryValue = new Text(CSVLine);
                context.write(countryValue, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Select US locations only");
        job.setJarByClass(SelectOnlyUSLocations.class);
        job.setMapperClass(SelectTargetLocationsMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]  + "/SelectTargetLocations/selectUnitedStatesLocations"));
        job.waitForCompletion(true);

        //Job 2: Narrow down hotspot states with the restrictions they implement

    }
}
