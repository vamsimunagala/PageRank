import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        private Text page = new Text();
        private Text rankAndLinks = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String pageName = parts[0];
            String[] links = parts[1].split(",");
            double initialRank = 1.0 / links.length; // Initialize rank equally among outgoing links
            for (String link : links) {
                page.set(link);
                rankAndLinks.set(initialRank + "");
                context.write(page, rankAndLinks);
            }
            page.set(pageName);
            rankAndLinks.set("\t" + parts[1]);
            context.write(page, rankAndLinks);
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double newRank = 0.0;
            String links = "";
            for (Text value : values) {
                String strValue = value.toString();
                if (strValue.contains("\t")) {
                    links = strValue.substring(strValue.indexOf("\t") + 1);
                } else {
                    newRank += Double.parseDouble(strValue);
                }
            }
            result.set(newRank + "\t" + links);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Page Rank ");

        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
