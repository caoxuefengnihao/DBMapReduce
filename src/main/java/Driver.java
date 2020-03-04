import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/dmtest", "root", "123456");
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(Driver.class);
        job.setInputFormatClass(DBInputFormat.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(DBInput.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String[] fields = {"word","value"};
        DBInputFormat.setInput(job,WordValue.class,"w","","word",fields);
        FileOutputFormat.setOutputPath(job,new Path("/wordcoundresult"));
        job.setNumReduceTasks(0);
        //提交作业
        job.waitForCompletion(true);

    }
}
