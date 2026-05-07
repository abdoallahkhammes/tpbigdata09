package spark.streaming.tp22;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import java.util.Arrays;

public class Stream {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("NetworkWordCount")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> lines = spark
                .readStream()
                .format("socket")
                .option("host", "0.0.0.0")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        Dataset<String> words = lines
                .flatMap((String x) -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<org.apache.spark.sql.Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        query.awaitTermination();
    }
}
