package spark.batch.tp21;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class WordCountTask {
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: WordCountTask <input file> <output dir>");
            System.exit(1);
        }
        
        new WordCountTask().run(args[0], args[1]);
    }
    
    public void run(String inputFilePath, String outputDir) {
        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName());
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        try {
            JavaRDD<String> textFile = sc.textFile(inputFilePath);
            
            JavaPairRDD<String, Integer> counts = textFile
                    .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b);
            
            counts.saveAsTextFile(outputDir);
            
            System.out.println("Job terminé avec succès!");
            System.out.println("Résultats sauvegardés dans: " + outputDir);
            
        } finally {
            sc.close();
        }
    }
}
