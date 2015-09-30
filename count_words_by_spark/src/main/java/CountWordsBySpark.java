import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

/**
 * bin/spark-submit --class "CountWordsBySpark" --master local[12] java-learning/count_words_by_spark/out/artifacts/count_words_by_spark_jar/count_words_by_spark.jar
 */
public class CountWordsBySpark {
    public static void main(String[] args) {
        String logFile = "a.log"; // Should be some file on your system

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();
        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();
        System.out.println("------------------------Lines with a: " + numAs + ", lines with b: " + numBs);

        long numByField = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                String[] token = s.split(";");
                boolean existed = false;
                for (int i = 0; i < token.length; i++) {
                    if (i == 7) {
                        String timeInHdfs = token[i]; //2015-06-30 14:00:29.0                System.out.println(timeInHdfs);
                        if (!timeInHdfs.equalsIgnoreCase("null") && timeInHdfs.compareTo("2015-06-29 23:59:59") > 0) {
                            existed = true;
                        }
                    }
                }
                return existed;
            }
        }).count();
        System.out.println("-----------------------------------------------------------------------");
        System.out.println("Lines with bigger time: numByField: " + numByField);

        JavaRDD<Integer> lineLengths = logData.map(s -> s.length());
        int total = lineLengths.reduce((a, b) -> a + b);
        System.out.println("-----------------------------------------------------------------------");
        System.out.println("Total chars in log: " + total);
        logData.take(100).forEach(s -> System.out.println(s));
    }
}
