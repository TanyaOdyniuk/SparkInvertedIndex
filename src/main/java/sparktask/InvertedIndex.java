package sparktask;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Odyniuk on 01/04/2017.
 */
public class InvertedIndex {
    public static void main(String[] args) {
        String master = "local[*]";
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir").concat("/winutils"));
        SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster(master);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaPairRDD<String,String> files = sparkContext.wholeTextFiles("20_newsgroup/*/*");

        JavaPairRDD<String, String> withoutServiceInfo = files.flatMapValues(text -> Arrays.asList(text.split("\n")))
                .filter(text -> !forFilter(text._2()))
                .filter(text-> !text._2().isEmpty())
                .filter(text -> !text._2().matches("^.+ wr[io]tes*:"))
                .mapValues(text-> text.trim().toLowerCase());

        JavaPairRDD<String, String> words = withoutServiceInfo.flatMapValues(text -> Arrays.asList(
                text.split("[\\p{javaWhitespace}\\.\\,\\:\\;\\!\\?\\(\\)\\<\\>\\\"\\{\\}]")))
                .filter(text -> text._2().matches("[a-z]+('[a-z]+){0,1}"));

        JavaPairRDD<String,String> swappedWords= words.mapToPair(item->item.swap());

        JavaPairRDD<String, Tuple2<Integer, String>> result = swappedWords.mapValues(path -> new Tuple2<>(1, path))
                .reduceByKey(
                        (field1, field2)->{ Tuple2<Integer, String> tuple;
                            if(!field1._2.contains(field2._2))
                                tuple = new Tuple2<>(field1._1+field2._1, field1._2+" "+field2._2);
                            else tuple = new Tuple2<>(field1._1+field2._1, field1._2);
                            return tuple;
                        });
        result.coalesce(1).saveAsTextFile("index.csv");
    }

    private static List<String> initServiceInfo(){
        List<String> serviceInfo = new ArrayList<>();
        serviceInfo.add("Path:");
        serviceInfo.add("Xref:");
        serviceInfo.add("From:");
        serviceInfo.add("Newsgroups:");
        serviceInfo.add("Subject:");
        serviceInfo.add("Summary:");
        serviceInfo.add("Keywords:");
        serviceInfo.add("Message-ID:");
        serviceInfo.add("Date:");
        serviceInfo.add("Expires:");
        serviceInfo.add("Followup-To:");
        serviceInfo.add("Distribution:");
        serviceInfo.add("Organization:");
        serviceInfo.add("Approved:");
        serviceInfo.add("Supersedes:");
        serviceInfo.add("Lines:");
        serviceInfo.add("Archive-name:");
        serviceInfo.add("Alt-atheism-archive-name:");
        serviceInfo.add("Last-modified:");
        serviceInfo.add("Version:");
        serviceInfo.add("Write to:");
        serviceInfo.add("Telephone:");
        serviceInfo.add("Fax:");
        serviceInfo.add("NNTP-Posting-Host:");
        serviceInfo.add("References:");
        serviceInfo.add("News-Software:");
        serviceInfo.add("Article-I.D.:");
        serviceInfo.add("Reply-To:");
        serviceInfo.add("e-mail:");
        serviceInfo.add("X-Newsreader:");
        serviceInfo.add("In-reply-to:");
        serviceInfo.add("X-Disclaimer:");
        serviceInfo.add("Originator:");
        serviceInfo.add("To:");
        serviceInfo.add("Sender:");
        serviceInfo.add("Re:");
        return serviceInfo;
    }
    private static Boolean forFilter(String str){
        List<String> serviceTokens = initServiceInfo();
        Iterator<String> iterator = serviceTokens.iterator();
        Boolean isContains = false;
        while(iterator.hasNext() && !isContains)
        {
            isContains = str.contains(iterator.next());
        }
        return isContains;
    }
}