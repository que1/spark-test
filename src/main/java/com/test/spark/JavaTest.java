package com.test.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * Created by admin on 2017/9/26.
 */
public class JavaTest {

    public static void main(String[] args) {
        //
        SparkConf conf = new SparkConf().setMaster("spark://data-center:7077").setAppName("java-test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 4, 5));
        JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer x) throws Exception {
                return x * x;
            }
        });

        System.out.println(org.apache.commons.lang3.StringUtils.join(result.collect(), ", "));

    }

}
