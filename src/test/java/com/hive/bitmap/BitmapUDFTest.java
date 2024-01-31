package com.hive.bitmap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;


public class BitmapUDFTest {

    private SparkConf sparkConf = new SparkConf().setAppName("build job");

    private  SparkSession spark = SparkSession.builder().enableHiveSupport()
            .master("local")
            .config(sparkConf).getOrCreate();

    @Before
    public void init() {
        spark.sql("CREATE TEMPORARY FUNCTION to_bitmap AS 'com.hive.bitmap.udf.ToBitmapUDAF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_union AS 'com.hive.bitmap.udf.BitmapUnionUDAF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_count AS 'com.hive.bitmap.udf.BitmapCountUDF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_and AS 'com.hive.bitmap.udf.BitmapAndUDF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_or AS 'com.hive.bitmap.udf.BitmapOrUDF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_xor AS 'com.hive.bitmap.udf.BitmapXorUDF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_to_array AS 'com.hive.bitmap.udf.BitmapToArrayUDF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_from_array AS 'com.hive.bitmap.udf.BitmapFromArrayUDF'");
        spark.sql("CREATE TEMPORARY FUNCTION bitmap_contains AS 'com.hive.bitmap.udf.BitmapContainsUDF'");
    }

    @Test
    public void bitmapToArrayUDFTest() {
        spark.sql("select bitmap_count(bitmap_from_array(array(1,2,3,4,5))) AS `cnt=5`").show();
        spark.sql("select bitmap_to_array(bitmap_from_array(array(1,2,3,4,5)))").show();
        spark.sql("select bitmap_to_array(bitmap_and(bitmap_from_array(array(1,2,3,4,5)),bitmap_from_array(array(1,2))))").show();
        spark.sql("select bitmap_to_array(bitmap_or(bitmap_from_array(array(1,2,3)),bitmap_from_array(array(5))))").show();
        spark.sql("select bitmap_to_array(bitmap_or(bitmap_from_array(array(1,2,3)),bitmap_from_array(array(3))))").show();
        spark.sql("select bitmap_contains(bitmap_from_array(array(1,2,3)),2)").show();



    }
}
