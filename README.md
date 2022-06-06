# hive-bitmap-udf

bitmap bitmap udf 在 hive 中使用 bitmap 实现精确去重功能

## 1. 在hive中创建UDF
```
add jar hdfs://node:9000/hive-bitmap-udf-jar-with-dependencies.jar;

CREATE TEMPORARY FUNCTION to_bitmap AS 'com.hive.bitmap.udf.ToBitmapUDAF';
CREATE TEMPORARY FUNCTION bitmap_union AS 'com.hive.bitmap.udf.BitmapUnionUDAF';

CREATE TEMPORARY FUNCTION bitmap_count AS 'com.hive.bitmap.udf.BitmapCountUDF';
CREATE TEMPORARY FUNCTION bitmap_and AS 'com.hive.bitmap.udf.BitmapAndUDF';
CREATE TEMPORARY FUNCTION bitmap_or AS 'com.hive.bitmap.udf.BitmapOrUDF';
CREATE TEMPORARY FUNCTION bitmap_xor AS 'com.hive.bitmap.udf.BitmapXorUDF';
```

## 2. UDF说明

|  UDF           |             描述                   | 案例       |结果类型|
| :-----------:  | :-------------------------------: |:-------------: |:-----: |
|   to_bitmap    |  将num（int或bigint） 转化为 bitmap  |to_bitmap(num)  |   bitmap              |
|   bitmap_union |  多个bitmap合并为一个bitmap（并集）   |bitmap_union(bitmap)|   bitmap              |
|   bitmap_count |  计算bitmap中存储的num个数           |bitmap_count(bitmap)|   long              |
|   bitmap_and   |  计算两个bitmap交集                 |bitmap_and(bitmap1,bitmap2)|   bitmap |             |
|   bitmap_or    |  计算两个bitmap并集                 |bitmap_or(bitmap1,bitmap2)|   bitmap |
|   bitmap_xor   |  计算两个bitmap差集                 |bitmap_xor(bitmap1,bitmap2)|   bitmap |


## 3. 在 hive 中创建 bitmap 类型表,导入数据并查询
```
CREATE TABLE IF NOT EXISTS `hive_bitmap_table`
( 
    k      int      comment 'id',
    bitmap binary   comment 'bitmap'
) comment 'hive bitmap 类型表' 
STORED AS ORC;

-- 数据写入
insert into table  hive_bitmap_table select  1 as id,to_bitmap(1) as bitmap;
insert into table hive_bitmap_table select  2 as id,to_bitmap(2) as bitmap;

-- 查询

select bitmap_union(bitmap) from hive_bitmap_table;
select bitmap_count(bitmap_union(bitmap)) from hive_bitmap_table;

```