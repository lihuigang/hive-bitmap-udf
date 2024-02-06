# hive-bitmap-udf

在hive、spark中使用Roaring64Bitmap实现精确去重功能
主要目的：
1. 提升 hive、spark 中精确去重性能，代替hive或Spark 中的 count(distinct uuid)；
2. 节省 hive 存储 ，使用 bitmap 对数据压缩 ，减少了存储成本；
3. 提供在 hive、spark 中 bitmap 的灵活运算 ，比如：交集、并集、差集运算 ，计算后的 bitmap 也可以直接写入 hive 表中；

如果方便的话，还请各位帮忙点个star，为开源项目加油！
## 1. 项目编译
```angular2html
java 版本：1.8
```
```angular2html
mvn clean package
```
编译完成后使用jar包：hive-bitmap-udf.jar
## 2. 在hive中创建UDF
将 hive-bitmap-udf.jar 上传至HDFS系统或者放到本地，在spark或者hive中注册使用
```
add jar hdfs://node:9000/hive-bitmap-udf.jar;

CREATE TEMPORARY FUNCTION to_bitmap AS 'com.hive.bitmap.udf.ToBitmapUDAF';
CREATE TEMPORARY FUNCTION bitmap_union AS 'com.hive.bitmap.udf.BitmapUnionUDAF';

CREATE TEMPORARY FUNCTION bitmap_count AS 'com.hive.bitmap.udf.BitmapCountUDF';
CREATE TEMPORARY FUNCTION bitmap_and AS 'com.hive.bitmap.udf.BitmapAndUDF';
CREATE TEMPORARY FUNCTION bitmap_or AS 'com.hive.bitmap.udf.BitmapOrUDF';
CREATE TEMPORARY FUNCTION bitmap_xor AS 'com.hive.bitmap.udf.BitmapXorUDF';
CREATE TEMPORARY FUNCTION bitmap_to_array AS 'com.hive.bitmap.udf.BitmapToArrayUDF';
CREATE TEMPORARY FUNCTION bitmap_from_array AS 'com.hive.bitmap.udf.BitmapFromArrayUDF';
CREATE TEMPORARY FUNCTION bitmap_contains AS 'com.hive.bitmap.udf.BitmapContainsUDF';

```

## 3. UDF说明

|        UDF        |             描述              |                案例                |     结果类型      |
|:-----------------:|:---------------------------:|:--------------------------------:|:-------------:|
|     to_bitmap     | 将num（int或bigint） 转化为 bitmap |          to_bitmap(num)          |    bitmap     |
|   bitmap_union    |   多个bitmap合并为一个bitmap（并集）   |       bitmap_union(bitmap)       |    bitmap     |
|   bitmap_count    |      计算bitmap中存储的num个数      |       bitmap_count(bitmap)       |     long      |
|    bitmap_and     |        计算两个bitmap交集         |   bitmap_and(bitmap1,bitmap2)    |    bitmap     |
|     bitmap_or     |        计算两个bitmap并集         |    bitmap_or(bitmap1,bitmap2)    |    bitmap     |
|    bitmap_xor     |        计算两个bitmap差集         |   bitmap_xor(bitmap1,bitmap2)    |    bitmap     |
| bitmap_from_array |  array 转化为bitmap         	  |     bitmap_from_array(array)     |    bitmap     |
|  bitmap_to_array  |       bitmap转化为array        |     bitmap_to_array(bitmap)      | array<bigint> |
|  bitmap_contains  |   bitmap是否包含另一个bitmap全部元素   | bitmap_contains(bitmap1,bitmap2) |    boolean    |
|  bitmap_contains  |       bitmap是否包含某个元素        |   bitmap_contains(bitmap,num)    |    boolean    |


## 4. 在 hive 中创建 bitmap 类型表,导入数据并查询
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

## 5. 在 hive 中使用 bitmap 实现精确去重
```
CREATE TABLE IF NOT EXISTS `hive_table`
( 
    k      int      comment 'id',
    uuid   bigint   comment '用户id'
) comment 'hive 普通类型表' 
STORED AS ORC;

-- 普通查询（计算去重人数）

select count(distinct uuid) from hive_table;

-- bitmap查询（计算去重人数）

select bitmap_count(to_bitmap(uuid)) from hive_table;

```
