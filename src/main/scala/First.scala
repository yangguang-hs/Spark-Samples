import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, expr, lit, udf, when}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}

object First {
  def main(args: Array[String]): Unit = {
    println("hello world")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("My First App")

    sparkConf.setMaster("local[*]")
    var spark = SparkSession.builder().config(sparkConf).getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")

    //load data from files and join together, and add extra column
    val path1 = "/Users/yangguang/Downloads/SG211215OTHRTGUF_Del Monte Pacific Limited_20211215172340_00_FS_2Q_20211031.4_2021-12-16 15_50_41_PL.csv"
    val path2 = "/Users/yangguang/Downloads/SG211215OTHRTGUF_Del Monte Pacific Limited_20211215172340_00_FS_2Q_20211031.4_2021-12-16 15_50_40_BS.csv"
    val path3 = "/Users/yangguang/Downloads/SG211215OTHRTGUF_Del Monte Pacific Limited_20211215172340_00_FS_2Q_20211031.4_2021-12-16 15_50_40_CF.csv"

    val df1 = spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load(path1)
        .withColumn("File Type",lit("PL"))
      .union(spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load(path2)
        .withColumn("File Type",lit("BS")))
      .union(spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load(path3)
        .withColumn("File Type",lit("CF")))


    df1.show()

    //get all column name
    val allColumnNames=df1.columns
    //generate new name based on existing names
    val newColNames = allColumnNames.map(o=>o.replace("PL ",""))
    //change all column names to new column names
    val columnsList = allColumnNames.zip(newColNames).map(f=>{col(f._1).as(f._2)})
      //change column type to int
    val df2 = df1.select(columnsList:_*).withColumn("Amount",col("Amount").cast("int"))
    df2.show()
    //show(count,truncate)
    df2.select(col("Mapped Values")).distinct().orderBy(col("Mapped Values")).show(50,false)
    //filter based on column name and column index
    df2.filter(
        (col("Mapped Values") === "Total Revenue")
          || (df2("Mapped Values") like  "Non-current%")
    ).show()

    //case when to add new column
    df2.select(col("*"),
      when(col("Reporting Type") === "QTR", "Quarter")
      .otherwise("FYE").alias("New Reporting Type")
    ).show()

    df2.groupBy("Mapped Values").pivot("Period Begin Date").avg("Amount").show()
    //check if df contains a field
    println(df2.schema.fieldNames.contains("Amount"))
    println(df2.schema.fieldNames.contains("firstname"))
    df2.groupBy("Mapped Values").sum("Amount").show()
    // define a struct
    val arrayStructureData = Seq(
      Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
      Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
      Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
      Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
      Row(Row("Jen","Mary","Brown"),List("Blogging","Football"),Map("hair"->"black","eye"->"black"))
    )
    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))
    val df3 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df3.printSchema()
    df3.show()
    //filter in struct/array/map
    df3.filter(col("name.firstname") === "James ").show()
    df3.filter(array_contains(col("hobbies"),"Football")).show()
    df3.filter(col("properties").getItem("hair") === "black").show()

    //UDF
    //1 create a function
    val convertCase =  (strQuote:String) => {
      val arr = strQuote.split(" ")
      arr.map(f=>  f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString(" ")
    }
    //2 convert this function convertCase() to UDF
    val convertUDF = udf(convertCase)
    df2.select(col("Amount"),col("Mapped Values"),
      convertUDF(col("Line Text")).as("Line Text") ).show(false)
  }
}

