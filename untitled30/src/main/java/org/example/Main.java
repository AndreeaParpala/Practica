package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SaveMode;


public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("D:/lucru_java_intellij/untitled30/src/main/resources/erasmus.csv");

        dataset.printSchema();
        dataset=dataset.filter(functions.col("Receiving Country Code").isin("RO", "FR","ES"));
        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy(functions.col("Receiving Country Code").desc())
                .show(100);
        saveData(dataset,"RO","Romania");
        saveData(dataset,"FR","Franta");
        saveData(dataset,"ES","Estonia");
    }

    public static void saveData(Dataset<Row> dataset, String countryCode, String tableName) {
        dataset
                .filter(col("Receiving Country Code").isin(countryCode))
                .groupBy("Receiving Country Code", "Sending Country Code")
                .count().orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/tema?serverTimezone=UTC")
                .option("dbtable", tableName)
                .option("user", "root")
                .option("password", "123456")
                .save(tableName + ".tema");
    }
}