package edu.upc.essi.dtim.nextiaqr.utils;

import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Utils {

    public static SparkSession getSparkSession(){
        SparkSession spark = SparkSession.builder()
                .appName("parquetPreview")
                .master("local[*]")
                .config("spark.driver.bindAddress","localhost")
                .config("spark.driver.memory","471859200")
                .config("spark.testing.memory", "471859200")
                .getOrCreate();
        spark.sparkContext().setLogLevel("OFF");
        return spark;
    }

    public static Connection getSQLiteConnection() {
        String url = "jdbc:sqlite:"+new File("data/sqlite/nextiaqr.db").getAbsolutePath();
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static InputStream getResourceAsStream(String filename) {
        InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        return in;
    }

}
