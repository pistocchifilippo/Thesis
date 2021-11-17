package edu.upc.essi.dtim.nextiaqr.models.querying.wrapper_impl;

import edu.upc.essi.dtim.nextiaqr.models.querying.RelationalSchema;
import edu.upc.essi.dtim.nextiaqr.models.querying.Wrapper;
import edu.upc.essi.dtim.nextiaqr.utils.SQLiteUtils;
import edu.upc.essi.dtim.nextiaqr.utils.Utils;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CSV_Wrapper extends Wrapper {

    private String path;
    private String columnDelimiter;
    private String rowDelimiter;
    private boolean headerInFirstRow;

    public CSV_Wrapper(String name) {
        super(name);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getColumnDelimiter() {
        return columnDelimiter;
    }

    public void setColumnDelimiter(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    public String getRowDelimiter() {
        return rowDelimiter;
    }

    public void setRowDelimiter(String rowDelimiter) {
        this.rowDelimiter = rowDelimiter;
    }

    public boolean isHeaderInFirstRow() {
        return headerInFirstRow;
    }

    public void setHeaderInFirstRow(boolean headerInFirstRow) {
        this.headerInFirstRow = headerInFirstRow;
    }

    @Override
    public void inferSchema() throws Exception {
        SparkSession spark = Utils.getSparkSession();
        Dataset<Row> ds = spark.read()
                .option("header",String.valueOf(this.headerInFirstRow))
                .option("delimiter",this.columnDelimiter)
                .csv(this.path);

        RelationalSchema schema = new RelationalSchema();
        schema.setAttributes(Arrays.asList(ds.schema().fieldNames()));
        setSchema(schema);

        spark.close();
    }

    @Override
    public String preview(List<String> attributes) throws Exception {
        JSONArray data = new JSONArray();
        SparkSession spark = Utils.getSparkSession();
        Dataset<Row> ds = spark.read()
                .option("header",String.valueOf(this.headerInFirstRow))
                .option("delimiter",this.columnDelimiter)
                .csv(this.path);
        String tableName = UUID.randomUUID().toString().replace("-","");
        ds.createTempView(tableName);

        spark.sql("select "+String.join(",",
                attributes.stream().filter(a->!a.isEmpty()).map(a -> "`"+a+"`")
                        .collect(Collectors.toList()))+" from "+tableName+" limit 10")
                .toJavaRDD()
                .collect()
                .forEach(r -> {
                    JSONArray arr = new JSONArray();
                    attributes.stream().filter(a->!a.isEmpty()).forEach(a -> {
                        JSONObject datum = new JSONObject();
                        datum.put("attribute",a);
                        datum.put("value",String.valueOf(r.get(r.fieldIndex(a))));
                        arr.add(datum);
                    });
                    data.add(arr);
                });
        spark.close();
        JSONObject res = new JSONObject(); res.put("data",data);
        return res.toJSONString();
    }

    @Override
    public void populate() throws Exception {
        JSONArray data = new JSONArray();
        SparkSession spark = Utils.getSparkSession();
        Dataset<Row> ds = spark.read()
                .option("header",String.valueOf(this.headerInFirstRow))
                .option("delimiter",this.columnDelimiter)
                .csv(this.path);
        String tableName = UUID.randomUUID().toString().replace("-","");
        ds.createTempView(tableName);

        spark.sql("select "+String.join(",",
                this.getSchema().getAttributes().stream().filter(a->!a.isEmpty() && !a.equals("\'\'"))
                        .map(a ->  "`"+a.replace("'","")+"`")
                        .collect(Collectors.toList()))+" from "+tableName)                .javaRDD()
                .collect()
                .forEach(r -> {
                    JSONArray arr = new JSONArray();
                    this.getSchema().getAttributes().stream().filter(a->!a.isEmpty() && !a.equals("\'\'")).forEach(a -> {
                        JSONObject datum = new JSONObject();
                        String att = a.replace("'","");
                        datum.put("attribute",att);
                        datum.put("value",String.valueOf(r.get(r.fieldIndex(att))));
                        arr.add(datum);
                    });
                    data.add(arr);
                });
        spark.close();
        SQLiteUtils.insertData(this,data);
    }
}
