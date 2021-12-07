package edu.upc.essi.dtim.nextiaqr.utils;

import edu.upc.essi.dtim.nextiaqr.jena.GraphOperations;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import edu.upc.essi.dtim.nextiaqr.models.querying.Wrapper;


import java.sql.*;

public class SQLiteUtils {

    public static void createTable(Wrapper w) {
        Connection conn = Utils.getSQLiteConnection();

        String droptable = "DROP TABLE IF EXISTS "+GraphOperations.nn(w.getWrapper())+";";
        try {
            conn.createStatement().executeUpdate(droptable);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        conn = Utils.getSQLiteConnection();
        StringBuilder SQL = new StringBuilder("CREATE TABLE "+ GraphOperations.nn(w.getWrapper())+" (");
        w.getSchema().getAttributes().forEach(a -> SQL.append(a+" text,"));

        String createStatement = SQL.toString().substring(0,SQL.toString().length()-1) + ");";

        System.out.println(createStatement);

        try {
            conn.createStatement().executeUpdate(createStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }

    public static void insertData(Wrapper w, JSONArray data) {
        Connection conn = Utils.getSQLiteConnection();

        data.forEach(tuple -> {
            String SQL = "INSERT INTO "+GraphOperations.nn(w.getWrapper())+" ";
            StringBuilder schema = new StringBuilder("(");
            StringBuilder values = new StringBuilder("(");
            ((JSONArray)tuple).forEach(datum -> {
                schema.append("'"+((JSONObject)datum).getAsString("attribute")+"'"+",");
                values.append("'"+((JSONObject)datum).getAsString("value").replace("'","")+"',");
            });
            SQL += schema.substring(0,schema.length()-1)+") VALUES "+values.substring(0,values.length()-1)+");";
            System.out.println(SQL);

            try {
                conn.createStatement().executeUpdate(SQL);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        try {
            conn.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static String executeSelect(String sql/*,List<String> features*/) {
        StringBuilder out = new StringBuilder();
        Connection conn = Utils.getSQLiteConnection();
        JSONArray data = new JSONArray();
        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) out.append(", ");
                    String columnValue = rs.getString(i);
                    out.append(columnValue);
                }
                out.append("\n");
            }


            /*while (rs.next()) {
                JSONArray arr = new JSONArray();
                for (int i = 0; i < features.size(); ++i) {
                    JSONObject datum = new JSONObject();
                    //datum.put("feature",features.get(i));
                    datum.put("value",rs.getString(i));
                    arr.add(datum);
                }
                data.add(arr);
            }*/
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return out.toString();
    }


}
