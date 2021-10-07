package uipath.java;

import java.sql.*;
import java.util.Properties;
import java.io.*;
import org.json.JSONArray;
import org.json.JSONObject;
public class Query {
    public String dbDriverClass, dbUrl, dbUsername, dbPassword;
    private Properties properties;

    // constructor
    public Query(String envPath) throws FileNotFoundException, IOException {
        super();
        this.setProperties(envPath);
    }

    // get jdbc's properties from .env
    private void setProperties(String envPath) throws FileNotFoundException, IOException {
        this.properties = new Properties();
        FileInputStream fileInputStream = new FileInputStream(envPath);
        properties.load(fileInputStream);

        this.dbDriverClass = this.properties.getProperty("DB_DRIVER_CLASS");
        this.dbUrl = this.properties.getProperty("DB_URL");
        this.dbUsername= this.properties.getProperty("DB_USERNAME");
        this.dbPassword = this.properties.getProperty("DB_PASSWORD");

        System.out.println("Read .env success");
    }

    // get connection
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Connection connection = null;
        Properties connectionProperties = new Properties();

        connectionProperties.put("user", this.dbUsername);
        connectionProperties.put("password", this.dbPassword);

        Class.forName(this.dbDriverClass);
        connection = DriverManager.getConnection(this.dbUrl, connectionProperties);
        
        System.out.println("Get connection success");
        return connection;
    }

    // close connection
    public static void closeConnection(Connection connection) throws SQLException {
        connection.close();
        System.out.println("Connection closed");
    }

    public static void selectQuery(Connection connection) {
        String query = "select * from regions";

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int id = resultSet.getInt("region_id");
                String region_name = resultSet.getString("region_name");
                System.out.println("id: " + id + " name: " + region_name);
            }
        } catch (SQLException e) {
            e.printStackTrace();            
        }
    }

    public static Connection uipathCreateConnection(String envPath) throws FileNotFoundException, IOException {
        Query query;
        Connection connection = null;
        
        query = new Query(envPath);

        try {
            connection = query.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connection;
    }

    public static JSONObject uipathSelectQuery(Connection connection) {
        String query = "select * from regions";
        JSONObject json = new JSONObject();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            while (resultSet.next()) {
                int numColumn = resultSetMetaData.getColumnCount();
                JSONObject queryPerRow = new JSONObject();

                for (int i = 1; i <= numColumn; i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    queryPerRow.put(columnName, resultSet.getObject(columnName));
                }
                json.put(String.valueOf(resultSet.getRow()), queryPerRow);
            }
        } catch (SQLException e) {
            e.printStackTrace();            
        }

        return json;
    }

    public static void main(String[] args) throws SQLException {
        Query query;
        Connection connection = null;

        try {
            query = new Query(args[0]);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return;
        } catch (IOException e1) {
            e1.printStackTrace();
            return;
        }
        
        try {
            connection = query.getConnection();

            // create sql command here
            // Query.selectQuery(connection);

            System.out.println(Query.uipathSelectQuery(connection));
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Query.closeConnection(connection);
        }
        // runSelect(DBConnection.getConnection());
    }

    public static void runSelect(Connection conn) {
        String query = "select * from regions";

        try {
            // Connection conn = DBConnection.getConnection();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int id = resultSet.getInt("region_id");
                String region_name = resultSet.getString("region_name");
                System.out.println("id: " + id + " name: " + region_name);
            }
        } catch (SQLException e) {
            e.printStackTrace();            
        }
    }
}
