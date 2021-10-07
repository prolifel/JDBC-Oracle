package uipath.java;

import java.sql.*;
import java.util.Properties;
import java.io.*;

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
            Query.selectQuery(connection);
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
