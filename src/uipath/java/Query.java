package uipath.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

import java.util.Properties;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.WebRowSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.rowset.WebRowSetImpl;

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

    public static JSONObject uipathSelectQueryJSON(Connection connection, String query) {
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

    public static String uipathSelectQueryWebRowSet(Connection connection, String query) throws SQLException {
        WebRowSet webRS = new WebRowSetImpl();
        StringWriter sw = new StringWriter();
        webRS.setCommand(query);
        webRS.execute(connection);
        webRS.writeXml(sw);
        System.out.println(sw.toString());
        webRS.close();
        
        // try {
        //     // RowSetMetaData rsMD = (RowSetMetaData) webRS.getMetaData();
        //     // System.out.println("rsMD=" + rsMD);
        //     // if (rsMD == null) {
        //     //     System.out.println("vendor does not support RowSetMetaData");
        //     //   } else {
        //     //     int columnCount = rsMD.getColumnCount();
        //     //     System.out.println("columnCount=" + columnCount);
        //     //   };
        // } catch (Exception e) {
        //     //TODO: handle exception   
        //     e.printStackTrace();
        // }
        return sw.toString();
    }

    public static String uipathSelectQueryDocument(Connection connection, String query) throws ParserConfigurationException, SQLException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.newDocument();
        StringWriter writer = new StringWriter();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            
            Element results = doc.createElement("Results");
            doc.appendChild(results);
            
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int colCount = rsmd.getColumnCount();
    
            while (resultSet.next())
            {
                Element row = doc.createElement("Row");
                results.appendChild(row);
    
                for (int i = 1; i <= colCount; i++)
                {
                    String columnName = rsmd.getColumnName(i);
                    Object value = resultSet.getObject(i);
    
                    Element node = doc.createElement(columnName);
                    node.appendChild(doc.createTextNode(value.toString()));
                    row.appendChild(node);
                }
            }
            
            DOMSource domSource = new DOMSource(doc);
            StreamResult result = new StreamResult(writer);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.transform(domSource, result);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return writer.toString();
    }

    public static void printDocument(Document doc, OutputStream out) throws IOException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
    
        transformer.transform(new DOMSource(doc), 
             new StreamResult(new OutputStreamWriter(out, "UTF-8")));
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

            // System.out.println(Query.uipathSelectQueryJSON(connection));
            // Query.uipathSelectQueryWebRowSet(connection);
            System.out.println(Query.uipathSelectQueryDocument(connection, "select * from regions"));
            // printDocument(Query.uipathSelectQueryDocument(connection), System.out);
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
