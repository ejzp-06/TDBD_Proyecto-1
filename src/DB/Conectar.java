//Devon's note: Replace the variables here with the variables of your database. Kuha mo?


package DB;

import java.sql.*;
import javax.swing.JOptionPane;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;


public class Conectar {
    public static Connection connection = null;
    public java.sql.Statement at = null;
    public java.sql.ResultSet rt = null;
    public String connectionString = "";
    public String user = "";
    public String password = "";
    ArrayList <String[]> result = new ArrayList<String[]>();
    
    
    public static void main (String[] args) throws SQLException, ClassNotFoundException
    {
       
    }
    
    //CONECCION CON LA BASE DE DATOS
    public boolean conectar(String connectionString, String user, String password) throws SQLException{
        System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			JOptionPane.showMessageDialog(null, e.getMessage());
			return false;
		}

		System.out.println("PostgreSQL JDBC Driver Registered!");

		

		try {
                        
			connection = DriverManager.getConnection(connectionString, user, password);
                                
		} catch (SQLException e) {

			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
                        JOptionPane.showMessageDialog(null, e.getMessage());
			return false;

		}
                
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
                
                return true;
    }
    
    //funcion general
    public void execute_query(String sql){
        try {
        at = connection.createStatement();
        rt = at.executeQuery(sql);
        }
        catch(SQLException e){
            if(e.getMessage().equals("No results were returned by the query."))
                JOptionPane.showMessageDialog(null,"Operacion realizada exitosamente.");
            else 
                JOptionPane.showMessageDialog(null,e.getMessage());
        }
    }
    
    //tablas
    
    public ArrayList listar_tablas(String sql){
        try {
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery(sql);
            int columnCount = 3;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, e.getMessage());
            //e.setStackTrace(stackTrace);
            return null;
        } 
    }
    
    public ArrayList getColumnNames(String table){
        try {
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery("select column_name from information_schema.columns where table_name='"+table+"';");
            int columnCount = 1;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, e.getMessage());
            return null;
        } 
    }
    
    public ArrayList get_tableNames(String schema){
        try {
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery("select * from pg_catalog.pg_tables where schemaname!= 'pg_catalog' and schemaname!='information_schema' and schemaname='"+schema+"';");
            int columnCount = 2;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, e.getMessage());
            return null;
        } 
    }
    
    public int get_NumColumns(String table){
        int columns =0;
        try {
        at = connection.createStatement();
        rt = at.executeQuery("select count(*) from information_schema.columns where table_name='"+table+"';");
        rt.next();
        columns = rt.getInt(1);

        }
        catch(SQLException e){
            
        }
        
        return columns;
    }
    
    public ArrayList populateJtable(String table, String schema){
        try {
            ArrayList<String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            int columnCount = get_NumColumns(table);

            if (schema.equals("public")) {
                rt = at.executeQuery("select * from " + table + ";");
            } else {
                rt = at.executeQuery("select * from " + schema + "." + table + ";");
            }

            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }

            return result;

        } catch (SQLException e) {
            //e.setStackTrace(stackTrace);
            JOptionPane.showMessageDialog(null, e.getMessage());
            return null;
        }
    }
    
    //FUNCIONES

    public ArrayList listar_funciones(String sql){
        try {
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery(sql);
            int columnCount = 1;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
            
        } catch (SQLException e) {
            //e.setStackTrace(stackTrace);
            JOptionPane.showMessageDialog(null, e.getMessage());
            return null;
        } 
    }
    
    //TRIGGERS
    
    public ArrayList listar_triggers(String sql){
        try {
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery(sql);
            int columnCount = 1;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
        } catch (SQLException e) {
            //e.setStackTrace(stackTrace);
            JOptionPane.showMessageDialog(null, e.getMessage());
            return null;
        } 
    }
    
    //VIEWS
    
    public ArrayList listar_views(String sql){
        try {
            
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery(sql);
            int columnCount = 2;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
            
        } catch (SQLException e) {
            //e.setStackTrace(stackTrace);
            JOptionPane.showMessageDialog(null, e.getMessage());
            return null;
        } 
    }
    
    //INDEXES
    public ArrayList listar_indexes(String sql){
        try {
            ArrayList <String[]> result = new ArrayList<String[]>();
            at = connection.createStatement();
            rt = at.executeQuery(sql);
            int columnCount = 2;
            while (rt.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rt.getString(i + 1);
                }
                result.add(row);
            }
            
            return result;
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, e.getMessage());
            //e.setStackTrace(stackTrace);
            return null;
        } 
    }
   
}