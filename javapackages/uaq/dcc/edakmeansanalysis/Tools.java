package uaq.dcc.edakmeansanalysis;

import uaq.dcc.oracle.Clusterdata;
import java.sql.*;

/**
 *
 * Miscelaneus tools.
 */
public class Tools {
    public static final String ABOUT_TEXT="""
                                          EdaKmeansAnalysis.jar
                                          This is a research work for the
                                          Doctorate in Data Science Program, of the
                                          Informatics Faculty, from the
                                          Queretaro Autonomous University
                                          https://www.uaq.mx/
                                          2024. Mexico.
                                          """;
    
    /**
     * TAB character
     */
    public static final String TAB="\t";
    /**
     * Class that provides tools
     */
    public Tools() {
    }

    /**
     * executes SQL command on Oracle instance
     * 
     * @param command
     * @return true if command is executed.
     */
    public boolean SQLcommand(String command) {
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.execute(command);
            stmt.close();conn.close();
            return true;
        } catch (Exception e) {
            System.out.println("ComandoSQL: "+e.getLocalizedMessage());
            return false;
        }
    }
    /**
     * executes SQL command on Oracle instance without notifing error
     * 
     * @param command
     * @return true if command is executed.
     */
    public boolean SQLcommandNoError(String command) {
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.execute(command);
            stmt.close();conn.close();
            return true;
        } catch (Exception e) {
            //System.out.println("ComandoSQLsinError: "+e.getLocalizedMessage());
            return false;
        }
    }
    /**
     * look out for a table in Oracle instance
     * 
     * @param atable the table name to look for
     * @return true if the table is found.
     */
    public boolean TableExists(String atable) {
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.execute("select count(*) from "+atable);
            stmt.close();conn.close();
            return true;
        } catch (Exception e) {
            //System.out.println("Existe: "+e.getLocalizedMessage());
            return false;
        }
    }
    
    /**
     * 
     * @return midrule tag for LaTeX
     */
    public static String latexMidrule() {
        return "\\midrule\n";
    }
    /**
     * 
     * @param arow data in string format
     * @return only numeric values (0..9,-,.)
     */
    public static Double OnlyNumbers(String arow) {
        int r=arow.length();
        String letra="";
        for (int i=0;i<r;i++) {
            switch (arow.substring(i, i+1)) {
                case "0":
                    letra+=arow.substring(i, i+1);
                    break;
                case "1":
                    letra+=arow.substring(i, i+1);
                    break;
                case "2":
                    letra+=arow.substring(i, i+1);
                    break;
                case "3":
                    letra+=arow.substring(i, i+1);
                    break;
                case "4":
                    letra+=arow.substring(i, i+1);
                    break;
                case "5":
                    letra+=arow.substring(i, i+1);
                    break;
                case "6":
                    letra+=arow.substring(i, i+1);
                    break;
                case "7":
                    letra+=arow.substring(i, i+1);
                    break;
                case "8":
                    letra+=arow.substring(i, i+1);
                    break;
                case "9":
                    letra+=arow.substring(i, i+1);
                    break;
                case ".":
                    letra+=arow.substring(i, i+1);
                    break;
                case "-":
                    letra+=arow.substring(i, i+1);
                    break;
                default:
                    break;
            }
        }
        if (letra.length()>0) return Double.valueOf(letra);
        else return 0.0;
    }
    
    /**
     * return normalized value according to:
     * x = (x-xmin)/(xmax-xmin)
     * 
     * @param xmin min value.
     * @param xmax max value.
     * @param adata data to be normalized.
     * @return 
     */
    public static Double Normaliza(Double xmin, Double xmax, Double adata) {
        Double xdivisor=xmax-xmin;
        if (xdivisor!=0) return (adata-xmin)/(xmax-xmin);
        else return 0.0;
    }

 
}