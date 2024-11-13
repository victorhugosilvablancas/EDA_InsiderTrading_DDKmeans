package uaq.dcc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 * Cluster configuration for an Oracle instance.
 */
public class Clusterdata {
    /**
     * Oracle connection driver.
     */
    public static final String Driver="oracle.jdbc.driver.OracleDriver";
    /**
     * URL connection line.
     */
    public static String Url ="jdbc:oracle:thin:@DESARROLLO:1521:XE";
    /**
     * User for the Oracle instance.
     */
    public static final String InstanceUser = "TESISUSR";
    /**
     * Password for the Oracle instance.
     */
    public static final String InstancePwd = "M6aE7heT7";
    
    /**
     * Class that defines Clusterdata.
     */
    public Clusterdata() {
    }
    
    public static String errorStr="";
    public static Integer SQLmax=0;
    public static String[] SQLcabeza=new String[0];
    public static String[][] SQLtabla=new String[0][0];
    public static boolean ComandoSQLframe(String comando,int cols) {
        errorStr="";
        try {
            Class.forName(Clusterdata.Driver);
            //Connection conn = DriverManager.getConnection(Url);
            Connection conn = DriverManager.getConnection(
                    Clusterdata.Url,
                    Clusterdata.InstanceUser,
                    Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(comando);
            int max=0;
            while (rs.next()) {
                max++;
            }
            SQLtabla=new String[max][cols];
            SQLcabeza=new String[cols];
            int j=0;
            rs = stmt.executeQuery(comando);            
            while (rs.next()) {
                for (int i=0;i<cols;i++) {
                    SQLtabla[j][i]=rs.getString(i+1);
                    SQLcabeza[i]=String.valueOf(i+1);
                }                
                j++;
            }
            SQLmax=max;
            rs.close();conn.close();
            return true;
        } catch (Exception e) {
            errorStr=e.getLocalizedMessage();
            return false;
        }
    }
    public static boolean ComandoSQL(String comando) {
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(
                    Clusterdata.Url,
                    Clusterdata.InstanceUser,
                    Clusterdata.InstancePwd);
            Statement s = conn.createStatement();
            s.execute(comando);
            s.close();
            conn.close();
            return true;
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return false;
        }
    }

    
}
