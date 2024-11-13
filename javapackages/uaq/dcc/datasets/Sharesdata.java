package uaq.dcc.datasets;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import uaq.dcc.edakmeansanalysis.Tools;

/**
 *
 * Transaction shares data definition.
 */
public class Sharesdata {
    public Integer idshare;
    public Timestamp transactionDate;
    public Integer transactionShares;
    public String transactionSharesFn;
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_SHARES";
    public static List<Sharesdata> listshares=new ArrayList<>();
    
    /**
     * Class that defines Sharesdata.
     */
    public Sharesdata() {
        idshare=0;
        transactionDate=new Timestamp(System.currentTimeMillis());
        transactionShares=0;
        transactionSharesFn="";
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return formatted string for the leff
     */
    public String stringleft() {
        return getTransactionYear()+"\t"
                + transactionShares+"\t"
                + transactionSharesFn
                ;
    }
    /**
     * 
     * @return formatted string for the right
     */
    public String stringright() {
        return getTransactionYear()+"\t"
                + transactionShares+"\t"
                + transactionSharesFn
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "Year\t"
                + "transaction Shares\t"
                + "Fn\t"
                + "Year\t"
                + "transaction Shares\t"
                + "Fn"
                ;
    }
    /**
     * 
     * @return table head for LaTeX
     */
    public static String LatexHead() {
        return "\\textbf{Year}"
                + "& \\textbf{transaction Shares}"
                + "& \\textbf{Fn}"
                + "& \\textbf{Year}"
                + "& \\textbf{transaction Shares}"
                + "& \\textbf{Fn}"
                + "\\\\\n";
    }
    /**
     * 
     * @return formatted string for the left for LaTeX
     */
    public String latexleft() {
        return getTransactionYear()
                + " & "+transactionShares
                + " & "+transactionSharesFn
                ;
    }
    /**
     * 
     * @return formatted string for the right for LaTeX
     */
    public String latexright() {
        return " & "+getTransactionYear()
                + " & "+transactionShares
                + " & "+transactionSharesFn
                + "\\\\\n";
    }
    public void setTransactionSharesFn(String afn) {
        transactionSharesFn="";
        if (afn!=null) {
            if (!afn.equals("null")) {
                transactionSharesFn=afn.replace("\"", "");
            }
        }
    }
    public String getTransactionSharesFn() {
        if (transactionSharesFn!=null) {
            if (!transactionSharesFn.equals("null")) {
                return transactionSharesFn;
            } else return "";
        } else return "";
    }
    /**
     * 
     * @return transaction date
     */
    public String getTransactionDate() {
        return transactionDate.toString().substring(0, 10);
    }
    /**
     * 
     * @return transaction year
     */
    public Integer getTransactionYear() {
        return Integer.valueOf(transactionDate.toString().substring(0, 4));
    }
    /**
     * 
     * @return transaction year and fn
     */
    public String getTransactionYearFn() {
        return getTransactionYear()+transactionSharesFn;
    }
    
    /**
     * 
     * @return apoint normalized
     */
    public String getApoint() {
        return String.format("%.3f",apoint);
    }
    /**
     * 
     * @return plotting position
     */
    public Double getApointgraphic() {
        return apoint*400;
    }
    /**
     * 
     * @return bpoint normalized
     */
    public String getBpoint() {
        return String.format("%.3f",bpoint);
    }
    /**
     * 
     * @return plotting position
     */
    public Double getBpointgraphic() {
        return bpoint*400;
    }
    /**
     * 
     * @return cpoint normalized
     */
    public String getCpoint() {
        return String.format("%.3f",cpoint);
    }
    /**
     * 
     * @return plotting position
     */
    public Double getCpointgraphic() {
        return cpoint*400;
    }
    /**
     * insert record string for Oracle instance
     * 
     * @return 
     */
    public String CreateStr() {
        return "insert into "+Sharesdata.TABLE+" ("
                + "idshare,"
                + "transactionDate,"
                + "transactionShares,"
                + "transactionSharesFn "
                + ") values ("
                + ""+idshare+","
                + "to_timestamp('"+transactionDate.toString()+"', 'YYYY-MM-DD HH24:MI:SS.FF'),"
                + ""+transactionShares+","
                + "'"+transactionSharesFn+"' "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+Sharesdata.TABLE+" set "
                + "transactionShares="+transactionShares+","
                + "transactionSharesFn='"+transactionSharesFn+"' "
                + "where idshare="+idshare+" "
                ;
    }
    
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+Sharesdata.TABLE+" ("
                + "idshare number(9) primary key,"
                + "transactionDate Timestamp(6) default current_timestamp,"
                + "transactionShares number(12) default 0,"
                + "transactionSharesFn varchar2(80) "
                + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+Sharesdata.TABLE+" ";
}
