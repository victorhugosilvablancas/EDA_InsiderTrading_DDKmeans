package uaq.dcc.datasets;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Equity Swaps data definition.
 */
public class EquitySwapsdata {
    public Integer idequity;
    public String transactionCode;
    public Integer equitySwapInvolved;
    public Integer records;
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_EQUITYSWAPS";
    public static List<EquitySwapsdata> listequityswaps=new ArrayList<>();
    public static Integer SumSwaps=0;
    public static Integer SumRecords=0;
    
    /**
     * Class that defines EquitySwapsdata.
     */
    public EquitySwapsdata() {
        idequity=0;
        transactionCode="";
        equitySwapInvolved=0;
        records=0;
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return transaction code
     */
    public String getTransactionCode() {
        if (transactionCode!=null) {
            if (transactionCode.equals("null")) return "";
            else return transactionCode;
        } else return "";
    }
    public Double transactioncodefloat() {
        char c=getTransactionCode().charAt(0);
        Integer ivalue=(int) c;
        return ivalue.doubleValue();
    }
    /**
     * 
     * @return formatted string
     */
    public String string() {
        return getTransactionCode()+"\t"
                + equitySwapInvolved+"\t"
                + records+"\t"
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "Transaction Code\t"
                + "Equity Swaps\t"
                + "Records\t"
                ;
    }
    /**
     * 
     * @return table head for LaTex
     */
    public static String LatexHead() {
        return """
               \\textbf{Transaction Code} & \\textbf{Equity Swaps} & \\textbf{\\%} & \\textbf{Records}& \\textbf{\\%} \\\\
               """;
    }
    /**
     * 
     * @return formatted string for LaTeX
     */
    public String latex() {
        return getTransactionCode()
                + " & "+equitySwapInvolved
                + " & "+records
                + "\\\\\n";
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
        return "insert into "+EquitySwapsdata.TABLE+" ("
                + "idequity,"
                + "transactionCode,"
                + "equitySwapInvolved,"
                + "registros "
                + ") values ("
                + ""+idequity+","
                + "'"+transactionCode+"',"
                + ""+equitySwapInvolved+","
                + ""+records+" "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+EquitySwapsdata.TABLE+" set "
                + "equitySwapInvolved="+equitySwapInvolved+","
                + "registros="+records+" "
                + "where idequity="+idequity+" "
                ;
    }
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+EquitySwapsdata.TABLE+" ("
                + "idequity number(6) primary key,"
                + "transactionCode varchar2(10) unique,"
                + "equitySwapInvolved number(6) default 0,"
                + "registros  number(12) default 0 "
                + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+EquitySwapsdata.TABLE+" ";
    
}
