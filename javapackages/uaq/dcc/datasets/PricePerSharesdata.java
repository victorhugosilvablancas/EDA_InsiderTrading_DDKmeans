package uaq.dcc.datasets;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Price share data definition.
 */
public class PricePerSharesdata {
    public Integer idpricepershare;
    public Double transactionPricePerShare;
    public Integer records;
    public String transactionAcquiredDisposedCode;
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_PRICEPERSHARE";
    public static List<PricePerSharesdata> listpricepershares=new ArrayList<>();
    
    /**
     * Class that defines PricePerSharesdata.
     */
    public PricePerSharesdata() {
        idpricepershare=0;
        transactionPricePerShare=0.0;
        records=0;
        transactionAcquiredDisposedCode="";
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return formatted string
     */
    public String string() {
        return idpricepershare+"\t"
                + getTransactionPricePerShare()+"\t"
                + records+"\t"
                + getTransactionAcquiredDisposedCode()
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "idpricepershare\t"
                + "transactionPricePerShare\t"
                + "records\t"
                + "transactionAcquiredDisposedCode"
                ;
    }
    /**
     * 
     * @return table head for LaTeX
     */
    public static String LatexHead() {
        return "\\textbf{Ac.Disp.Code}"
                + "& \\textbf{PricePerShare}"
                + "& \\textbf{Records}"
                + "\\\\\n";
    }
    /**
     * 
     * @return formatted string for latex
     */
    public String latex() {
        return getTransactionAcquiredDisposedCode()
                + " & "+getTransactionPricePerShare()
                + " & "+records
                + "\\\\\n";
    }
    /**
     * 
     * @return transaction price per share
     */
    public String getTransactionPricePerShare() {
        return String.format("%,.2f", transactionPricePerShare);
    }
    /**
     * 
     * @return transaction acquired dispose code
     */
    public String getTransactionAcquiredDisposedCode() {
        if (transactionAcquiredDisposedCode!=null) {
            if (transactionAcquiredDisposedCode.equals("null")) return "";
            else return transactionAcquiredDisposedCode;
        } else return "";
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
        return "insert into "+PricePerSharesdata.TABLE+" ("
                + "idpricepershare,"
                + "transactionPricePerShare,"
                + "records,"
                + "transacAcquiredDisposedCode "
                + ") values ("
                + ""+idpricepershare+","
                + ""+transactionPricePerShare+","
                + ""+records+","
                + "'"+transactionAcquiredDisposedCode+"' "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+PricePerSharesdata.TABLE+" set "
                + "transactionPricePerShare="+transactionPricePerShare+","
                + "records="+records+","
                + "transacAcquiredDisposedCode='"+transactionAcquiredDisposedCode+"' "
                + "where idpricepershare="+idpricepershare+" "
                ;
    }
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+PricePerSharesdata.TABLE+" ("
                + "idpricepershare number(9) primary key,"
                + "transactionPricePerShare number(9,2) default 0,"
                + "records number(16) default 0,"
                + "transacAcquiredDisposedCode varchar2(10) "
                + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+PricePerSharesdata.TABLE+" ";
    
}
