package uaq.dcc.datasets;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Transaction type data definition.
 */
public class Transactionstypedata {
    public Integer idtransaction;
    public String transactionType;
    public Integer transactionDate;
    public Integer records;
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_TRANSACTIONS";
    public static List<Transactionstypedata> listtransactions=new ArrayList<>();
    
    /**
     * Class that defines Transactiondata.
     */
    public Transactionstypedata() {
        idtransaction=0;
        transactionType="";
        transactionDate=0;
        records=0;
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return formatted string
     */
    public String string() {
        return idtransaction+"\t"
                + getTransactionType()+"\t"
                + getTransactionDate()+"\t"
                + records+"\t"
                + getApoint()+"\t"
                + getBpoint()
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "idtransaction\t"
                + "transactionType\t"
                + "transactionDate\t"
                + "transactions\t"
                + "normalized records\t"
                + "normalized dates"
                ;
    }
    /**
     * 
     * @return table head for LaTeX
     */
    public static String LatexHead() {
        return "\\textbf{idtransaction} "
                + "& \\textbf{transactionType}"
                + "& \\textbf{registros}"
                + "\\\\\n";
    }
    /**
     * 
     * @return formatted string for LaTeX
     */
    public String latex() {
        return idtransaction
                + " & "+getTransactionType()
                + " & "+records
                + "\\\\\n";
    }
    /**
     * 
     * @return transaction type
     */
    public String getTransactionType() {
        if (transactionType!=null) {
            if (transactionType.equals("null")) return "";
            else return transactionType;
        } else return "";
    }
    /**
     * Set the transaction date.
     * 
     * @param date the new date.
     */
    public void setTransactionDate(Timestamp date) {
        transactionDate=Integer.valueOf(date.toString().substring(0, 4));
    }
    /**
     * 
     * @return transaction date
     */
    public String getTransactionDate() {
        return transactionDate+"";
    }
    /**
     * 
     * @return transaction type and date
     */
    public String getTypeAndDate() {
        return getTransactionType()+getTransactionDate();
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
        return "insert into "+Transactionstypedata.TABLE+" ("
                + "idtransaction,"
                + "transactionType,"
                + "transactionDate,"
                + "registros "
                + ") values ("
                + ""+idtransaction+","
                + "'"+transactionType+"',"
                + ""+transactionDate+","
                + ""+records+" "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+Transactionstypedata.TABLE+" set "
                + "registros="+records+" "
                + "where idtransaction="+idtransaction+" "
                ;
    }
    /**
     * Add to the total amount the transaction value.
     * 
     * @param rdata the FileRecordsdata
     */
    public static void AddRecordsValue(FileRecordsdata rdata) {
        Transactionstypedata midato=new Transactionstypedata();
        for (int i=0;i<Transactionstypedata.listtransactions.size();i++) {
            midato=Transactionstypedata.listtransactions.get(i);
            if (midato.getTransactionType().equals(rdata.transactionType)) {
                midato.records++;
                Transactionstypedata.listtransactions.set(i, midato);
            }
        }
    }
    
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+Transactionstypedata.TABLE+" ("
            + "idtransaction number(6) primary key,"
            + "transactionType varchar2(50),"
            + "transactionDate number(4) default 0,"
            + "registros number(9) default 0 "
            + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+Transactionstypedata.TABLE+" ";
    
}
