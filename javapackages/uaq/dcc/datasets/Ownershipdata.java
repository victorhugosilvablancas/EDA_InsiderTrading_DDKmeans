package uaq.dcc.datasets;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Ownershipt data definition.
 */
public class Ownershipdata {
    public Integer idowner;
    public String directOrIndirectOwnership;
    public Long transactionShares;
    public Integer exerciseDate;
    public Integer records;
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_OWNERSHIP";
    public static List<Ownershipdata> listownerships=new ArrayList<>();
    public static Long SumShares=Long.valueOf(0);
    public static Integer SumRecords=0;
        
    /**
     * Class that defines Ownershipdata.
     */
    public Ownershipdata() {
        idowner=0;
        directOrIndirectOwnership="";
        transactionShares=Long.valueOf(0);
        exerciseDate=0;
        records=0;
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return D if direct ownershipe, I otherwise
     */
    public String getDirectOrIndirectOwnership() {
        if (directOrIndirectOwnership!=null) {
            if (directOrIndirectOwnership.equals("null")) return "";
            else {
                String cade=directOrIndirectOwnership;
                if (directOrIndirectOwnership.equals("D")) cade="Direct";
                else if (directOrIndirectOwnership.equals("I")) cade="Indirect";
                return cade;
            }
        } else return "";
    }
    public String getDIOandDate() {
        switch (directOrIndirectOwnership) {
            case "D":
                return "D"+exerciseDate;
            case "I":
                return "I"+exerciseDate;
            default:
                return "";
        }
    }
    /**
     * 
     * @param adate the date to be assigned
     */
    public void setExerciseDate(Timestamp adate) {
        exerciseDate=Integer.valueOf(adate.toString().substring(0, 4));
    }
    /**
     * 
     * @return exercise data
     */
    public String getExerciseDate() {
        return exerciseDate+"";
    }
    /**
     * 
     * @return formatted string
     */
    public String string() {
        return getDirectOrIndirectOwnership()+"\t"
                + transactionShares+"\t"
                + records+"\t"
                + exerciseDate
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "D|I Ownership\t"
                + "Transaction Shares\t"
                + "Records\t"
                + "Exercise Date"
                ;
    }
    /**
     * 
     * @return table head for LaTeX
     */
    public static String LatexHead() {
        return """
               \\textbf{Ownership} & \\textbf{Transaction Shares(abs)} & \\textbf{\\%} & \\textbf{Records}& \\textbf{\\%} & \\textbf{exerciseDate} \\\\
               """;
    }
    /**
     * 
     * @return formatted string for LaTeX
     */
    public String latex() {
        return getDirectOrIndirectOwnership()
                + " & "+transactionShares
                + " & "+transactionShares/SumShares
                + " & "+records
                + " & "+records/SumRecords
                + " & "+exerciseDate
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
        return "insert into "+Ownershipdata.TABLE+" ("
                + "idowner,"
                + "directOrIndirectOwnership,"
                + "transactionShares,"
                + "exerciseDate,"
                + "registros "
                + ") values ("
                + ""+idowner+","
                + "'"+directOrIndirectOwnership+"',"
                + ""+transactionShares+","
                + ""+exerciseDate+","
                + ""+records+" "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+Ownershipdata.TABLE+" set "
                + "transactionShares="+transactionShares+","
                + "exerciseDate="+exerciseDate+","
                + "registros="+records+" "
                + "where idowner="+idowner+" "
                ;
    }
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+Ownershipdata.TABLE+" ("
                + "idowner number(6) primary key,"
                + "directOrIndirectOwnership varchar2(10),"
                + "transactionShares number(12) default 0,"
                + "exerciseDate number(4) default 0,"
                + "registros  number(12) default 0 "
                + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+Ownershipdata.TABLE+" ";
    
}
