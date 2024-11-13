package uaq.dcc.datasets;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Titles data definition.
 */
public class Titlesdata {
    public Integer idtitle;
    public Integer type;
    public String securityTitle;
    public Integer transactionShares;
    public Integer underlyingSecurityShares;
    
    public Long longshares;
    public Long longsecurity;
    
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_TITLES";
    public static List<Titlesdata> listtitles=new ArrayList<>();
    
    /**
     * Class that defines Titlesdata.
     */
    public Titlesdata() {
        idtitle=0;
        type=TITLE_COMPLEMENTARIES;
        securityTitle="";
        transactionShares=0;
        underlyingSecurityShares=0;
        
        longshares=Long.valueOf(0);
        longsecurity=Long.valueOf(0);
        
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return security title
     */
    public String getSecurityTitle() {
        if (securityTitle!=null) {
            if (securityTitle.equals("null")) return "";
            else {
                securityTitle=securityTitle.replace("'", "=");
                return securityTitle;
            }
        } else return "";
    }
    /**
     * 
     * @return security type
     */
    public String getType() {
        return titleslist[type];
    }
    /**
     * 
     * @return longshares + longsecurity
     */
    public Long totaltitles() {
        return longshares+longsecurity;
    }
    /**
     * 
     * @return float value for longshares
     */
    public Double floatlongshares() {
        return Double.valueOf(longshares)/1000000;
    }
    /**
     * 
     * @return float value for longsecurity
     */
    public Double floatlongsecurity() {
        return Double.valueOf(longsecurity)/1000000;
    }
    /**
     * 
     * @return float value for totaltitles
     */
    public Double floattotaltitles() {
        return Double.valueOf(longshares+longsecurity)/1000000;
    }
    /**
     * 
     * @return formatted string
     */
    public String string() {
        return getType()+"\t"
                + String.format("%,.1f", floatlongshares())+"\t"
                + String.format("%,.1f", floatlongsecurity())+"\t"
                + String.format("%,.1f", floattotaltitles())
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "titleType\t"
                + "transactionShares\t"
                + "underlyingSecurityShares\t"
                + "totalTitles"
                ;
    }
    /**
     * 
     * @return table head for LaTeX
     */
    public static String LatexHead() {
        return """
               \\textbf{titleType} & \\textbf{transactionShares}& \\textbf{underlyingSecurityShares} & \\textbf{totalTitles} \\\\
               """;
    }
    /**
     * 
     * @return formatted string for latex
     */
    public String latex() {
        return getType()
                + " & "+String.format("%,.1f", floatlongshares())
                + " & "+String.format("%,.1f", floatlongsecurity())
                + " & "+String.format("%,.1f", floattotaltitles())
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
        return "insert into "+Titlesdata.TABLE+" ("
                + "idtitle,"
                + "tipo,"
                + "securityTitle,"
                + "transactionShares,"
                + "underlyingSecurityShares "
                + ") values ("
                + ""+idtitle+","
                + ""+type+","
                + "'"+securityTitle+"',"
                + ""+transactionShares+","
                + ""+underlyingSecurityShares+" "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+Titlesdata.TABLE+" set "
                + "transactionShares="+transactionShares+","
                + "underlyingSecurityShares="+underlyingSecurityShares+" "
                + "where idtitle="+idtitle+" "
                ;
    }
    
    public static void AddRecordsValue(FileRecordsdata rdata) {
        Titlesdata midato=new Titlesdata();
        for (int i=0;i<Titlesdata.listtitles.size();i++) {
            midato=Titlesdata.listtitles.get(i);
            if (midato.getSecurityTitle().equals(rdata.securityTitle)) {
                midato.transactionShares++;
//                midato.underlyingSecurityShares+=rdata.transactionTotalValue;
                Titlesdata.listtitles.set(i, midato);
            }
        }
    }
    
    public static final int TITLE_COMMON_STOCK  =0;
    public static final int TITLE_WARRANT       =1;
    public static final int TITLE_SHARES        =2;
    public static final int TITLE_RENTS         =3;
    public static final int TITLE_INCENTIVE     =4;
    public static final int TITLE_PETROLEUM     =5;
    public static final int TITLE_STOCK_OPTION  =6;
    public static final int TITLE_INTEREST      =7;
    public static final int TITLE_SALARY        =8;
    public static final int TITLE_INVESTMENT    =9;
    public static final int TITLE_CALL_OPTION   =10;
    public static final int TITLE_HOLDINGS      =11;
    public static final int TITLE_PURCHASE      =12;
    public static final int TITLE_SELL          =13;
    public static final int TITLE_BUY           =14;
    public static final int TITLE_ACQUIRE       =15;
    public static final int TITLE_CANCELLATION  =16;
    public static final int TITLE_CONVERTIBLE   =17;
    public static final int TITLE_RESTRICTED    =18;
    public static final int TITLE_COMPLEMENTARIES=19;
    public static String[] titleslist=new String[] {
        "COMMON STOCK",
        "WARRANT",
        "SHARES",
        "RENTS",
        "INCENTIVE",
        "PETROLEUM",
        "STOCK OPTION",
        "INTEREST",
        "SALARY",
        "INVESTMENT",
        "CALL OPTION",
        "HOLDINGS",
        "PURCHASE",
        "SELL",
        "BUY",
        "ACQUIRE",
        "CANCELLATION",
        "CONVERTIBLE",
        "RESTRICTED",
        "COMPLEMENTARIES",
    };
    /**
     * retrieve title type by title name
     * 
     * @param atitulo title name
     * @return title type
     */
    public static Integer getType(String atitulo) {
        Integer itipo=TITLE_COMPLEMENTARIES;
        for (int i=0;i<titleslist.length;i++) {
            if (atitulo.contains(titleslist[i])) {
                itipo=i;
                break;
            }
        }
        return itipo;
    }
    
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+Titlesdata.TABLE+" ("
                + "idtitle number(6) primary key,"
                + "tipo number(3) default 19,"
                + "securityTitle varchar2(80) unique,"
                + "transactionShares number(12) default 0,"
                + "underlyingSecurityShares number(12) default 0 "
                + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+Titlesdata.TABLE+" ";
    
}
