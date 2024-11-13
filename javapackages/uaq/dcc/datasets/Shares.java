package uaq.dcc.datasets;

import uaq.dcc.oracle.Clusterdata;
import uaq.dcc.edakmeansanalysis.Tools;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *
 * Transaction shares analysis.
 */
public class Shares {
    public Sharesdata data;
    private String filename;
    
    private Tools l=new Tools();
    
    /**
     * Class that defines Shares.
     */
    public Shares() {
        CheckOutTable();
        data=new Sharesdata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        Sharesdata.listshares=new ArrayList<>();
        Sharesdata midato=new Sharesdata();
        
        Double rmax=0.0;
        Double rmin=999999999.0;
        Double dmax=0.0;
        Double dmin=3000.0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select * from "+Sharesdata.TABLE+" "
                    + "where transactionShares between 1000000 and 10000000 "
                    ;
            //System.out.println(instruccion);
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new Sharesdata();
                midato.idshare=rs.getInt("idshare");
                midato.transactionShares=rs.getInt("transactionShares");
                midato.transactionDate=rs.getTimestamp("transactionDate");
                midato.transactionSharesFn=rs.getString("transactionSharesFn");
                Sharesdata.listshares.add(midato);
            }
            //System.out.println(Sharesdata.listshares.size());
            
            //normalizing
            instruccion="select "
                    + "transactionShares,"
                    + "transactionDate "
                    + "from "+Sharesdata.TABLE+" "
                    + "where transactionShares between 1000000 and 10000000 "
                    + "order by transactionShares "
                    ;
            rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new Sharesdata();
                midato.transactionShares=rs.getInt(1);
                midato.transactionDate=rs.getTimestamp(2);
                
                if (rmax<midato.transactionShares) rmax=midato.transactionShares.doubleValue();
                if (rmin>midato.transactionShares) rmin=midato.transactionShares.doubleValue();
                if (dmax<midato.getTransactionYear()) dmax=midato.getTransactionYear().doubleValue();
                if (dmin>midato.getTransactionYear()) dmin=midato.getTransactionYear().doubleValue();
            }
            //System.out.println(String.format("%,.0f:%,.0f:%.0f:%.0f",rmax,rmin,dmax,dmin));
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        //normalizing
        if (Sharesdata.listshares.size()>0) {
            for (int i=0;i<Sharesdata.listshares.size();i++) {
                midato=Sharesdata.listshares.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.transactionShares.doubleValue());
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.getTransactionYear().doubleValue());
                Sharesdata.listshares.set(i,midato);
            }
        }
    }
    /**
     * Draws list in table format
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] head=Sharesdata.Head().split(Tools.TAB);
        int r=Sharesdata.listshares.size();
        int k=r/2;
        String[][] atable=new String[k+1][head.length];
        Sharesdata midato=new Sharesdata();
        String cade="";
        k=0;
        for (int i=0;i<r;i++) {
            midato=Sharesdata.listshares.get(i);
            if (i % 2 == 0) {
                cade+=midato.stringright();
                atable[k]=cade.split(Tools.TAB);
                cade="";
                k++;
            }
            else {
                cade=midato.stringleft()+Tools.TAB;
            }
            
        }
        return atable;
    }
    /**
     * Draws list in LaTeX table format
     * @return formatted string to be paste in LaTeX editor.
     */
    public String DrawTableLaTeX() {
        Sharesdata midato=new Sharesdata();
        String cade="""
                    \\begin{table}[H]
                    \\caption{transactionShares per Year for insider trading*.\\label{tabla:transactionShares}}
                    \\begin{tabularx}{\\textwidth}{ l r l r}
                    \\toprule
                    """;
        cade+=Sharesdata.LatexHead();
        cade+=Tools.latexMidrule();
        int r=Sharesdata.listshares.size();
        for (int i=0;i<r;i++) {
            midato=Sharesdata.listshares.get(i);
            if (midato.getTransactionYear()>=2000) {
                if (i % 2 == 0) {
                    cade+=midato.latexright();
                    //cade+=Librerias.latexMidrule();
                } else {
                    cade+=midato.latexleft();
                }
            }
        }
        cade+="""
              \\bottomrule
              \\end{tabularx}
              \\noindent{\\footnotesize{Source: Proposed work. *Data since 2000}}
              \\end{table}""";
        return cade;
    }
    
    /**
     * Retrieve dinamic list from CSV file
     */
    public void getDataset() {
        List<Sharesdata> milista=new ArrayList<>();
        Sharesdata.listshares=new ArrayList<>();
        Sharesdata midato=new Sharesdata();
        
        String line="";
        int max=0;
        try {
            FileRecordsdata rdato=new FileRecordsdata();
            File f=new File(filename);
            if (f.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                boolean primera=true;
                line = br.readLine();
                while (line != null) {
                    if (primera) primera=false;
                    else {
                        rdato=new FileRecordsdata(line);
                        midato=new Sharesdata();
                        midato.transactionDate=rdato.transactionDate;
                        midato.transactionShares=rdato.transactionShares;
                        midato.setTransactionSharesFn(rdato.transactionSharesFn);
                        if (midato.getTransactionSharesFn().length()>0 && midato.transactionShares>0) {
                            if (midato.getTransactionYear()>2000 && midato.getTransactionYear()<2024) {
                                milista.add(midato);
                                max++;
                                //if (max % 1000 == 0) System.out.println(max);
                            }
                        }
                    }
                    line = br.readLine();
                }
                br.close();
                //System.out.println("Shares.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<Sharesdata>() {
                @Override
                public int compare(Sharesdata o1, Sharesdata o2) {
                    return o1.getTransactionYearFn().compareTo(o2.getTransactionYearFn());
                }
            });
            
            String xtran="1000";
            Boolean primera=true;
            Sharesdata auxdato=new Sharesdata();
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.getTransactionYearFn().equals(xtran)) {
                    if (primera) primera=false;
                    else {
                        Sharesdata.listshares.add(auxdato);
                    }
                    auxdato=new Sharesdata();
                    auxdato.transactionDate=midato.transactionDate;
                    auxdato.transactionSharesFn=midato.transactionSharesFn;
                    xtran=midato.getTransactionYearFn();
                }
                auxdato.transactionShares+=midato.transactionShares;
            }
            Sharesdata.listshares.add(auxdato);
            //System.out.println(Sharesdata.listshares.size());
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(Sharesdata.DROP_TABLE);
            stmt.executeUpdate(Sharesdata.CREATE_TABLE);
            for (int i=0;i<Sharesdata.listshares.size();i++) {
                midato=Sharesdata.listshares.get(i);
                midato.idshare=i+1;
                //System.out.println(midato.CreateStr());
                stmt.execute(midato.CreateStr());
            }
            stmt.close();conn.close();
            //System.out.println("getDataset: End");
        } catch (Exception e) {
            System.out.println(line);
            System.out.println("getDataset: "+e.getLocalizedMessage());
        }
    }
    
    /**
     * Create datatype table in Oracle instance
     */
    private void CreateTable() {
        l.SQLcommand("create table "+Sharesdata.TABLE+" ("
                + "idshare number(9) primary key,"
                + "transactionDate Timestamp(6) default current_timestamp,"
                + "transactionShares number(12) default 0,"
                + "transactionSharesFn varchar2(50) "
                + ")");
    }

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void CheckOutTable() {
        if (!l.TableExists(Sharesdata.TABLE)) CreateTable();
    }
    
}
