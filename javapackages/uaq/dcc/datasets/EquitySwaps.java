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
 * Equity Swaps analysis.
 */
public class EquitySwaps {
    /**
     * EquitySwapsdata data type.
     */
    public EquitySwapsdata data;
    /**
     * source CSV file.
     */
    private String filename;
    
    /**
     * Tools instance
     */
    private Tools l=new Tools();
    
    /**
     * Class that defines EquitySwaps.
     */
    public EquitySwaps() {
        ChekOutTable();
        data=new EquitySwapsdata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        EquitySwapsdata.listequityswaps=new ArrayList<>();
        EquitySwapsdata midato=new EquitySwapsdata();
        
        Double rmax=0.0;
        Double rmin=999999999.0;
        Double dmax=0.0;
        Double dmin=999999999.0;
        Double tmax=0.0;
        Double tmin=999999999.0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select * from "+EquitySwapsdata.TABLE+" "
                    + "where equitySwapInvolved>0 "
                    + "order by transactionCode";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new EquitySwapsdata();
                midato.idequity=rs.getInt("idequity");
                midato.transactionCode=rs.getString("transactionCode");
                midato.equitySwapInvolved=rs.getInt("equitySwapInvolved");
                midato.records=rs.getInt("registros");
                
                //rounding because to mainting focus on transactionCode
                //at the moment of normalizing
                if (midato.records>99999) midato.records=100000;
                if (midato.records<5000) midato.records=5000;
                
                EquitySwapsdata.listequityswaps.add(midato);
            }
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        if (EquitySwapsdata.listequityswaps.size()>0) {
            //normalizing
            for (int i=0;i<EquitySwapsdata.listequityswaps.size();i++) {
                midato=EquitySwapsdata.listequityswaps.get(i);
                
                if (rmax<midato.transactioncodefloat()) rmax=midato.transactioncodefloat();
                if (rmin>midato.transactioncodefloat()) rmin=midato.transactioncodefloat();
                if (dmax<midato.equitySwapInvolved.doubleValue()) dmax=midato.equitySwapInvolved.doubleValue();
                if (dmin>midato.equitySwapInvolved.doubleValue()) dmin=midato.equitySwapInvolved.doubleValue();
                if (tmax<midato.records.doubleValue()) tmax=midato.records.doubleValue();
                if (tmin>midato.records.doubleValue()) tmin=midato.records.doubleValue();
            }
            for (int i=0;i<EquitySwapsdata.listequityswaps.size();i++) {
                midato=EquitySwapsdata.listequityswaps.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.transactioncodefloat());
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.equitySwapInvolved.doubleValue());
                midato.cpoint=Tools.Normaliza(dmin,dmax,midato.records.doubleValue());
                EquitySwapsdata.listequityswaps.set(i,midato);
            }
        }
    }
    /**
     * Draws list in table format
     * 
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] ahead=EquitySwapsdata.Head().split(Tools.TAB);
        
        int r=EquitySwapsdata.listequityswaps.size();
        String[][] atable=new String[r][ahead.length];
        
        EquitySwapsdata midato=new EquitySwapsdata();
        
        for (int i=0;i<EquitySwapsdata.listequityswaps.size();i++) {
            midato=EquitySwapsdata.listequityswaps.get(i);
            atable[i]=midato.string().split("\t");
        }
        return atable;
    }
    /**
     * 
     * Draws list in LaTeX table format
     * @return formatted string to be paste in LaTeX editor.
     */
    public String DrawTableLaTeX() {
        String cade="";
        List<EquitySwapsdata> milista=new ArrayList<>();
        EquitySwapsdata midato=new EquitySwapsdata();
        EquitySwapsdata.SumSwaps=0;
        EquitySwapsdata.SumRecords=0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select "
                    + "transactionCode,"
                    + "sum(equitySwapInvolved) as equitySwapInvolved,"
                    + "sum(registros) as registros "
                    + "from "+EquitySwapsdata.TABLE+" "
                    + "group by transactionCode "
                    + "order by transactionCode ";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new EquitySwapsdata();
                midato.transactionCode=rs.getString("transactionCode");
                midato.equitySwapInvolved=rs.getInt("equitySwapInvolved");
                midato.records=rs.getInt("registros");
                
                milista.add(midato);
                
                EquitySwapsdata.SumSwaps+=midato.equitySwapInvolved;
                EquitySwapsdata.SumRecords+=midato.records;
            }
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("DrawTable: "+e.getLocalizedMessage());
        }
        if (milista.size()>0) {
            Double rporshare=0.0;
            Double rporrecord=0.0;

                cade="""
                    \\begin{table}[H]
                    \\caption{Equity Swaps Involved by Transaction Code.\\label{tabla:equitySwapInvolved}}
                    \\begin{tabularx}{\\textwidth}{ l r r r r }
                    \\toprule
                    """;
                cade+=EquitySwapsdata.LatexHead();
                cade+=Tools.latexMidrule();
                int r=milista.size();
                for (int i=0;i<r;i++) {
                    midato=milista.get(i);
                    
                    rporshare=midato.equitySwapInvolved.doubleValue()/EquitySwapsdata.SumSwaps.doubleValue()*100;
                    rporrecord=midato.records.doubleValue()/EquitySwapsdata.SumRecords.doubleValue()*100;
                    
                    cade+=midato.getTransactionCode()
                            + " & "+midato.equitySwapInvolved
                            + " & "+String.format("%.2f",rporshare)+"\\%"
                            + " & "+midato.records
                            + " & "+String.format("%.2f",rporrecord)+"\\%"
                            + "\\\\\n";
                    if (i+1 < r)
                        cade+=Tools.latexMidrule();
                }
                cade+="""
                      \\bottomrule
                      \\end{tabularx}
                      \\noindent{\\footnotesize{Source: Proposed work.}}
                      \\end{table}""";
            
        }
        return cade;
    }
    
    /**
     * Retrieve dinamic list from CSV file
     * @param maxdata
     */
    public void getDataset(int maxdata) {
        List<EquitySwapsdata> milista=new ArrayList<>();
        EquitySwapsdata.listequityswaps=new ArrayList<>();
        EquitySwapsdata midato=new EquitySwapsdata();
        
        String line="";
        int max=0;
        try {
            FileRecordsdata rdato=new FileRecordsdata();
            File f=new File(filename);
            if (f.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                boolean primera=true;
                line = br.readLine();
                while (line != null && max<maxdata) {
                    if (primera) primera=false;
                    else {
                        rdato=new FileRecordsdata(line);
                        
                        midato=new EquitySwapsdata();
                        midato.transactionCode=rdato.transactionCode;
                        midato.equitySwapInvolved=rdato.equitySwapInvolved;
                        //System.out.println(midato.cadena());
                        milista.add(midato);
                    }
                    line = br.readLine();
                    max++;
                    //if (max % 1000 == 0) System.out.println(max);
                }
                br.close();
                //System.out.println("Swaps.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<EquitySwapsdata>() {
                @Override
                public int compare(EquitySwapsdata o1, EquitySwapsdata o2) {
                    return o1.getTransactionCode().compareTo(o2.getTransactionCode());
                }
            });
            
            String xaux="XXX";
            Boolean primera=true;
            EquitySwapsdata auxdato=new EquitySwapsdata();
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.getTransactionCode().equals(xaux)) {
                    if (primera) primera=false;
                    else {
                        //System.out.println(auxdato.cadena());
                        EquitySwapsdata.listequityswaps.add(auxdato);
                    }
                    xaux=midato.getTransactionCode();
                    auxdato=new EquitySwapsdata();
                    auxdato.transactionCode=midato.transactionCode;
                }
                auxdato.equitySwapInvolved+=midato.equitySwapInvolved;
                auxdato.records++;
            }
            if (auxdato.records>0) {
                //System.out.println(auxdato.cadena());
                EquitySwapsdata.listequityswaps.add(auxdato);
            }
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(EquitySwapsdata.DROP_TABLE);
            stmt.executeUpdate(EquitySwapsdata.CREATE_TABLE);
            for (int i=0;i<EquitySwapsdata.listequityswaps.size();i++) {
                midato=EquitySwapsdata.listequityswaps.get(i);
                midato.idequity=i+1;
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
        l.SQLcommand(EquitySwapsdata.CREATE_TABLE);
    }

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void ChekOutTable() {
        if (!l.TableExists(EquitySwapsdata.TABLE)) CreateTable();
    }
    
    
    
}
