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
 * Price share analysis.
 */
public class PricePerShares {
    public PricePerSharesdata data;
    private String filename;
    
    private Tools l=new Tools();
    
    /**
     * Class that defines PricePerShares.
     */
    public PricePerShares() {
        CheckOutTable();
        data=new PricePerSharesdata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        PricePerSharesdata.listpricepershares=new ArrayList<>();
        PricePerSharesdata midato=new PricePerSharesdata();
        
        Double rmax=0.0;
        Double rmin=999999999.0;
        Double dmax=0.0;
        Double dmin=3000.0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select * from "+PricePerSharesdata.TABLE+" ";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new PricePerSharesdata();
                midato.idpricepershare=rs.getInt("idpricepershare");
                midato.transactionPricePerShare=rs.getDouble("transactionPricePerShare");
                midato.records=rs.getInt("records");
                midato.transactionAcquiredDisposedCode=rs.getString("transacAcquiredDisposedCode");
                
                PricePerSharesdata.listpricepershares.add(midato);
            }
            //System.out.println(Sharesdata.listshares.size());
            
            //normalizing
            instruccion="select "
                    + "transactionPricePerShare,"
                    + "records "
                    + "from "+PricePerSharesdata.TABLE+" "
                    //+ "where transactionShares between 1000000 and 10000000 "
                    + "order by transactionPricePerShare "
                    ;
            rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new PricePerSharesdata();
                midato.transactionPricePerShare=rs.getDouble(1);
                midato.records=rs.getInt(2);
                
                if (rmax<midato.transactionPricePerShare) rmax=midato.transactionPricePerShare;
                if (rmin>midato.transactionPricePerShare) rmin=midato.transactionPricePerShare;
                if (dmax<midato.records) dmax=midato.records.doubleValue();
                if (dmin>midato.records) dmin=midato.records.doubleValue();
            }
            //System.out.println(String.format("%,.0f:%,.0f:%.0f:%.0f",rmax,rmin,dmax,dmin));
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        //normalizing
        if (PricePerSharesdata.listpricepershares.size()>0) {
            for (int i=0;i<PricePerSharesdata.listpricepershares.size();i++) {
                midato=PricePerSharesdata.listpricepershares.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.transactionPricePerShare);
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.records.doubleValue());
                PricePerSharesdata.listpricepershares.set(i,midato);
            }
        }
    }
    /**
     * Draws list in table format
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] ahead=PricePerSharesdata.Head().split(Tools.TAB);
        int r=PricePerSharesdata.listpricepershares.size();
        String[][] atable=new String[r][ahead.length]; 
        PricePerSharesdata midato=new PricePerSharesdata();
        for (int i=0;i<r;i++) {
            midato=PricePerSharesdata.listpricepershares.get(i);
            atable[i]=midato.string().split(Tools.TAB);
        }
        return atable;
    }
    /**
     * Draws list in LaTeX table format
     * @return formatted string to be paste in LaTeX editor.
     */
    public String DrawTableLaTeX() {
        PricePerSharesdata midato=new PricePerSharesdata();
        String cade="""
                    \\begin{table}[H]
                    \\caption{Price per Share on Acquired Dispose Code.\\label{tabla:pricePerShares}}
                    \\begin{tabularx}{\\textwidth}{ l r r r }
                    \\toprule
                    """;
        cade+=PricePerSharesdata.LatexHead();
        cade+=Tools.latexMidrule();
        int r=PricePerSharesdata.listpricepershares.size();
        for (int i=0;i<r;i++) {
            midato=PricePerSharesdata.listpricepershares.get(i);
            if (true) {
            cade+=midato.latex();
                if (i+1 < r)
                    cade+=Tools.latexMidrule();
            }
        }
        cade+="""
              \\bottomrule
              \\end{tabularx}
              \\noindent{\\footnotesize{Source: Proposed work.}}
              \\end{table}""";
        return cade;
    }
    
    /**
     * Retrieve dinamic list from CSV file
     * @param maxdata
     */
    public void getDataset(int maxdata) {
        List<PricePerSharesdata> milista=new ArrayList<>();
        PricePerSharesdata.listpricepershares=new ArrayList<>();
        PricePerSharesdata midato=new PricePerSharesdata();
        
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
                        
                        midato=new PricePerSharesdata();
                        midato.transactionPricePerShare=rdato.transactionPricePerShare;
                        midato.transactionAcquiredDisposedCode=rdato.transactionAcquiredDisposedCode;
                        if (midato.transactionPricePerShare>100000 && midato.transactionPricePerShare<1000000) { 
                            milista.add(midato);
                            max++;
                            //if (max % 1000 == 0) 
                            //System.out.println(max+"\t"+midato.getTransactionAcquiredDisposedCode()+"\t"+midato.transactionPricePerShare);
                        }
                    }
                    line = br.readLine();
                }
                br.close();
                //System.out.println("Price.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<PricePerSharesdata>() {
                @Override
                public int compare(PricePerSharesdata o1, PricePerSharesdata o2) {
                    return o1.transactionPricePerShare.compareTo(o2.transactionPricePerShare);
                }
            });
            
            PricePerSharesdata aux=new PricePerSharesdata();
            Boolean primera=true;
            PricePerSharesdata auxdato=new PricePerSharesdata();
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.transactionPricePerShare.equals(aux.transactionPricePerShare)) {
                    if (primera) primera=false;
                    else {
                        PricePerSharesdata.listpricepershares.add(aux);
                    }
                    aux=midato;
                }
                aux.records++;
            }
            PricePerSharesdata.listpricepershares.add(auxdato);
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(PricePerSharesdata.DROP_TABLE);
            stmt.executeUpdate(PricePerSharesdata.CREATE_TABLE);
            for (int i=0;i<PricePerSharesdata.listpricepershares.size();i++) {
                midato=PricePerSharesdata.listpricepershares.get(i);
                midato.idpricepershare=i+1;
                //System.out.println(midato.CreateStr());
                stmt.execute(midato.CreateStr());
            }
            //System.out.println("getDataset: End ("+PricePerSharesdata.listpricepershares.size()+")");
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println(line);
            System.out.println("getDataset: "+e.getLocalizedMessage());
        }
    }
    
    /**
     * Create datatype table in Oracle instance
     */
    private void CreateTabla() {
        l.SQLcommand(PricePerSharesdata.CREATE_TABLE);
    }

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void CheckOutTable() {
        if (!l.TableExists(PricePerSharesdata.TABLE)) CreateTabla();
    }
    
}
