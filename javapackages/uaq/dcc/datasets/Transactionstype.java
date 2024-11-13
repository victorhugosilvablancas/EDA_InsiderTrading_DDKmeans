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
 * Transaction type analysis.
 */
public class Transactionstype {
    public Transactionstypedata data;
    private String filename;
    
    private Tools l=new Tools();
    
    /**
     * Class that defines Transaction.
     */
    public Transactionstype() {
        CheckOutTable();
        data=new Transactionstypedata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        Transactionstypedata.listtransactions=new ArrayList<>();
        Transactionstypedata midato=new Transactionstypedata();
        
        Double rmax=0.0;
        Double rmin=0.0;
        Double dmax=0.0;
        Double dmin=0.0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select * from "+Transactionstypedata.TABLE+" "
                    + "where registros between 99999 and 999999 "
                    + "order by transactionDate "
                    ;
            ResultSet rs=stmt.executeQuery(instruccion);
            int i=1;
            while (rs.next()) {
                midato=new Transactionstypedata();
                midato.idtransaction=i;
                midato.transactionType=rs.getString("transactionType");
                midato.transactionDate=rs.getInt("transactionDate");
                midato.records=rs.getInt("registros");
                
                Transactionstypedata.listtransactions.add(midato);
                i++;
            }
            //normalizing
            instruccion="select "
                    + "max(registros),"
                    + "min(registros),"
                    + "max(transactionDate),"
                    + "min(transactionDate) "
                    + "from "+Transactionstypedata.TABLE+" "
                    + "where registros between 99999 and 999999 "
                    ;
            rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                rmax=rs.getDouble(1);
                rmin=rs.getDouble(2);
                dmax=rs.getDouble(3);
                dmin=rs.getDouble(4);
            }
            //System.out.println(rmax+":"+ rmin+":"+ dmax+":"+ dmin);
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        //normalizing
        if (Transactionstypedata.listtransactions.size()>0) {
            for (int i=0;i<Transactionstypedata.listtransactions.size();i++) {
                midato=Transactionstypedata.listtransactions.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.records.doubleValue());
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.transactionDate.doubleValue());
                Transactionstypedata.listtransactions.set(i,midato);
            }
        }
    }
    
    
    /**
     * Draws list in table format
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] ahead=Transactionstypedata.Head().split(Tools.TAB);
        int r=Transactionstypedata.listtransactions.size();
        String[][] atable=new String[r][ahead.length];
        Transactionstypedata midato=new Transactionstypedata();
        for (int i=0;i<r;i++) {
            midato=Transactionstypedata.listtransactions.get(i);
            atable[i]=midato.string().split(Tools.TAB);
        }
        return atable;
    }
    /**
     * Draws list in LaTeX table format
     * @return formatted string to be paste in LaTeX editor.
     */
    public String DrawTableLaTeX() {
        Transactionstypedata midato=new Transactionstypedata();
        String cade="""
                    \\begin{table}[H]
                    \\caption{transactionTypes for insider trading.\\label{tabla:transactionTypes}}
                    \\begin{tabularx}{\\textwidth}{ r l r r}
                    \\toprule
                    """;
        cade+=Transactionstypedata.LatexHead();
        cade+=Tools.latexMidrule();
        int r=Transactionstypedata.listtransactions.size();
        for (int i=0;i<r;i++) {
            midato=Transactionstypedata.listtransactions.get(i);
            cade+=midato.latex();
            if (i+1 < r)
                cade+=Tools.latexMidrule();
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
     */
    public void getDataset() {
        List<Transactionstypedata> milista=new ArrayList<>();
        Transactionstypedata.listtransactions=new ArrayList<>();
        Transactionstypedata midato=new Transactionstypedata();
        
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
                        midato=new Transactionstypedata();
                        midato.transactionType=rdato.transactionType;
                        midato.setTransactionDate(rdato.transactionDate);
                        if (midato.transactionDate<2024) {
                            milista.add(midato);
                            max++;
                            //if (max % 1000 == 0) System.out.println(max);
                        }
                    }
                    line = br.readLine();
                }
                br.close();
                //System.out.println("Transactions.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<Transactionstypedata>() {
                @Override
                public int compare(Transactionstypedata o1, Transactionstypedata o2) {
                    return o1.getTypeAndDate().compareTo(o2.getTypeAndDate());
                }
            });
            
            Transactionstypedata aux=new Transactionstypedata();
            String xtran="XXX";
            Boolean primera=true;
            Integer isuma=0;
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.getTypeAndDate().equals(xtran)) {
                    if (primera) primera=false;
                    else {
                        aux.records=isuma;
                        Transactionstypedata.listtransactions.add(aux);
                        //System.out.println(aux.string());
                    }
                    isuma=0;
                    xtran=midato.getTypeAndDate();
                    aux=midato;
                }
                isuma++;
            }
            if (isuma>0) Transactionstypedata.listtransactions.add(aux);
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(Transactionstypedata.DROP_TABLE);
            stmt.executeUpdate(Transactionstypedata.CREATE_TABLE);
            
            for (int i=0;i<Transactionstypedata.listtransactions.size();i++) {
                midato=Transactionstypedata.listtransactions.get(i);
                midato.idtransaction=i+1;
                //System.out.println(midato.CreateStr());
                stmt.execute(midato.CreateStr());
            }
            stmt.close();conn.close();
            //System.out.println("Transactionstype.getDataset: Proceso Terminado "+milista.size());
        } catch (Exception e) {
            System.out.println(line);
            System.out.println("getDataset: "+e.getLocalizedMessage());
        }
    }

    /**
     * Calculates total amount from CSV file and stores in Oracle instance
     */
    public void getTotalAmount() {
        Transactionstypedata midato=new Transactionstypedata();
        String line="";
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
                        
                        Transactionstypedata.AddRecordsValue(rdato);
                    }
                    line = br.readLine();
                }
                br.close();
            }
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            for (int i=0;i<Transactionstypedata.listtransactions.size();i++) {
                midato=Transactionstypedata.listtransactions.get(i);
                //System.out.println(midato.SaveStr());
                stmt.execute(midato.SaveStr());
            }
            stmt.close();conn.close();
            System.out.println("getTotalAmount: End");
        } catch (Exception e) {
            System.out.println(line);
            System.out.println("getTotalAmount: "+e.getLocalizedMessage());
        }
    }

    

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void CheckOutTable() {
        if (!l.TableExists(Transactionstypedata.TABLE)) 
            l.SQLcommand(Transactionstypedata.CREATE_TABLE);
    }
    
}
