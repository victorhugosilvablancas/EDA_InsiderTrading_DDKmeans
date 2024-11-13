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
 * Titles analysis.
 */
public class Titles {
    public Titlesdata data;
    private String filename;
    
    private Tools l=new Tools();
    
    /**
     * Class that defines Titles.
     */
    public Titles() {
        CheckOutTable();
        data=new Titlesdata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        Titlesdata.listtitles=new ArrayList<>();
        Titlesdata midato=new Titlesdata();
        
        Double rmax=0.0;
        Double rmin=999999999.0;
        Double dmax=0.0;
        Double dmin=3000.0;
        Double tmax=0.0;
        Double tmin=3000.0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select "
                    + "tipo,"
                    + "sum(transactionShares) as transactionShares,"
                    + "sum(underlyingSecurityShares) as underlyingSecurityShares "
                    + "from "+Titlesdata.TABLE+" "
                    + "group by tipo "
                    + "order by tipo ";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new Titlesdata();
                midato.type=rs.getInt("tipo");
                midato.longshares=rs.getLong("transactionShares");
                midato.longsecurity=rs.getLong("underlyingSecurityShares");
                
                if (midato.floatlongshares()>10 && midato.floatlongshares()<1000) {
                    Titlesdata.listtitles.add(midato);
                }
            }
            
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        if (Titlesdata.listtitles.size()>0) {
            //normalizing
            for (int i=0;i<Titlesdata.listtitles.size();i++) {
                midato=Titlesdata.listtitles.get(i);
                
                if (rmax<midato.floatlongshares()) rmax=midato.floatlongshares();
                if (rmin>midato.floatlongshares()) rmin=midato.floatlongshares();
                if (dmax<midato.floatlongsecurity()) dmax=midato.floatlongsecurity();
                if (dmin>midato.floatlongsecurity()) dmin=midato.floatlongsecurity();
                if (tmax<midato.floattotaltitles()) tmax=midato.floattotaltitles();
                if (tmin>midato.floattotaltitles()) tmin=midato.floattotaltitles();
            }
            for (int i=0;i<Titlesdata.listtitles.size();i++) {
                midato=Titlesdata.listtitles.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.floatlongshares());
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.floatlongsecurity());
                midato.cpoint=Tools.Normaliza(dmin,dmax,midato.floattotaltitles());
                Titlesdata.listtitles.set(i,midato);
            }
        }
    }
    /**
     * Draws list in table format
     * 
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] ahead=Titlesdata.Head().split(Tools.TAB);
        int r=Titlesdata.listtitles.size();
        String[][] atable=new String[r][ahead.length]; 
        
        Titlesdata midato=new Titlesdata();
        for (int i=0;i<r;i++) {
            midato=Titlesdata.listtitles.get(i);
            atable[i]=midato.string().split(Tools.TAB);
        }
        return atable;
    }
    /**
     * Draws list in LaTeX table format
     * @return formatted string to be paste in LaTeX editor.
     */
    public String DrawTableLaTeX() {
        Titlesdata midato=new Titlesdata();
        String cade="""
                    \\begin{table}[H]
                    \\caption{securityTitles for insider trading.\\label{tabla:securityTitles}}
                    \\begin{tabularx}{\\textwidth}{ r l r r}
                    \\toprule
                    """;
        cade+=Titlesdata.LatexHead();
        cade+=Tools.latexMidrule();
        int r=Titlesdata.listtitles.size();
        for (int i=0;i<r;i++) {
            midato=Titlesdata.listtitles.get(i);
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
     * @param maxdata
     */
    public void getDataset(int maxdata) {
        List<Titlesdata> milista=new ArrayList<>();
        Titlesdata.listtitles=new ArrayList<>();
        Titlesdata midato=new Titlesdata();
        
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
                        
                        midato=new Titlesdata();
                        String cade=rdato.securityTitle.toUpperCase();
                        cade=cade.replace("WARRENTS","WARRANTS");
                        cade=cade.replace("WARRNAT","WARRANTS");
                        cade=cade.replace("WARRNATS","WARRANTS");
                        cade=cade.replace("WARRENT","WARRANTS");
                        cade=cade.replace("WARRATS","WARRANTS");
                        cade=cade.replace("WARRATNS","WARRANTS");
                        cade=cade.replace("WARRATMS","WARRANTS");
                        cade=cade.replace("WAARRANT","WARRANTS");
                        cade=cade.replace("WARANT","WARRANTS");
                        cade=cade.replace("WARRANTSS","WARRANTS");
                        cade=cade.replace("\"","");
                        
                        midato.type=Titlesdata.getType(cade);
                        midato.securityTitle=cade;
                        midato.transactionShares=rdato.transactionShares;
                        midato.underlyingSecurityShares=rdato.underlyingSecurityShares;
                        //System.out.println(midato.cadena());
                        
                        if (midato.getSecurityTitle().length()>0)
                            milista.add(midato);
                    }
                    line = br.readLine();
                    max++;
                    //if (max % 1000 == 0) System.out.println(max);
                }
                br.close();
                //System.out.println("Titles.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<Titlesdata>() {
                @Override
                public int compare(Titlesdata o1, Titlesdata o2) {
                    return o1.getSecurityTitle().compareTo(o2.getSecurityTitle());
                }
            });
            
            String xtitle="XXX";
            Boolean primera=true;
            Titlesdata auxtitle=new Titlesdata();
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.getSecurityTitle().equals(xtitle)) {
                    if (primera) primera=false;
                    else {
                        Titlesdata.listtitles.add(midato);
                    }
                    xtitle=midato.getSecurityTitle();
                    auxtitle=new Titlesdata();
                    auxtitle.type=midato.type;
                    auxtitle.securityTitle=midato.securityTitle;
                }
                auxtitle.transactionShares+=midato.transactionShares;
                auxtitle.underlyingSecurityShares+=midato.underlyingSecurityShares;
            }
            if (!midato.getSecurityTitle().equals(xtitle)) {
                Titlesdata.listtitles.add(midato);
            }
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(Titlesdata.DROP_TABLE);
            stmt.executeUpdate(Titlesdata.CREATE_TABLE);
            for (int i=0;i<Titlesdata.listtitles.size();i++) {
                midato=Titlesdata.listtitles.get(i);
                midato.idtitle=i+1;
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
     * Calculates total amount from CSV file and stores in Oracle instance
     */
    public void getTotalAmount() {
        Titlesdata midato=new Titlesdata();
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
                        
                        Titlesdata.AddRecordsValue(rdato);
                    }
                    line = br.readLine();
                }
                br.close();
            }
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            for (int i=0;i<Titlesdata.listtitles.size();i++) {
                midato=Titlesdata.listtitles.get(i);
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
     * Create datatype table in Oracle instance
     */
    private void CreateTable() {
        l.SQLcommand(Titlesdata.CREATE_TABLE);
    }

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void CheckOutTable() {
        if (!l.TableExists(Titlesdata.TABLE)) CreateTable();
    }
    
    
    
}
