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
 * Ownershipt Analysis.
 */
public class Ownership {
    public Ownershipdata data;
    private String filename;
    
    private Tools l=new Tools();
    
    /**
     * Class that defines Ownership.
     */
    public Ownership() {
        CheckOutTable();
        data=new Ownershipdata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        Ownershipdata.listownerships=new ArrayList<>();
        Ownershipdata midato=new Ownershipdata();
        
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
            String instruccion="select * from "+Ownershipdata.TABLE+" "
                    + "where transactionShares between 1000000 and 10000000 "
                    + "order by directOrIndirectOwnership,exerciseDate";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new Ownershipdata();
                midato.idowner=rs.getInt("idowner");
                midato.directOrIndirectOwnership=rs.getString("directOrIndirectOwnership");
                midato.transactionShares=rs.getLong("transactionShares");
                midato.exerciseDate=rs.getInt("exerciseDate");
                midato.records=rs.getInt("registros");
                
                Ownershipdata.listownerships.add(midato);
            }
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        if (Ownershipdata.listownerships.size()>0) {
            //normalizing
            for (int i=0;i<Ownershipdata.listownerships.size();i++) {
                midato=Ownershipdata.listownerships.get(i);
                
                if (rmax<midato.transactionShares.doubleValue()) rmax=midato.transactionShares.doubleValue();
                if (rmin>midato.transactionShares.doubleValue()) rmin=midato.transactionShares.doubleValue();
                if (dmax<midato.exerciseDate.doubleValue()) dmax=midato.exerciseDate.doubleValue();
                if (dmin>midato.exerciseDate.doubleValue()) dmin=midato.exerciseDate.doubleValue();
                if (tmax<midato.records.doubleValue()) tmax=midato.records.doubleValue();
                if (tmin>midato.records.doubleValue()) tmin=midato.records.doubleValue();
            }
            for (int i=0;i<Ownershipdata.listownerships.size();i++) {
                midato=Ownershipdata.listownerships.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.transactionShares.doubleValue());
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.exerciseDate.doubleValue());
                midato.cpoint=Tools.Normaliza(dmin,dmax,midato.records.doubleValue());
                Ownershipdata.listownerships.set(i,midato);
            }
        }
    }
    /**
     * Draws list in table format
     * 
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] ahead=Ownershipdata.Head().split(Tools.TAB);
        
        int r=Ownershipdata.listownerships.size();
        String[][] atable=new String[r][ahead.length];
        
        Ownershipdata midato=new Ownershipdata();
        for (int i=0;i<Ownershipdata.listownerships.size();i++) {
            midato=Ownershipdata.listownerships.get(i);
            atable[i]=midato.string().split(Tools.TAB);
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
        List<Ownershipdata> milista=new ArrayList<>();
        Ownershipdata midato=new Ownershipdata();
        Ownershipdata.SumShares=Long.valueOf(0);
        Ownershipdata.SumRecords=0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select "
                    + "directOrIndirectOwnership,"
                    + "sum(transactionShares) as transactionShares,"
                    + "sum(registros) as registros "
                    + "from "+Ownershipdata.TABLE+" "
                    + "group by directOrIndirectOwnership "
                    + "order by directOrIndirectOwnership ";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new Ownershipdata();
                midato.directOrIndirectOwnership=rs.getString("directOrIndirectOwnership");
                midato.transactionShares=Math.abs(rs.getLong("transactionShares"));
                midato.records=rs.getInt("registros");
                
                milista.add(midato);
                
                Ownershipdata.SumShares+=midato.transactionShares;
                Ownershipdata.SumRecords+=midato.records;
            }
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("DrawTable: "+e.getLocalizedMessage());
        }
        if (milista.size()>0) {
            cade="""
                \\begin{table}[H]
                \\caption{Direct Or Indirect Ownerships for insider trading.\\label{tabla:directOrIndirectOwnerships}}
                \\begin{tabularx}{\\textwidth}{ l r r r r }
                \\toprule
                """;
            cade+=Ownershipdata.LatexHead();
            cade+=Tools.latexMidrule();
            int r=milista.size();
            for (int i=0;i<r;i++) {
                midato=milista.get(i);

                Double rporshare=midato.transactionShares.doubleValue()/Ownershipdata.SumShares.doubleValue()*100;
                Double rporrecord=midato.records.doubleValue()/Ownershipdata.SumRecords.doubleValue()*100;

                cade+=midato.getDirectOrIndirectOwnership()
                        + " & "+midato.transactionShares
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
        List<Ownershipdata> milista=new ArrayList<>();
        Ownershipdata.listownerships=new ArrayList<>();
        Ownershipdata midato=new Ownershipdata();
        
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
                        
                        midato=new Ownershipdata();
                        midato.directOrIndirectOwnership=rdato.directOrIndirectOwnership;
                        midato.transactionShares=Long.valueOf(rdato.transactionShares);
                        midato.setExerciseDate(rdato.exerciseDate);
                        
                        milista.add(midato);
                        max++;
                        //if (max % 1000 == 0) System.out.println(max);
                    }
                    line = br.readLine();
                }
                br.close();
                //System.out.println("Ownership.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<Ownershipdata>() {
                @Override
                public int compare(Ownershipdata o1, Ownershipdata o2) {
                    return o1.getDIOandDate().compareTo(o2.getDIOandDate());
                }
            });
            
            String xaux="XXX";
            Boolean primera=true;
            Ownershipdata auxdato=new Ownershipdata();
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.getDIOandDate().equals(xaux)) {
                    if (primera) primera=false;
                    else {
                        Ownershipdata.listownerships.add(auxdato);
                    }
                    xaux=midato.getDIOandDate();
                    auxdato=new Ownershipdata();
                    auxdato.directOrIndirectOwnership=midato.directOrIndirectOwnership;
                    auxdato.exerciseDate=midato.exerciseDate;
                }
                auxdato.transactionShares+=midato.transactionShares;
                auxdato.records++;
            }
            if (auxdato.records>0) {
                Ownershipdata.listownerships.add(auxdato);
            }
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(Ownershipdata.DROP_TABLE);
            stmt.executeUpdate(Ownershipdata.CREATE_TABLE);
            for (int i=0;i<Ownershipdata.listownerships.size();i++) {
                midato=Ownershipdata.listownerships.get(i);
                midato.idowner=i+1;
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
        l.SQLcommand(Ownershipdata.CREATE_TABLE);
    }

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void CheckOutTable() {
        if (!l.TableExists(Ownershipdata.TABLE)) CreateTable();
    }
    
    
    
}
