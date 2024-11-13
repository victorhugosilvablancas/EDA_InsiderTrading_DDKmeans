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
 * Exercise analysis.
 */
public class Exercise {
    public Exercisesdata data;
    private String filename;
    
    private Tools l=new Tools();
    
    /**
     * Class that defines Exercises.
     */
    public Exercise() {
        CheckOutTable();
        data=new Exercisesdata();
        filename=Dataverse.listaarchivos[uaq.dcc.datasets.Dataverse.HARVARD_INSIDERTRADING];
    }
    
    /**
     * Retrieve dynamic list from database
     */
    public void getList() {
        Exercisesdata.listpricepershares=new ArrayList<>();
        Exercisesdata midato=new Exercisesdata();
        
        Double rmax=0.0;
        Double rmin=999999999.0;
        Double dmax=0.0;
        Double dmin=3000.0;
        try {
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            String instruccion="select * from "+Exercisesdata.TABLE+" "
                    + "where records between 1000 and 100000 ";
            ResultSet rs=stmt.executeQuery(instruccion);
            while (rs.next()) {
                midato=new Exercisesdata();
                midato.idexercise=rs.getInt("idexercise");
                midato.exerciseDate=rs.getInt("exerciseDate");
                midato.exerciseDateFn=rs.getString("exerciseDateFn");
                midato.records=rs.getInt("records");
                
                Exercisesdata.listpricepershares.add(midato);
            }
            stmt.close();conn.close();
        } catch (Exception e) {
            System.out.println("getList: "+e.getLocalizedMessage());
        }
        if (Exercisesdata.listpricepershares.size()>0) {
            Exercisesdata.listpricepershares.sort(new Comparator<Exercisesdata>() {
                @Override
                public int compare(Exercisesdata o1, Exercisesdata o2) {
                    return o1.getExerciseNum().compareTo(o2.getExerciseNum());
                }
            });
            
            //normalizing
            for (int i=0;i<Exercisesdata.listpricepershares.size();i++) {
                midato=Exercisesdata.listpricepershares.get(i);
                
                if (rmax<midato.getExerciseNum()) rmax=midato.getExerciseNum().doubleValue();
                if (rmin>midato.getExerciseNum()) rmin=midato.getExerciseNum().doubleValue();
                if (dmax<midato.records) dmax=midato.records.doubleValue();
                if (dmin>midato.records) dmin=midato.records.doubleValue();
            }
            
            for (int i=0;i<Exercisesdata.listpricepershares.size();i++) {
                midato=Exercisesdata.listpricepershares.get(i);
                midato.apoint=Tools.Normaliza(rmin,rmax,midato.getExerciseNum().doubleValue());
                midato.bpoint=Tools.Normaliza(dmin,dmax,midato.records.doubleValue());
                Exercisesdata.listpricepershares.set(i,midato);
            }
        }
        
    }
    /**
     * Draws list in table format
     * @return a table with all data list
     */
    public String[][] DrawTable() {
        String[] ahead=Exercisesdata.Head().split(Tools.TAB);
        int r=Exercisesdata.listpricepershares.size();
        String[][] atable=new String[r][ahead.length]; 
        Exercisesdata midato=new Exercisesdata();
        for (int i=0;i<r;i++) {
            midato=Exercisesdata.listpricepershares.get(i);
            atable[i]=midato.string().split(Tools.TAB);
        }
        return atable;
    }
    /**
     * Draws list in LaTeX table format
     * @return formatted string to be paste in LaTeX editor.
     */
    public String DrawTableLaTeX() {
        Exercisesdata midato=new Exercisesdata();
        String cade="""
                    \\begin{table}[H]
                    \\caption{Exercise Date Funcion records.\\label{tabla:exerciseDateFn}}
                    \\begin{tabularx}{\\textwidth}{ l r l r}
                    \\toprule
                    """;
        cade+=Exercisesdata.LatexHead();
        cade+=Tools.latexMidrule();
        int r=Exercisesdata.listpricepershares.size();
        boolean fin=false;
        for (int i=0;i<r;i++) {
            midato=Exercisesdata.listpricepershares.get(i);
            cade+=midato.latex();
        }
        if (!fin) cade+="\\\\\n";
        //cade+=midato.latexpuntos();
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
        List<Exercisesdata> milista=new ArrayList<>();
        Exercisesdata.listpricepershares=new ArrayList<>();
        Exercisesdata midato=new Exercisesdata();
        
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
                        
                        midato=new Exercisesdata();
                        midato.setExerciseDate(rdato.exerciseDate);
                        midato.exerciseDateFn=rdato.exerciseDateFn.replace("\"", "");
                        
                        if (midato.exerciseDateFn.contains("-")) {
                            midato.exerciseDateFn=midato.exerciseDateFn.replace("-", ",");
                            String[] partes=midato.exerciseDateFn.split(",");
                            if (partes.length>1) {
                                for (int k=0;k<partes.length;k++) {
                                    Exercisesdata kdato=new Exercisesdata();
                                    kdato.exerciseDate=midato.exerciseDate;
                                    kdato.exerciseDateFn=partes[k];
                                    milista.add(kdato);
                                    max++;
                                    //if (max % 1000 == 0) System.out.println(max);
                                }
                            }
                        } else {
                            milista.add(midato);
                        }
                        //System.out.println(midato.cadena());
                    }
                    line = br.readLine();
                }
                br.close();
                //System.out.println("Exercise.getDataset: size "+milista.size());
            }
            
            milista.sort(new Comparator<Exercisesdata>() {
                @Override
                public int compare(Exercisesdata o1, Exercisesdata o2) {
                    return o1.getExerciseDateFn().compareTo(o2.getExerciseDateFn());
                }
            });
            
            String xfuncion="XXX";
            Boolean primera=true;
            Exercisesdata auxdato=new Exercisesdata();
            for (int i=0;i<milista.size();i++) {
                midato=milista.get(i);
                if (!midato.getExerciseDateFn().equals(xfuncion)) {
                    if (primera) primera=false;
                    else {
                        Exercisesdata.listpricepershares.add(auxdato);
                    }
                    auxdato=new Exercisesdata();
                    auxdato.exerciseDate=midato.exerciseDate;
                    auxdato.exerciseDateFn=midato.exerciseDateFn;
                    xfuncion=midato.getExerciseDateFn();
                }
                auxdato.records++;
            }
            Exercisesdata.listpricepershares.add(auxdato);
            
            Class.forName(Clusterdata.Driver);
            Connection conn = DriverManager.getConnection(Clusterdata.Url,Clusterdata.InstanceUser,Clusterdata.InstancePwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(Exercisesdata.DROP_TABLE);
            stmt.executeUpdate(Exercisesdata.CREATE_TABLE);
            for (int i=0;i<Exercisesdata.listpricepershares.size();i++) {
                midato=Exercisesdata.listpricepershares.get(i);
                midato.idexercise=i+1;
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
        l.SQLcommand(Exercisesdata.CREATE_TABLE);
    }

    /**
     * Check if datatype table exists in Oracle instances,
     * otherwise call for its creation.
     */
    private void CheckOutTable() {
        if (!l.TableExists(Exercisesdata.TABLE)) CreateTable();
    }
    
}
