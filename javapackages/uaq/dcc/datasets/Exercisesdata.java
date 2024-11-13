package uaq.dcc.datasets;

import uaq.dcc.edakmeansanalysis.Tools;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Exercise data definition.
 */
public class Exercisesdata {
    public Integer idexercise;
    public Integer exerciseDate;
    public String exerciseDateFn;
    public Integer records;
    public Double apoint;
    public Double bpoint;
    public Double cpoint;
    
    public static final String TABLE="HARVARD_EXERCISE";
    public static List<Exercisesdata> listpricepershares=new ArrayList<>();
    
    /**
     * Class that defines Exercisesdata.
     */
    public Exercisesdata() {
        idexercise=0;
        exerciseDate=0;
        exerciseDateFn="";
        records=0;
        apoint=0.0;
        bpoint=0.0;
        cpoint=0.0;
    }
    /**
     * 
     * @return exercise function
     */
    public String getExerciseDateFn() {
        if (exerciseDateFn!=null) {
            if (exerciseDateFn.equals("null")) return "Unknwon";
            else return exerciseDateFn.replace("\"", "");
        } else return "Unknwon";
    }
    /**
     * 
     * @return exercise number
     */
    public Integer getExerciseNum() {
        return Tools.OnlyNumbers(getExerciseDateFn()).intValue();
    }
    /**
     * 
     * @return formatted string
     */
    public String string() {
        return getExerciseDateFn()+"\t"
                + exerciseDate+"\t"
                + records
                ;
    }
    /**
     * 
     * @return table head
     */
    public static String Head() {
        return "exerciseDateFn\t"
                + "exerciseDate (last)\t"
                + "records"
                ;
    }
    /**
     * 
     * @return table head for LaTeX
     */
    public static String LatexHead() {
        return """
               \\textbf{exerciseDateFn}& \\textbf{exerciseDate (last)}& \\textbf{records}\\\\
               """;
    }
    /**
     * 
     * @return formatted string for LaTex at the end
     */
    public String latex() {
        return getExerciseDateFn()
                + " & "+exerciseDate
                + " & "+records
                + "\\\\\n";
    }
    /**
     * 
     * @return formatted points for LaTex
     */
    public String latexpoints() {
        return "... & ... & ... \\\\\n";
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
        return "insert into "+Exercisesdata.TABLE+" ("
                + "idexercise,"
                + "exerciseDate,"
                + "exerciseDateFn,"
                + "records "
                + ") values ("
                + ""+idexercise+","
                + ""+exerciseDate+","
                + "'"+exerciseDateFn+"',"
                + ""+records+" "
                + ")";
    }
    /**
     * update record string for Oracle instance
     * 
     * @return 
     */
    public String SaveStr() {
        return "update "+Exercisesdata.TABLE+" set "
                + "exerciseDateFn='"+exerciseDateFn+"',"
                + "records="+records+" "
                + "where idexercise="+idexercise+" "
                ;
    }
    
    /**
     * Create datatype table in Oracle instance
     */
    public static final String CREATE_TABLE="create table "+Exercisesdata.TABLE+" ("
                + "idexercise number(9) primary key,"
                + "exerciseDate number(4) default 0,"
                + "exerciseDateFn varchar2(80),"
                + "records number(9) default 0 "
                + ")";
    /**
     * Drops datatype table in Oracle instance
     */
    public static final String DROP_TABLE="drop table "+Exercisesdata.TABLE+" ";
    
}
