package uaq.dcc.oracle;

/**
 *
 * @author Victor Hugo Silva
 */
public class Oracleinstancedata {
    private String instruccion;
    
    public String[] cabeza=new String[0];
    public String[][] tabla=new String[0][0];
    

    public Oracleinstancedata() {
        instruccion="";
        cabeza=new String[0];
        tabla=new String[0][0];
    }
    
    public void setInstruccion(String instruccion) {
        if (instruccion!=null) {
            if (instruccion.equals("null")) 
                this.instruccion="";
            else this.instruccion=instruccion.toUpperCase();
        } else this.instruccion="";
    }
    public String getInstruccion() {
        return instruccion.toUpperCase();
    }
    
    public boolean esValida() {
        boolean hay=false;
        for (int i=0;i<tipos.length;i++) {
            hay=getInstruccion().contains(tipos[i]);
            if (hay) break;
        }
        return hay;
    }
    
    public Boolean esSelect() {
        return getInstruccion().contains(tipos[SELECT]);
    }
    public Integer getColumnas() {
        Integer cols=1;
        cabeza=new String[] {"Resultado"};
        if (esSelect()) {
            //LIMITES DE SELECT..FROM
            if (getInstruccion().contains("FROM")) {
                int idx=7;
                int fdx=getInstruccion().indexOf("FROM");
                cabeza=getInstruccion().substring(idx, fdx).split(",");
                cols=cabeza.length;
            }
        }
        return cols;
    }
    
    public static final int SELECT=0;
    public static final int UPDATE=1;
    public static final int DELETE=2;
    public static final int INSERT=3;
    public static String[] tipos=new String[] {
        "SELECT",
        "UPDATE",
        "DELETE",
        "INSERT",
    };
    
}
