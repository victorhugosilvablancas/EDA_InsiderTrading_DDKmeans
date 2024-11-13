

package uaq.dcc.datasets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Dataverse configuration and retrieving.
 */
public class Dataverse {
    /**
     * file name
     */
    public String filename;
    /**
     * dynamic String type field list
     */
    public List<String> fields;
    /**
     * dynamic Registrsdata type list
     */
    public List<FileRecordsdata> data;
    /**
     * dynamic Integer type list for fields selected
     */
    public List<Integer> seleccionados;
    
    private static final String COMMA=",";
    private static final String TABB="\t";
    private static final String ENTER="\n";
    private static final String ET=" & ";
    private static final String BK=" \\\\";
    /**
     * if LaTeX output will be needed
     */
    public static Boolean LaTeX=false;
    
    /**
     * Class that defines empty Dataverse.
     */
    public Dataverse() {
        filename="";
        fields=new ArrayList<>();
        data=new ArrayList<>();
        seleccionados=new ArrayList<>();
    }
    /**
     * Class that defines Dataverse from:
     * index 0: HARVARD_INSIDERTRADING, lit_deriv.csv
     * index 1: HARVARD_SHAREHOLDER, lbh_blockholders.csv
     * 
     * @param iarchivo CSV file index.
     */
    public Dataverse(int iarchivo) {
        filename=listaarchivos[iarchivo];
        fields=new ArrayList<>();
        data=new ArrayList<>();
        seleccionados=new ArrayList<>();
    }
    
    /**
     * Retrieving csv headers.
     */
    public void getFields() {
        try {
            File f=new File(filename);
            if (f.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                //cabeza
                String[] partes=new String[0];
                String line = br.readLine();
                //System.out.println(line);
                while (line != null) {
                    partes=line.split(COMMA);
                    break;
                }
                br.close();
                if (partes.length>0) {
                    fields.addAll(Arrays.asList(partes));
                }
            }
        } catch (Exception e) {
            System.out.println("getFields: "+e.getLocalizedMessage());
        }
    }
    /**
     * Retrieving file records.
     * 
     * @param iregistros max number of records (for testing purposes, 0 = all)
     */
    public void getDataset(int iregistros) {
        Long max=Long.valueOf(0);
        if (iregistros>6900000 || iregistros==0) iregistros=6900000;
        FileRecordsdata midato=new FileRecordsdata();
        try {
            File f=new File(filename);
            if (f.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                boolean primera=true;
                String line = br.readLine();
                while (line != null) {
                    if (primera) primera=false;
                    else {
                        midato=new FileRecordsdata(line);
                        //System.out.println(midato.cadena());
                        data.add(midato);
                    }
                    line = br.readLine();
                    //if (max==iregistros) break;
                    max++;
                    
                    if (max % 100000 == 0)
                        System.out.println(max);
                }
                br.close();
                System.out.println("getDataset: size "+data.size());
            }
        } catch (Exception e) {
            System.out.println("getDataset: "+e.getLocalizedMessage());
        }
        System.out.println(max);
    }
    /**
     * Showing headers.
     */
    public void DrawFields() {
        for (int i=0;i<fields.size();i++) {
            System.out.println(i+TABB+fields.get(i));
            //System.out.println(campos.get(i)+" varchar2(0),");
        }
    }
    /**
     * Showing data.
     */
    public void DrawData() {
        for (int i=0;i<data.size();i++) {
            System.out.println(i+TABB+data.get(i).string());
            if (i==1000) break;
        }
    }
    /**
     * true if element is found
     */
    public static Boolean Encontrado=false;
    /**
     * Index for element find
     */
    public static Integer Indice=0;
    /**
     * Selecting just a field.
     * 
     * @param afield field name to be selected.
     */
    public void Select(String afield) {
        Encontrado=false;
        Indice=0;
        for (int i=0;i<fields.size();i++) {
            if (fields.get(i).equals(afield)) {
                Encontrado=true;
                Indice=i;
                break;
            }
        }
        if (Encontrado) {
            DrawRecords();
        }
    }
    /**
     * Selecting just a field.
     * 
     * @param ifiled field index to be selected.
     */
    public void Select(int ifiled) {
        Encontrado=false;
        Indice=0;
        for (int i=0;i<fields.size();i++) {
            if (i==ifiled) {
                Encontrado=true;
                Indice=i;
                break;
            }
        }
        if (Encontrado) {
            DrawRecords();
        }
    }
    
    /**
     * Selecting bulk of fields.
     * @param ifields field to be selected.
     */
    public void Select(int[] ifields) {
        Encontrado=false;
        seleccionados=new ArrayList<>();
        for (int i=0;i<fields.size();i++) {
            for (int j=0;j<ifields.length;j++) {
                if (i==ifields[j]) {
                    seleccionados.add(ifields[j]);
                    Encontrado=true;
                    break;
                }
            }
        }
        if (Encontrado) {
            DrawSelectedRecords();
        }
    }
    /**
     * Showing all data.
     */
    private void DrawRecords() {
        String afield="("+Indice+")"+fields.get(Indice);
        FileRecordsdata midato=new FileRecordsdata();
        for (int i=0;i<data.size();i++) {
            midato=data.get(i);
            System.out.println(midato.string());/*
            if (datos.get(i)!=null) {
                if (datos.get(i).length()>0) {
                    String[] partes=datos.get(i).split(COMMA);
                    if (Indice<partes.length) {
                        if (partes[Indice].length()>0)
                            System.out.println(acampo+TABB+i+":"+partes[Indice]);
                    }
                }
            }*/
        }
    }
    /**
     * Showing selected data.
     */
    private void DrawSelectedRecords() {
        FileRecordsdata midato=new FileRecordsdata();
        String SEPA=TABB;
        String FIN=ENTER;
        if (Dataverse.LaTeX) {
            System.out.println("\\hline");
            SEPA=ET;
            FIN=BK;
        }
        String cade="";
        int m=seleccionados.size();
        //cabeza
        for (int j=0;j<seleccionados.size();j++) {
            Indice=seleccionados.get(j);
            
            if (Dataverse.LaTeX) cade+="\\textbf{"+fields.get(Indice)+"}";
            else cade+=fields.get(Indice);
            
            if ((j+1)==m) cade+=FIN;
            else cade+=SEPA;
        }
        if (Dataverse.LaTeX) {
            System.out.println(cade);
            System.out.println("\\hline");
        } else System.out.print(cade);
        
        //datos
        for (int i=0;i<data.size();i++) {
            midato=data.get(i);
            System.out.println(midato.string());
            /*
            cade="";
            if (datos.get(i)!=null) {
                if (datos.get(i).length()>0) {
                    String[] partes=datos.get(i).split(COMMA);
                    for (int j=0;j<seleccionados.size();j++) {
                        Indice=seleccionados.get(j);
                        if (Indice<partes.length) {
                            cade+=partes[Indice];
                            if ((j+1)==m) cade+=FIN;
                            else cade+=SEPA;
                        }   
                    }
                }
            }
            if (Dataversos.LaTeX) {
                System.out.println(cade);
                System.out.println("\\hline");
            } else System.out.print(cade);*/
        } //while
    } //DespliegaDatosSeleccionados
    
    /**
     * selector for Harvard insider trading dataverse
     */
    public static final int HARVARD_INSIDERTRADING  =0;
    /**
     * selector for share holder dataverse
     */
    public static final int HARVARD_SHAREHOLDER     =1;
    /**
     * dataverses' path and names
     */
    public static String[] listaarchivos=new String[] {
        "C:\\VictorHugo\\Doctorado\\Tesis\\lit_deriv\\lit_deriv.csv", //Datos: 10'643,865
        "C:\\VictorHugo\\Doctorado\\Tesis\\lbh_blockholders\\lbh_blockholders.csv",
    };
        
}
