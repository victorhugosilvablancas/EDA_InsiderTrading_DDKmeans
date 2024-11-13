package uaq.dcc.centroides;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Data structure definition.
 */
public class Datasdata {
    /**
     *  triangle's A point
     */
    public Parametricscoordinatesdata A;
    /**
     *  triangle's B point
     */
    public Parametricscoordinatesdata B;
    /**
     *  triangle's C point
     */
    public Parametricscoordinatesdata C;
    /**
     *  euclidean distance
     */
    public Double distance;
    /**
     * nearest cluster
     */
    private Integer k;
    /**
     * dynamic data list for Datasdata type
     */
    public static List<Datasdata> Dataset=new ArrayList<>();
    
    /**
     * Class that defines the parametrics coordinates 
     * with zero values.
     */
    public Datasdata() {
        A=new Parametricscoordinatesdata();
        B=new Parametricscoordinatesdata();
        C=new Parametricscoordinatesdata();
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from renglon.
     * 
     * @param renglon text string with X coordinates for each point.
     */
    public Datasdata(String renglon) {
        String[] partes=renglon.split(",");
        A=new Parametricscoordinatesdata(partes[0]);
        B=new Parametricscoordinatesdata(partes[1]);
        C=new Parametricscoordinatesdata(partes[2]);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from Titlesdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.Transactionstypedata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from Sharesdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.Sharesdata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from PricePerSharesdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.PricePerSharesdata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from Exercisesdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.Exercisesdata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from Titlesdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.Titlesdata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from Ownershipdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.Ownershipdata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    /**
     * Class that defines the parametrics coordinates
     * with values taken from EquitySwapsdata.
     * 
     * @param tdato text string with X coordinates for each point.
     */
    public Datasdata(uaq.dcc.datasets.EquitySwapsdata tdato) {
        A=new Parametricscoordinatesdata(tdato.bpoint);
        B=new Parametricscoordinatesdata(tdato.apoint);
        C=new Parametricscoordinatesdata(tdato.cpoint);
        distance=0.0;
        k=0;
    }
    
    
    /**
     * 
     * @param k value.
     */
    public void setK(Integer k) {
        this.k=k;
    }
    /**
     * 
     * @return k value.
     */
    public Integer getK() {
        return k;
    }
    /**
     * 
     * @return square distance.
     */
    public Double getSquareDistance() {
        return distance*distance;
    }
    /**
     * 
     * @return centroid for (A,B,C).
     */
    public Parametricscoordinatesdata getCentroid() {
        return Centroiddata.CentroidOf(A, B, C);
    }
    /**
     * 
     * @return k value and its distance.
     */
    public String stringDistance() {
        return String.format("k,%d,distancia,%.4f", k,distance);
    }
    /**
     * 
     * @return columns names in tabular separated format.
     */
    public String head() {
        String cade="P(x,y,x";
        for (int i=0;i<A.units.size();i++) {
            cade+=","+A.units.get(i);
        }
        cade+=")";
        return cade;
    }
    /**
     * 
     * @return columns names for LaTeX format.
     */
    public String headLaTeX() {
        String cade="P(x & y & x";
        for (int i=0;i<A.units.size();i++) {
            cade+=" & "+A.units.get(i);
        }
        cade+=")\\\\";
        return cade;
    }
    /**
     * 
     * @return parameters values.
     */
    public String stringParameters() {
        return A.stringWithParameters()+"\n"
                + B.stringWithParameters()+"\n"
                + C.stringWithParameters()
                ;
    }
    /**
     * 
     * @return units values.
     */
    public String stringUnits() {
        return A.stringunits()+"\n"
                + B.stringunits()+"\n"
                + C.stringunits()
                ;
    }
    /**
     * 
     * @return descriptions values.
     */
    public String stringDescriptions() {
        return A.stringdescriptions()+"\n"
                + B.stringdescriptions()+"\n"
                + C.stringdescriptions()
                ;
    }
    
    /**
     * 
     * @return parameters values in LaTeX format.
     */
    public String stringLaTeXParameters() {
        return A.stringLaTeXparameters()+"\\\\"
                + B.stringLaTeXparameters()+"\\\\"
                + C.stringLaTeXparameters()+"\\\\"
                ;
    }
    /**
     * 
     * @return units values in LaTeX format.
     */
    public String stringLaTeXUnits() {
        return A.stringLaTeXunits()+"\\\\"
                + B.stringLaTeXunits()+"\\\\"
                + C.stringLaTeXunits()+"\\\\"
                ;
    }
    /**
     * 
     * @return descriptions values in LaTeX format.
     */
    public String stringLaTeXDescriptions() {
        return A.stringLaTeXdescriptions()+"\\\\"
                + B.stringLaTeXdescriptions()+"\\\\"
                + C.stringLaTeXdescriptions()+"\\\\"
                ;
    }
    
    /**
     * Random dataset initialization.
     * 
     * @param point max point numbers.
     */
    public void RandomDatasetInit(Integer point) {
        Dataset=new ArrayList<>();
        Datasdata midato=new Datasdata();
        Random rn=new Random();
        
        Double rtemperatura=0.0;
        Double rdensidad=0.0;
        if (Kmeans.Print) System.out.println(head());
        
        for (int i=0;i<point;i++) {
            midato=new Datasdata();
            midato.A.x=rn.nextDouble(0.001, 0.999);
            midato.A.y=rn.nextDouble(0.001, 0.999);
            midato.A.z=rn.nextDouble(0.001, 0.999);
            
            rtemperatura=rn.nextDouble(-140.0, 140.0);
            midato.A.parameters.add(rtemperatura);
            midato.A.units.add("°C");
            midato.A.descriptions.add("temperatura");
            rdensidad=rn.nextDouble(10.0, 500.0);
            midato.A.parameters.add(rdensidad);
            midato.A.units.add("g/cc");
            midato.A.descriptions.add("densidad");
            
            
            midato.B.x=rn.nextDouble(0.001, 0.999);
            midato.B.y=rn.nextDouble(0.001, 0.999);
            midato.B.z=rn.nextDouble(0.001, 0.999);
            
            rtemperatura=rn.nextDouble(-140.0, 140.0);
            midato.B.parameters.add(rtemperatura);
            midato.B.units.add("°C");
            midato.B.descriptions.add("temperatura");
            rdensidad=rn.nextDouble(10.0, 500.0);
            midato.B.parameters.add(rdensidad);
            midato.B.units.add("g/cc");
            midato.B.descriptions.add("densidad");
            
            
            midato.C.x=rn.nextDouble(0.001, 0.999);
            midato.C.y=rn.nextDouble(0.001, 0.999);
            midato.C.z=rn.nextDouble(0.001, 0.999);
            
            rtemperatura=rn.nextDouble(-140.0, 140.0);
            midato.C.parameters.add(rtemperatura);
            midato.C.units.add("°C");
            midato.C.descriptions.add("temperatura");
            rdensidad=rn.nextDouble(10.0, 500.0);
            midato.C.parameters.add(rdensidad);
            midato.C.units.add("g/cc");
            midato.C.descriptions.add("densidad");
            
            Datasdata.Dataset.add(midato);
            //System.out.println(i+": "+midato.cadena());
        }
        //if (Kmeans.Imprime) System.out.println("***Dataset de "+puntos+" registros");
    }
    /**
     * CSV dataset initialization form RAWDATA.CSV file.
     * 
     */
    public static void DatasetCsvInit() {
        Datasdata.Dataset=new ArrayList<>();
        Datasdata midato=new Datasdata();
        try {
            File f=new File("datosbrutos.csv");
            BufferedReader br = new BufferedReader(new FileReader(f));
            
            String line="";
            Boolean cabeza=true;
            int i=0;
            //String line = br.readLine();
            while ((line = br.readLine()) != null) {
                if (cabeza) cabeza=false;
                else {
                    midato=new Datasdata(line);
                    Datasdata.Dataset.add(midato);
                    if (Kmeans.Print) {
                        //if (i<Mostrar) System.out.println(i+": "+midato.cadena());
                    }
                    i++;
                }
            }
            br.close();
        } catch (Exception e) {
            System.out.println("IniciaDatasetCsv: "+e.getLocalizedMessage());
        }
        //Normalizando
        if (Datasdata.Dataset.size()>0) {
            if (Kmeans.Print) 
                System.out.println("***Dataset de "+Datasdata.Dataset.size()+" registros (Normalizado)");
            //x = (x-xmin)/(xmax-xmin)
            //y = (y-ymin)/(ymax-ymin)
            //z = (z-zmin)/(zmax-zmin)
            //1.obteniendo mínimos y máximos
            Normalizando();
            if (Kmeans.Print) 
                System.out.println("...");
        } else System.out.println("***Dataset vacío");
    }
    
    public static void Normalizando() {
        Datasdata midato=new Datasdata();
        
        Double rmin=999999.0;
        int imaxparameters=Datasdata.Dataset.get(0).A.parameters.size();

        Datasdata min=new Datasdata();
        min.A.x=rmin;
        min.A.y=rmin;
        min.A.z=rmin;
        min.B.x=rmin;
        min.B.y=rmin;
        min.B.z=rmin;
        min.C.x=rmin;
        min.C.y=rmin;
        min.C.z=rmin;
        for (int j=0;j<imaxparameters;j++) {
            min.A.parameters.add(rmin);
            min.B.parameters.add(rmin);
            min.C.parameters.add(rmin);
        }

        Datasdata max=new Datasdata();
        for (int i=0;i<Datasdata.Dataset.size();i++) {
            midato=Datasdata.Dataset.get(i);
            if (midato.A.x>max.A.x) max.A.x=midato.A.x;
            if (midato.A.y>max.A.y) max.A.y=midato.A.y;
            if (midato.A.z>max.A.z) max.A.z=midato.A.z;
            if (midato.B.x>max.B.x) max.B.x=midato.B.x;
            if (midato.B.y>max.B.y) max.B.y=midato.B.y;
            if (midato.B.z>max.B.z) max.B.z=midato.B.z;
            if (midato.C.x>max.C.x) max.C.x=midato.C.x;
            if (midato.C.y>max.C.y) max.C.y=midato.C.y;
            if (midato.C.z>max.C.z) max.C.z=midato.C.z;

            for (int j=0;j<imaxparameters;j++) {
                if (midato.A.parameters.get(j)>max.A.parameters.get(j)) 
                    max.A.parameters.set(j,midato.A.parameters.get(j));
                if (midato.B.parameters.get(j)>max.B.parameters.get(j)) 
                    max.B.parameters.set(j,midato.B.parameters.get(j));
                if (midato.C.parameters.get(j)>max.B.parameters.get(j)) 
                    max.C.parameters.set(j,midato.C.parameters.get(j));
            }

            if (midato.A.x<min.A.x) min.A.x=midato.A.x;
            if (midato.A.y<min.A.y) min.A.y=midato.A.y;
            if (midato.A.z<min.A.z) min.A.z=midato.A.z;
            if (midato.B.x<min.B.x) min.B.x=midato.B.x;
            if (midato.B.y<min.B.y) min.B.y=midato.B.y;
            if (midato.B.z<min.B.z) min.B.z=midato.B.z;
            if (midato.C.x<min.C.x) min.C.x=midato.C.x;
            if (midato.C.y<min.C.y) min.C.y=midato.C.y;
            if (midato.C.z<min.C.z) min.C.z=midato.C.z;
            for (int j=0;j<imaxparameters;j++) {
                if (midato.A.parameters.get(j)<min.A.parameters.get(j)) 
                    min.A.parameters.set(j,midato.A.parameters.get(j));
                if (midato.B.parameters.get(j)<min.B.parameters.get(j)) 
                    min.B.parameters.set(j,midato.B.parameters.get(j));
                if (midato.C.parameters.get(j)<min.B.parameters.get(j)) 
                    min.C.parameters.set(j,midato.C.parameters.get(j));
            }
        } //for i
        //2.normalizando de acuerdo a la fórmula
        for (int i=0;i<Datasdata.Dataset.size();i++) {
            midato=Datasdata.Dataset.get(i);
            midato=Normaliza(min,max,midato);
            Datasdata.Dataset.set(i, midato);

            if (Kmeans.Print) 
                System.out.println(String.format("%d : distance = %.2f",i,midato.distance));

        }
    }
    
    /**
     * Min dataset value pending normalization.
     */
    public static Datasdata DataMin=new Datasdata();
    /**
     * Max dataset value pending normalization.
     */
    public static Datasdata DataMax=new Datasdata();
    /**
     * return normalized Datasdata(x,y,z) value according to:
     * x = (x-xmin)/(xmax-xmin)
     * y = (y-ymin)/(ymax-ymin)
     * z = (z-zmin)/(zmax-zmin)
     * 
     * @param amin min values.
     * @param amax max values.
     * @param adata data to be normalized.
     * @return 
     */
    public static Datasdata Normaliza(Datasdata amin, Datasdata amax, Datasdata adata) {
        Datasdata normalizado=new Datasdata();
        
        Double xmaxmin=0.0;
        Double ymaxmin=0.0;
        Double zmaxmin=0.0;
        Double parammaxmin=0.0;
        int imaxparameters=adata.A.parameters.size();
        
        //difference between max and min for A
        xmaxmin=amax.A.x-amin.A.x;
        ymaxmin=amax.A.y-amin.A.y;
        zmaxmin=amax.A.z-amin.A.z;
        if (xmaxmin!=0) 
            normalizado.A.x=(adata.A.x-amin.A.x)/xmaxmin;
        if (ymaxmin!=0) 
            normalizado.A.y=(adata.A.y-amin.A.y)/ymaxmin;
        if (zmaxmin!=0) 
            normalizado.A.z=(adata.A.z-amin.A.z)/zmaxmin;
        for (int j=0;j<imaxparameters;j++) {
            parammaxmin=amax.A.parameters.get(j)-amin.A.parameters.get(j);
            normalizado.A.parameters.add(parammaxmin);
        }
        
        //difference between max and min for B
        xmaxmin=amax.B.x-amin.B.x;
        ymaxmin=amax.B.y-amin.B.y;
        zmaxmin=amax.B.z-amin.B.z;
        if (xmaxmin!=0) 
            normalizado.B.x=(adata.B.x-amin.B.x)/xmaxmin;
        if (ymaxmin!=0) 
            normalizado.B.y=(adata.B.y-amin.B.y)/ymaxmin;
        if (zmaxmin!=0) 
            normalizado.B.z=(adata.B.z-amin.B.z)/zmaxmin;
        for (int j=0;j<imaxparameters;j++) {
            parammaxmin=amax.B.parameters.get(j)-amin.B.parameters.get(j);
            normalizado.B.parameters.add(parammaxmin);
        }
        
        //difference between max and min for C
        xmaxmin=amax.C.x-amin.C.x;
        ymaxmin=amax.C.y-amin.C.y;
        zmaxmin=amax.C.z-amin.C.z;
        if (xmaxmin!=0) 
            normalizado.C.x=(adata.C.x-amin.C.x)/xmaxmin;
        if (ymaxmin!=0) 
            normalizado.C.y=(adata.C.y-amin.C.y)/ymaxmin;
        if (zmaxmin!=0) 
            normalizado.C.z=(adata.C.z-amin.C.z)/zmaxmin;
        for (int j=0;j<imaxparameters;j++) {
            parammaxmin=amax.C.parameters.get(j)-amin.C.parameters.get(j);
            normalizado.C.parameters.add(parammaxmin);
        }
        
        DataMin=amin;
        DataMax=amax;
                
        return normalizado;
    }
}
