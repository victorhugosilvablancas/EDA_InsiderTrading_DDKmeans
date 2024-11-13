package uaq.dcc.centroides;

/**
 *
 * Main class.
 */
public class InfiniteTypeCentroid {

    public static void main(String[] args) {
        //dcc.ejemplos.Librerosdata libreros=new dcc.ejemplos.Librerosdata();
        //libreros.setIniciales();
        
        //dcc.ejemplos.Triangulodata triangulo=new dcc.ejemplos.Triangulodata();
        //triangulo.setIniciales();
        
        uaq.dcc.centroides.Datasdata misdatos=new uaq.dcc.centroides.Datasdata();
        //temperatura y densidad
        misdatos.RandomDatasetInit(100);
        System.out.println(Datasdata.Dataset.size());
        
        
        System.out.println("\t"+Datasdata.Dataset.get(0).head());
        for (int j=0;j<Datasdata.Dataset.size();j++) {
            System.out.println(j+"\t"+Datasdata.Dataset.get(j).getCentroid().stringWithParameters());
        }
        
        System.out.println("Distancia entre 30 y 85 = "
                + uaq.dcc.centroides.Euclideandata.EuclideanDistance(Datasdata.Dataset.get(30).A,Datasdata.Dataset.get(85).A));
        System.out.println("Distancia entre 50 y 51 = "
                + uaq.dcc.centroides.Euclideandata.EuclideanDistance(Datasdata.Dataset.get(50).A,Datasdata.Dataset.get(51).A));
        
    }
}
