package uaq.dcc.centroides;

import java.util.Random;

/**
 *
 * K-means execution.
 */
public class Kmeans {
    /**
     * Define el arreglo estático de clústers.
     */
    public static Datasdata[] Cluster;
    /**
     * Define el número de épocas para entrenamiento.
     */
    private static int Ephocs=100;
    /**
     * Determina si (true) los datos serán tomados desde un archivo CSV
     * o (false) serán aleatorios.
     */
    public static boolean isCsvFile=false;
    /**
     * Determina si (true) los datos serán exportados en formato LaTeX
     * o (false) en formato TAB para hoja de cálculo.
     */
    private static boolean exportToLaTeX=false;
    /**
     * Determina si (true) los datos serán impresos a la consola.
     */
    public static Boolean Print=false;
    
    /**
     * Auxiliar en la generación de valores aleatorios.
     */
    private Random rn=new Random();
    
    /**
     * Clase que realiza el entrenamiento de centroides para la técnica K-means.
     */
    public Kmeans() {
    }
    
    /**
     * Ejecuta el análisis de las 3 hipótesis de la investigación.
     */
    public void AllHypothesis() {
        Kmeans.isCsvFile=true;
        Kmeans.exportToLaTeX=false;
        
        DataInitialization(100000);
        //System.out.println("Hipotesis_K_igual_M\t"+resultado.getTiempo());
        
        int k=8;
        ClustersDefinition(k);
        CentroidsInitialization();
        ComputeEachEuclideanDistance();
        CentroidTraining(10);
        //System.out.println("Hipotesis_K_igual_I\t"+resultado.getTiempo());
                /*
        //Tomando las matrices para MLP
        uaq.dcc.mlpAdaptaciones.Datasets.X=new Double[Kmeans.Cluster.length][2];
        uaq.dcc.mlpAdaptaciones.Datasets.Y=new Double[Kmeans.Cluster.length][1];
        uaq.dcc.mlpAdaptaciones.Datasets.input=new Double[Kmeans.Cluster.length][2];
        for (int j=0;j<Kmeans.Cluster.length;j++) {
            uaq.dcc.mlpAdaptaciones.Datasets.X[j][0]=Kmeans.Cluster[j].A.x;
            uaq.dcc.mlpAdaptaciones.Datasets.X[j][1]=Kmeans.Cluster[j].B.x;
            //Datasets.X[j][2]=Kmeans.Cluster[j].C.x;
            uaq.dcc.mlpAdaptaciones.Datasets.Y[j][0]=Kmeans.Cluster[j].getCentroid().x;
            //System.out.println(Kmeans.Cluster[j].cadena());
            
            //predictores aleatorios
            
            uaq.dcc.mlpAdaptaciones.Datasets.input[j][0]=rn.nextDouble(0, .99);
            uaq.dcc.mlpAdaptaciones.Datasets.input[j][1]=rn.nextDouble(0, .99);
        }
        //Fin Tomando las matrices para MLP
        */
        k=1;
        ClustersDefinition(k);
        CentroidsInitialization();
        ComputeEachEuclideanDistance();
        CentroidTraining(10);
        //System.out.println("Hipotesis_K_igual_0\t"+resultado.getTiempo());
    }
    /**
     * Ejecuta el análisis de la hipótesis k=m de la investigación.
     */
    public void Hypothesis_K_equals_M() {
        Kmeans.isCsvFile=true;
        Kmeans.exportToLaTeX=false;
        
        DataInitialization(100000);
        int k=Datasdata.Dataset.size();
        //se hace innecesario el entrenamiento
        //DefinirClusters(k);
        //IniciarClusters();
        //CalcularDistanciaEuclideanaYAsignarCluster();
        //EntrenarCentroides(10);
        //System.out.println("Hipotesis_K_igual_M\t"+resultado.getTiempo());
    }
    /**
     * Ejecuta el análisis de la hipótesis k=i de la investigación.
     */
    public void Hypothesis_K_equals_I() {
        Kmeans.isCsvFile=true;
        Kmeans.exportToLaTeX=false;
        
        DataInitialization(100000);
        int k=8;
        ClustersDefinition(k);
        CentroidsInitialization();
        ComputeEachEuclideanDistance();
        CentroidTraining(10);
        //System.out.println("Hipotesis_K_igual_I\t"+resultado.getTiempo());
    }
    /**
     * Ejecuta el análisis de la hipótesis k=1 de la investigación.
     */
    public void Hypothesis_K_equals_1() {
        Kmeans.isCsvFile=true;
        Kmeans.exportToLaTeX=false;
        
        DataInitialization(100000);
        int k=1;
        ClustersDefinition(k);
        CentroidsInitialization();
        ComputeEachEuclideanDistance();
        CentroidTraining(10);
        //System.out.println("Hipotesis_K_igual_0\t"+resultado.getTiempo());
    }
    /**
     * Rutina de experimentación de la clase.
     */
    public void Experimentation() {
        Kmeans.isCsvFile=true;
        Kmeans.exportToLaTeX=false;
        
        DataInitialization(100000);
        ClustersDefinition(3);
        CentroidsInitialization();
        ComputeEachEuclideanDistance();
        CentroidTraining(10);
        ElbowMethod();
        
        //PREDECIR
        Datasdata Hy=new Datasdata();
        if (Kmeans.isCsvFile) {
            Hy.A.x=3.0;
            Hy.B.x=500.0;
            Hy.C.x=200.0;
            Hy=Datasdata.Normaliza(Datasdata.DataMin, Datasdata.DataMax, Hy);
        } else {
            Hy.A.x=0.03;
            Hy.A.y=0.06;
            Hy.A.z=0.09;
            Hy.B.x=0.04;
            Hy.B.y=0.07;
            Hy.B.z=0.10;
            Hy.C.x=0.05;
            Hy.C.y=0.08;
            Hy.C.z=0.11;
        }
        AssignCathegory(Hy);
    }
    
    /**
     * 1.Iniciar datos ya sea desde archivo csv o * de forma aleatoria.
     * 
     * @param max máximos registros en caso de ser aleatorios.
     */
    public void DataInitialization(int max) {
        if (Kmeans.isCsvFile) Datasdata.DatasetCsvInit();
        else new Datasdata().RandomDatasetInit(max);
    }
    /**
     * 2.Definir el número de clústers K.
     * 
     * @param k número de clústers.
     */
    public void ClustersDefinition(Integer k) {
        Cluster=new Datasdata[k];
        for (int i=0;i<k;i++) {
            Cluster[i]=new Datasdata();
        }
    }
    /**
     * 3.Inicializar los centroides con valores aleatorios del dataset.
     */
    public void CentroidsInitialization() {
        int max=Datasdata.Dataset.size();
        
        int idato=0;
        int ianterior=0;
        
        if (Kmeans.exportToLaTeX) {
            System.out.println("\\begin{table}[ht!]");
            System.out.println("\\begin{spacing}{1.5}");
            System.out.println("\\caption{Clústeres definidos por selección aleatoria a partir del Dataset}\\label{table:mlp_kdefinidos}");
            System.out.println("\\centering");
            System.out.println("\\begin{tabular}{r r r r r r}");
            System.out.println("\\hline");
            System.out.println("\\textbf{k} & \\textbf{Dataset} & \\textbf{A} & \\textbf{B} & \\textbf{C} & \\textbf{Centroide}\\\\");
            System.out.println("\\hline");
            //System.out.println("***Clusters definidos:");
            for (int j=0;j<Cluster.length;j++) {
                //seleccionando un dato que no se repita
                while (idato==ianterior) {
                    idato=rn.nextInt(0, max-1);
                }
                ianterior=idato;

                Cluster[j]=Datasdata.Dataset.get(idato);
                Cluster[j].setK(j);
                //System.out.println(j+" & "+idato+" & "+Cluster[j].cadenaLaTeX()+"\\\\");
                System.out.println("\\hline");
            }
            System.out.println("\\end{tabular}");
            System.out.println("\\end{spacing}");
            System.out.println("\\end{table}");
            System.out.println("");
        } else {
            if (Kmeans.Print) 
                System.out.println("Clusters defined by random selection from dataset");
            //System.out.println("***Clusters definidos:");
            for (int j=0;j<Cluster.length;j++) {
                //seleccionando un dato que no se repita
                while (idato==ianterior) {
                    idato=rn.nextInt(0, max-1);
                }
                ianterior=idato;

                Cluster[j]=Datasdata.Dataset.get(idato);
                Cluster[j].setK(j);
              //  if (Kmeans.Imprime) 
                 //   System.out.println(j+"\t"+idato+"\t"+Cluster[j].cadena());
            }
            if (Kmeans.Print) System.out.println("");
        }
    }
    /**
     * 4.Calcular la distancia euclideana de cada punto al centroide de clústers.
     * Asignar cada punto al cluster que le es más cercano.
     */
    public void ComputeEachEuclideanDistance() {
        Datasdata midato=new Datasdata();
        Double rdistancia=0.0;
        if (Kmeans.Print) System.out.println("idx\tEuclideana\tDisCuadrada");
        for (int j=0;j<Cluster.length;j++) {
            for (int i=0;i<Datasdata.Dataset.size();i++) {
                midato=Datasdata.Dataset.get(i);
                rdistancia=Euclideandata.EuclideanDistance(Cluster[j].getCentroid(), midato.getCentroid());
                if (midato.distance>0) {
                    if (rdistancia<midato.distance) {
                        midato.distance=rdistancia;
                        midato.setK(j);
                    }
                } else {
                    midato.distance=rdistancia;
                    midato.setK(j);
                }
                //actualizando
                Datasdata.Dataset.set(i, midato);
                //if (Kmeans.Print) 
                    //System.out.println(String.format("%d\t%.5f\t%.5f\t%d", i,midato.distance,midato.getSquareDistance(),midato.getK()));
            } //for i
        } //for j
    }
    /**
     * 5.Entrenar los clústers al actualizar el valor del centroide,
     * tomando la media de todos los puntos de ese clúster.
     * @param iepocas número de épocas.
     */
    public void CentroidTraining(int iepocas) {
        Datasdata midato=new Datasdata();
        Parametricscoordinatesdata sumaA=new Parametricscoordinatesdata();
        Parametricscoordinatesdata sumaB=new Parametricscoordinatesdata();
        Parametricscoordinatesdata sumaC=new Parametricscoordinatesdata();
        
        Ephocs=iepocas;
        for (int iepoca=0;iepoca<Ephocs;iepoca++) {
            if (Kmeans.Print) 
                System.out.println(String.format("epoch %d",iepoca));
            Double rdistancia=0.0;
            int r=0;
            //actualizamos centroides
            for (int icluster=0;icluster<Cluster.length;icluster++) {
                sumaA=new Parametricscoordinatesdata();
                sumaB=new Parametricscoordinatesdata();
                sumaC=new Parametricscoordinatesdata();
                r=Datasdata.Dataset.size();
                for (int i=0;i<r;i++) {
                    midato=Datasdata.Dataset.get(i);
                    if (midato.getK().equals(icluster)) {
                        sumaA.x+=midato.A.x;
                        sumaA.y+=midato.A.y;
                        sumaA.z+=midato.A.z;
                        sumaB.x+=midato.B.x;
                        sumaB.y+=midato.B.y;
                        sumaB.z+=midato.B.z;
                        sumaC.x+=midato.C.x;
                        sumaC.y+=midato.C.y;
                        sumaC.z+=midato.C.z;
                    }
                }
                if (r>0) {
                    Cluster[icluster].A.x=sumaA.x/r;
                    Cluster[icluster].A.y=sumaA.y/r;
                    Cluster[icluster].A.z=sumaA.z/r;
                    Cluster[icluster].B.x=sumaB.x/r;
                    Cluster[icluster].B.y=sumaB.y/r;
                    Cluster[icluster].B.z=sumaB.z/r;
                    Cluster[icluster].C.x=sumaC.x/r;
                    Cluster[icluster].C.y=sumaC.y/r;
                    Cluster[icluster].C.z=sumaC.z/r;
                    
                    //recalcular distancia euclidiana Paso5
                    for (int i=0;i<Datasdata.Dataset.size();i++) {
                        midato=Datasdata.Dataset.get(i);
                        if (midato.getK().equals(icluster)) {
                            rdistancia=Euclideandata.EuclideanDistance(Cluster[icluster].getCentroid(), midato.getCentroid());
                            midato.distance=rdistancia;
                            //System.out.println("189: "+midato.distancia+","+midato.getDistanciaCuadrada());
                            Datasdata.Dataset.set(i, midato);
                        }
                    }
                }
                if (Kmeans.Print) 
                    System.out.println("\t"+icluster+":"+rdistancia);
            } //for icluster
        } //for iepocas
        
        if (Kmeans.exportToLaTeX) {
            System.out.println("\\begin{table}[ht!]");
            System.out.println("\\begin{spacing}{1.5}");
            System.out.println("\\caption{Clústeres entrenados ("+Ephocs+" épocas)}\\label{table:mlp_kentrenados}");
            System.out.println("\\centering");
            System.out.println("\\begin{tabular}{r r r r r}");
            System.out.println("\\hline");
            System.out.println("\\textbf{k} & \\textbf{A} & \\textbf{B} & \\textbf{C} & \\textbf{Centroide}\\\\");
            System.out.println("\\hline");
            for (int j=0;j<Kmeans.Cluster.length;j++) {
                //System.out.println(j+" &"+Kmeans.Cluster[j].cadenaLaTeX()+"\\\\");
                System.out.println("\\hline");
            }
            System.out.println("\\end{tabular}");
            System.out.println("\\end{spacing}");
            System.out.println("\\end{table}");
            System.out.println("");
        } else {
            if (Kmeans.Print) {
                String cade="Clusters trained with ("+Ephocs+" ephocs)";
                System.out.println(cade);
                for (int j=0;j<Kmeans.Cluster.length;j++) {
                    //System.out.println(j+"\t"+Kmeans.Cluster[j].cadenaLaTeX());
                }
                System.out.println("");
            }
        }
    }
    
    /**
     * Prueba de eficiencia a través de método de Elbow.
     */
    public void ElbowMethod() {
        Datasdata midato=new Datasdata();
        Double sdistancia=0.0;
        System.out.println("K-value\tWCSS");
        //System.out.println("idx\tEuclideana\tDisCuadrada");
        //SUMANDO LA DISTANCIA CUADRADA
        sdistancia=0.0;
        for (int i=0;i<Datasdata.Dataset.size();i++) {
            midato=Datasdata.Dataset.get(i);
            sdistancia+=midato.getSquareDistance();
            //System.out.println(String.format("%d\t%.4f\t%.4f\t%.4f",i+1,midato.distancia,midato.getDistanciaCuadrada(),sdistancia));
        } //for i
        Double wcss=0.0;
        for (int mika=0;mika<Cluster.length;mika++) {
            wcss=sdistancia/(mika+1);
            System.out.println(String.format("%d\t%.6f", mika,wcss));
        } //for j
    }
    
    /**
     * Prediction result
     */
    public static String PredictionResult="";
    /**
     * Asigna la categoría de la hipótesis H(y) en el clúster.
     * 
     * @param HipotesisYe hipótesis de y solicitada.
     * @return clúster padre
     */
    public static Integer AssignCathegory(Datasdata HipotesisYe) {
        Double rmaxdistance=99999.9;
        Double rmindistance=0.0;
        Integer icluster=-1;
        for (int j=0;j<Cluster.length;j++) {
            rmindistance=Euclideandata.EuclideanDistance(Cluster[j].getCentroid(), HipotesisYe.getCentroid());
            if (rmindistance<rmaxdistance) {
            //System.out.println(String.format("\tk=%d : %.3f < %.3f",j,rmindistance,rmaxdistance));
                icluster=j;
                rmaxdistance=rmindistance;
            }
        }
        if (Kmeans.exportToLaTeX) {
            System.out.println("\\begin{table}[ht!]");
            System.out.println("\\begin{spacing}{1.5}");
            System.out.println("\\caption{Predicción de Hy: el dato}\\label{table:mlp_predecir}");
            System.out.println("\\centering");
            System.out.println("\\begin{tabular}{r r r r}");
            System.out.println("\\hline");
            System.out.println("\\textbf{A} & \\textbf{B} & \\textbf{C} & \\textbf{Centroide}\\\\");
            System.out.println("\\hline");
            //System.out.println(HipotesisYe.cadenaLaTeX()+"\\\\");
            System.out.println("\\hline");
            System.out.println("\\multicolumn{3}{l}{Pertenece al Clúster} & "+icluster+"\\\\");
            System.out.println("\\hline");
            System.out.println("\\end{tabular}");
            System.out.println("\\end{spacing}");
            System.out.println("\\end{table}");
            System.out.println("");
        } else {
            PredictionResult="""
                             Hypothesis Prediction:
                             This Datasdata belongs to:
                             cluster\t"""+icluster+" ";
        }
        return icluster;
    }

}

