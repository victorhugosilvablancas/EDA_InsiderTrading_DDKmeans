package uaq.dcc.centroides;

/**
 * Infinite Type Centroid Definition.
 */
public class Centroiddata {
    /**
     * NetValue is the centroid value including parameters.
     */
    public static Double NetValue=0.0;
    
    /**
     * Class that defines the Centroid.
     */
    public Centroiddata() {
    }
    /**
     * 
     * Matrix X centroid belonging cluster k.
     * 
     * @param X data matrix.
     * @param ik cluster k (i=type integer).
     * @return centroid value.
     */
    public static Double CentroidOf(Double[][] X,Integer ik) {
        Double centroide=0.0;
        Double rsumaDeXenCk=0.0;
        Integer ipuntostotales=0;
        for (int i=0;i<X.length;i++) {
            if (X[i][1].equals(ik.doubleValue())) {
                rsumaDeXenCk+=X[i][0];
                ipuntostotales++;
            }
        }
        centroide=rsumaDeXenCk/ipuntostotales;
        return centroide;
    }
    
    /**
     * 
     * Matrix X Error Square Sum Suma belonging cluster k.
     * 
     * @param X data matrix X.
     * @param ik cluster k (i=type integer).
     * @return error square sum.
     */
    public static Double SquareErrSum(Double[][] X,Integer ik) {
        Double rsuma=0.0;
        Double error=0.0;
        for (int i=0;i<X.length;i++) {
            if (X[i][1].equals(ik.doubleValue())) {
                error=X[i][0]-CentroidOf(X,ik);
                rsuma+=Math.pow(error, 2);
            }
        }
        return rsuma;
    }
    /**
     * 
     * Triangle (A,B,C) centroid for parametrics coordinates.
     * 
     * @param A Parametricscoordinatesdata A.
     * @param B Parametricscoordinatesdata B.
     * @param C Parametricscoordinatesdata C.
     * @return centroid location for (A,B,C,param*) triangle.
     */
    public static Parametricscoordinatesdata CentroidOf(Parametricscoordinatesdata A,Parametricscoordinatesdata B,Parametricscoordinatesdata C) {
        Parametricscoordinatesdata midato=new Parametricscoordinatesdata();
        int points=3;
        midato.x=(A.x+B.x+C.x)/points;
        midato.y=(A.y+B.y+C.y)/points;
        midato.z=(A.z+B.z+C.z)/points;
        int n=A.parameters.size();
        
        Double rparameterssum=0.0;
        for (int i=0;i<n;i++) {
            rparameterssum=(A.parameters.get(i)+B.parameters.get(i)+C.parameters.get(i))/points;
            
        }
        if (n>0) midato.parameters.add(rparameterssum/n);
        else midato.parameters.add(0.0);
        
        NetValue=((A.x+B.x+C.x)+(A.y+B.y+C.y)+(A.z+B.z+C.z)+rparameterssum)/points;
        
        return midato;
    }
    
    
    /**
     * 
     * Triangle (A,B,C) centroid for 3 points dimension.
     * 
     * @param Ax coordinate x for A point.
     * @param Ay coordinate y for A point.
     * @param Bx coordinate x for B point.
     * @param By coordinate y for B point.
     * @param Cx coordinate x for C point.
     * @param Cy coordinate y for C point.
     * @return centroid location for (A,B,C) triangle.
     */
    public static String CentroidOf(Double Ax,Double Ay,Double Bx,Double By,Double Cx,Double Cy) {
        Double equis=(Ax+Bx+Cx)/3;
        Double ye=(Ay+By+Cy)/3;
        return String.format("Centroide = (%.4f,%.4f)", equis, ye);
    }
    
}
