package uaq.dcc.centroides;

/**
 *
 * Euclidian distance definition.
 */
public class Euclideandata {
    
    /**
     * Class that defines the Euclidian distance on 3 dimensions for 2 points.
     */
    public Euclideandata() {
    }
    
    /**
     * Calculate Euclidian distance on 3 dimensions for 2 points.
     * 
     * @param A Parametricscoordinatesdata, point a.
     * @param B Parametricscoordinatesdata, point b.
     * @return Euclidian distance on 3 dimensions.
     */
    public static Double EuclideanDistance(Parametricscoordinatesdata A,Parametricscoordinatesdata B) {
        Double rx=(B.x-A.x)*(B.x-A.x);
        Double ry=(B.y-A.y)*(B.y-A.y);
        Double rz=(B.z-A.z)*(B.z-A.z);
        Double rparameters=0.0;
        int n=A.parameters.size();
        for (int i=0;i<A.parameters.size();i++) {
            rparameters+=(B.parameters.get(i)-A.parameters.get(i))*(B.parameters.get(i)-A.parameters.get(i));
        }
        Double AtoB = rx+ry+rz+rparameters;
        Double AtoBto2 = Math.sqrt(AtoB);
        return AtoBto2;
    }
    
    
}
