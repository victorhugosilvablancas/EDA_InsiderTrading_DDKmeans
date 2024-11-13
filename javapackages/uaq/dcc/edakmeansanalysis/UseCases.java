package uaq.dcc.edakmeansanalysis;

import uaq.dcc.centroides.Centroiddata;
import uaq.dcc.centroides.Parametricscoordinatesdata;

/**
 *
 * Use cases for classes Centroid
 */
public class UseCases {
    
    /**
     * Constructor for UseCases class
     */
    public UseCases() {
    }
    
    /**
     * SoftwareX example 1.
     */
    
    public static void InsiderTradingCase() {
        
    }
    
    /**
     * SoftwareX example 2.
     * 
     * Look for eda_shuffle figure
     */
    public static void ShuttleCentroid() {
        //1.defining variables
        Parametricscoordinatesdata A;//Nose
        Parametricscoordinatesdata B;//LeftWing
        Parametricscoordinatesdata C;//RightWing
        
        //2.taking initial values with temperature = 10 Celcius degrees and earth gravity
        Double rtemperature=10.0;
        Double rgravity=9.81;
        
        A=new uaq.dcc.centroides.Parametricscoordinatesdata(
                1.0,5.0,1.0,
                rtemperature,"degrees","start up temperature",
                rgravity,"m/s^2","earth gravity"
                );
        B=new uaq.dcc.centroides.Parametricscoordinatesdata(
                10.0,3.0,5.0,rtemperature,"degrees","Start up temperature",
                rgravity,"m/s^2","earth gravity"
                );
        C=new uaq.dcc.centroides.Parametricscoordinatesdata(
                1.0,7.0,11.0,rtemperature,"degrees","Start up temperature",
                rgravity,"m/s^2","earth gravity"
                );
        
        //3.retrieving the centroid
        Parametricscoordinatesdata Centroid=Centroiddata.CentroidOf(A, B, C);
        System.out.println(String.format("Suttle Centroid with %.0f temperature, %.2f gravity and %.2f NetValue.",
                rtemperature,rgravity,Centroiddata.NetValue));
        System.out.println(Centroid.stringWithParameters());
        
        //4.changing temperature to 500 Celcius degrees and zero gravity
        rtemperature=500.0;
        rgravity=0.0;
        
        A=new uaq.dcc.centroides.Parametricscoordinatesdata(
                1.0,5.0,1.0,rtemperature,"degrees","Start up temperature",
                rgravity,"m/s^2","zero gravity"
                );
        B=new uaq.dcc.centroides.Parametricscoordinatesdata(
                10.0,3.0,5.0,rtemperature,"degrees","Start up temperature",
                rgravity,"m/s^2","zero gravity"
                );
        C=new uaq.dcc.centroides.Parametricscoordinatesdata(
                1.0,7.0,11.0,rtemperature,"degrees","Start up temperature",
                rgravity,"m/s^2","zero gravity"
                );
        
        //5.retrieving the centroid
        Centroid=uaq.dcc.centroides.Centroiddata.CentroidOf(A, B, C);
        System.out.println(String.format("Suttle Centroid with %.0f temperature, %.2f gravity and %.2f NetValue.",
                rtemperature,rgravity,Centroiddata.NetValue));
        System.out.println(Centroid.stringWithParameters());
    }

}
