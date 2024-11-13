package uaq.dcc.centroides;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Parametrics coordinates definition.
 */
public class Parametricscoordinatesdata {
    /**
     * cartesian x coordinate.
     */
    public Double x;
    /**
     * cartesian y coordinate.
     */
    public Double y;
    /**
     * cartesian z coordinate.
     */
    public Double z;
    /**
     * dynamic list for parameters values.
     */
    public List<Double> parameters;
    /**
     * dynamic list for parameters units.
     */
    public List<String> units;
    /**
     * dynamic list for parameters descriptions.
     */
    public List<String> descriptions;
    
    /**
     * Class that defines coordinates (x,y,z,param*) where * is the ancient notation for 'dynamic' or 'pointer'
     * with zero values.
     * 
     */
    public Parametricscoordinatesdata() {
        x=0.0;
        y=0.0;
        z=0.0;
        parameters=new ArrayList<>();
        units=new ArrayList<>();
        descriptions=new ArrayList<>();
    }
    /**
     * Class that defines coordinates (x,y,z,param*) where * is the ancient notation for 'dynamic' or 'pointer'
     * with zero values.
     * 
     * @param rx the X coordinate
     * @param ry the Y coordinate
     * @param rz the Z coordinate
     * @param rparam1 parameter 1
     * @param aunit1 unit 1
     * @param adescription1 description 1
     * @param rparam2 parameter 2
     * @param aunit2 unit 2
     * @param adescription2 description 2
     */
    public Parametricscoordinatesdata(Double rx,Double ry,Double rz,
            Double rparam1,String aunit1,String adescription1,
            Double rparam2,String aunit2,String adescription2) {
        x=rx;
        y=ry;
        z=rz;
        parameters=new ArrayList<>();
        units=new ArrayList<>();
        descriptions=new ArrayList<>();
        
        parameters.add(rparam1);
        units.add(aunit1);
        descriptions.add(adescription1);
        parameters.add(rparam2);
        units.add(aunit2);
        descriptions.add(adescription2);
    }
    /**
     * Class that defines coordinates (x,y,z,param*) where * is the ancient notation for 'dynamic' or 'pointer'
     * with x value read from ax.
     * 
     * @param ax x intial value.
     */
    public Parametricscoordinatesdata(String ax) {
        x=Double.valueOf(ax);
        y=0.0;
        z=0.0;
        parameters=new ArrayList<>();
        units=new ArrayList<>();
        descriptions=new ArrayList<>();
    }
    /**
     * Class that defines coordinates (x,y,z,param*) where * is the ancient notation for 'dynamic' or 'pointer'
     * with float(double) values.
     * 
     * @param xvalue initial value.
     */
    public Parametricscoordinatesdata(Double xvalue) {
        x=xvalue;
        y=0.0;
        z=0.0;
        parameters=new ArrayList<>();
        units=new ArrayList<>();
        descriptions=new ArrayList<>();
    }
    /**
     * 
     * @return string compose of (x,y,z) coordinates and parameters values.
     */
    public String stringWithParameters() {
        String cade="(";
        cade+=String.format("%.4f,%.4f,%.4f", x,y,z);
        for (int i=0;i<parameters.size();i++) {
            cade+=String.format(",%.4f", parameters.get(i));
        }
        cade+=")";
        return cade;
    }
    /**
     * 
     * @return string compose of (x,y,z) names and units values.
     */
    public String stringunits() {
        String cade="(x,y,z";
        for (int i=0;i<units.size();i++) {
            cade+=","+units.get(i);
        }
        cade+=")";
        return cade;
    }
    /**
     * 
     * @return string compose of (x,y,z) names and description values.
     */
    public String stringdescriptions() {
        String cade="(x,y,z";
        for (int i=0;i<descriptions.size();i++) {
            cade+=","+descriptions.get(i);
        }
        cade+=")";
        return cade;
    }
    
    /**
     * @return string compose of (x,y,z) coordinates and parameters values for LaTeX.
     */
    public String stringLaTeXparameters() {
        String cade="(";
        cade+=String.format("%.4f & %.4f & %.4f", x,y,z);
        for (int i=0;i<parameters.size();i++) {
            cade+=String.format(" & %.4f", parameters.get(i));
        }
        cade+=")\\\\";
        return cade;
    }
    /**
     * 
     * @return string compose of (x,y,z) names and units values for LaTeX.
     */
    public String stringLaTeXunits() {
        String cade="(x & y & z";
        for (int i=0;i<units.size();i++) {
            cade+=" & "+units.get(i);
        }
        cade+=")\\\\";
        return cade;
    }
    /**
     * 
     * @return string compose of (x,y,z) names and description values for LaTeX.
     */
    public String stringLaTeXdescriptions() {
        String cade="(x & y & z";
        for (int i=0;i<descriptions.size();i++) {
            cade+=" & "+descriptions.get(i);
        }
        cade+=")\\\\";
        return cade;
    }
    
}
