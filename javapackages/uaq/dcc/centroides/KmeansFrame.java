package uaq.dcc.centroides;

import java.awt.event.KeyEvent;
import java.util.ArrayList;
import static uaq.dcc.centroides.Datasdata.Normalizando;
import uaq.dcc.datasets.Titles;
import uaq.dcc.datasets.Titlesdata;
import uaq.dcc.edakmeansanalysis.MessageFrame;

/**
 *  Kmeans Environment.
 */
public class KmeansFrame extends javax.swing.JFrame {
    private KmeansForTitles kft;

    /**
     * Creates new form KmeansFrame
     */
    public KmeansFrame() {
        initComponents();
        setLocation(200,100);
        
        kft=new KmeansForTitles();
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        pTodo = new javax.swing.JPanel();
        tabulador = new javax.swing.JTabbedPane();
        pTitles = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        t_results = new javax.swing.JTextArea();
        pCabeza = new javax.swing.JPanel();
        t_clustersLb = new javax.swing.JLabel();
        t_clusters = new javax.swing.JTextField();
        t_epochsLb = new javax.swing.JLabel();
        t_epochs = new javax.swing.JTextField();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        t_results.setColumns(20);
        t_results.setRows(5);
        jScrollPane1.setViewportView(t_results);

        javax.swing.GroupLayout pTitlesLayout = new javax.swing.GroupLayout(pTitles);
        pTitles.setLayout(pTitlesLayout);
        pTitlesLayout.setHorizontalGroup(
            pTitlesLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pTitlesLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 662, Short.MAX_VALUE)
                .addContainerGap())
        );
        pTitlesLayout.setVerticalGroup(
            pTitlesLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pTitlesLayout.createSequentialGroup()
                .addGap(34, 34, 34)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 319, Short.MAX_VALUE)
                .addContainerGap())
        );

        tabulador.addTab("Titles", pTitles);

        pCabeza.setBorder(javax.swing.BorderFactory.createEtchedBorder());

        t_clustersLb.setText("Clusters");

        t_clusters.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        t_clusters.setText("0");
        t_clusters.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                t_clustersFocusGained(evt);
            }
        });
        t_clusters.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                t_clustersKeyPressed(evt);
            }
        });

        t_epochsLb.setText("Ephocs");

        t_epochs.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        t_epochs.setText("0");
        t_epochs.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                t_epochsFocusGained(evt);
            }
        });
        t_epochs.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                t_epochsKeyPressed(evt);
            }
        });

        javax.swing.GroupLayout pCabezaLayout = new javax.swing.GroupLayout(pCabeza);
        pCabeza.setLayout(pCabezaLayout);
        pCabezaLayout.setHorizontalGroup(
            pCabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCabezaLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(t_clustersLb)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(t_clusters, javax.swing.GroupLayout.PREFERRED_SIZE, 77, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(t_epochsLb)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(t_epochs, javax.swing.GroupLayout.PREFERRED_SIZE, 77, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        pCabezaLayout.setVerticalGroup(
            pCabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCabezaLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pCabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(t_clustersLb)
                    .addComponent(t_clusters, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(t_epochsLb)
                    .addComponent(t_epochs, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        javax.swing.GroupLayout pTodoLayout = new javax.swing.GroupLayout(pTodo);
        pTodo.setLayout(pTodoLayout);
        pTodoLayout.setHorizontalGroup(
            pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pTodoLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(tabulador)
                    .addComponent(pCabeza, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap())
        );
        pTodoLayout.setVerticalGroup(
            pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pTodoLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(pCabeza, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(tabulador, javax.swing.GroupLayout.DEFAULT_SIZE, 394, Short.MAX_VALUE)
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pTodo, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pTodo, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void t_clustersFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_t_clustersFocusGained
        t_clusters.selectAll();
    }//GEN-LAST:event_t_clustersFocusGained

    private void t_clustersKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_t_clustersKeyPressed
        if (evt.getKeyCode()==KeyEvent.VK_ENTER) t_epochs.requestFocus();
        if (evt.getKeyCode()==KeyEvent.VK_LEFT) t_epochs.requestFocus();
    }//GEN-LAST:event_t_clustersKeyPressed

    private void t_epochsFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_t_epochsFocusGained
        t_epochs.selectAll();
    }//GEN-LAST:event_t_epochsFocusGained

    private void t_epochsKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_t_epochsKeyPressed
        if (evt.getKeyCode()==KeyEvent.VK_ENTER) {
            if (getT_clusters()==0 || getT_epochs()==0) {
                new MessageFrame("Invalid numbers (must be > 0)").setVisible(true);
            } else {
                kft.Execute();
                t_clusters.requestFocus();
            }
        }
        if (evt.getKeyCode()==KeyEvent.VK_LEFT) t_clusters.requestFocus();
    }//GEN-LAST:event_t_epochsKeyPressed


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JPanel pCabeza;
    private javax.swing.JPanel pTitles;
    private javax.swing.JPanel pTodo;
    private javax.swing.JTextField t_clusters;
    private javax.swing.JLabel t_clustersLb;
    private javax.swing.JTextField t_epochs;
    private javax.swing.JLabel t_epochsLb;
    private javax.swing.JTextArea t_results;
    private javax.swing.JTabbedPane tabulador;
    // End of variables declaration//GEN-END:variables

    private Integer getT_clusters() {
        return Integer.valueOf(t_clusters.getText());
    }
    private Integer getT_epochs() {
        return Integer.valueOf(t_epochs.getText());
    }
    
    
    private class KmeansForTitles extends Kmeans {
        private Titles title;
        
        public KmeansForTitles() {
            title=new Titles();
            title.getList();
        }
        public void Execute() {
            Kmeans.PredictionResult="";
            Kmeans.Print=true;
            //step 1
            DataInitialization();
            //step 2
            ClustersDefinition(getT_clusters());
            if (Kmeans.Print) System.out.println("Clusters Defined");
            //step 3
            CentroidsInitialization();
            if (Kmeans.Print) System.out.println("Centroids Initialized");
            //step 4
            ComputeEachEuclideanDistance();
            if (Kmeans.Print) System.out.println("Euclidean Estimated");
            //step 5
            CentroidTraining(getT_epochs());
            if (Kmeans.Print) System.out.println("Training finished");
            //step 6
            //prediction
            Datasdata Hy=new Datasdata();
            
            Hy.A.x=0.03;
            Hy.A.y=0.06;
            Hy.A.z=0.09;
            Hy.B.x=0.04;
            Hy.B.y=0.07;
            Hy.B.z=0.10;
            Hy.C.x=0.05;
            Hy.C.y=0.08;
            Hy.C.z=0.11;
            
            AssignCathegory(Hy);
            t_results.setText(Kmeans.PredictionResult);
        }
        
        /**
        * 1.Data initializacion and Normalization.
        * 
        */
        public final void DataInitialization() {
            Datasdata.Dataset=new ArrayList<>();
            Datasdata midato=new Datasdata();
            
            Titlesdata tdato=new Titlesdata();
            for (int i=0;i<Titlesdata.listtitles.size();i++) {
                tdato=Titlesdata.listtitles.get(i);
                
                midato=new Datasdata(tdato);
                Datasdata.Dataset.add(midato);
            }
            //Normalizando
            if (Datasdata.Dataset.size()>0) {
                if (Kmeans.Print) 
                    System.out.println("Dataset "+Datasdata.Dataset.size());
                Normalizando();
            } else {
                if (Kmeans.Print) 
                    System.out.println("***Empty Dataset");
            }
        } //DataInitialization
        
        
        
    } //KmeansForTitles

}
