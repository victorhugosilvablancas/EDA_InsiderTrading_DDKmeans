package uaq.dcc.ploteo;


import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RenderingHints;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.Ellipse2D;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.swing.JPanel;
import uaq.dcc.datasets.EquitySwaps;
import uaq.dcc.datasets.EquitySwapsdata;
import uaq.dcc.datasets.Exercise;
import uaq.dcc.datasets.Exercisesdata;
import uaq.dcc.datasets.Ownership;
import uaq.dcc.datasets.Ownershipdata;
import uaq.dcc.datasets.PricePerShares;
import uaq.dcc.datasets.PricePerSharesdata;
import uaq.dcc.datasets.Shares;
import uaq.dcc.datasets.Sharesdata;
import uaq.dcc.datasets.Titles;
import uaq.dcc.datasets.Titlesdata;
import uaq.dcc.datasets.Transactionstype;
import uaq.dcc.datasets.Transactionstypedata;
import uaq.dcc.edakmeansanalysis.MessageFrame;

/**
 * 
 * Frame for user interaction with all research hypothesis.
 */
public class PloteoFrame  extends javax.swing.JFrame {
    private Transactionstype type;
    private Shares share;
    private PricePerShares pps;
    private Exercise exer;
    private Titles title;
    private Ownership own;
    private EquitySwaps swaps;
    
    private static int iHypothesis=0;
    
    public static final int HYPO_TRANSACTIONS  =0;
    public static final int HYPO_SHARES        =1;
    public static final int HYPO_PRICE         =2;
    public static final int HYPO_EXERCISE      =3;
    public static final int HYPO_TITLES        =4;
    public static final int HYPO_OWNERSHIP     =5;
    public static final int HYPO_SWAPS         =6;
    
    public PloteoFrame(int iHypothesis) {
        PloteoFrame.iHypothesis=iHypothesis;
        
        switch (PloteoFrame.iHypothesis) {
            case PloteoFrame.HYPO_TRANSACTIONS  : type=new Transactionstype(); break;
            case PloteoFrame.HYPO_SHARES        : share=new Shares(); break;
            case PloteoFrame.HYPO_PRICE         : pps=new PricePerShares(); break;
            case PloteoFrame.HYPO_EXERCISE      : exer=new Exercise(); break;
            case PloteoFrame.HYPO_TITLES        : title=new Titles(); break;
            case PloteoFrame.HYPO_OWNERSHIP    : own=new Ownership(); break;
            case PloteoFrame.HYPO_SWAPS         : swaps=new EquitySwaps(); break;
        }
        
        
        initComponents();
        switch (PloteoFrame.iHypothesis) {
            case PloteoFrame.HYPO_TRANSACTIONS  : setBounds(160, 60,600,600); break;
            case PloteoFrame.HYPO_SHARES        : setBounds(180, 80,600,600); break;
            case PloteoFrame.HYPO_PRICE         : setBounds(200,100,600,600); break;
            case PloteoFrame.HYPO_EXERCISE      : setBounds(220,120,600,600); break;
            case PloteoFrame.HYPO_TITLES        : setBounds(240,140,600,600); break;
            case PloteoFrame.HYPO_OWNERSHIP     : setBounds(260,160,600,600); break;
            case PloteoFrame.HYPO_SWAPS         : setBounds(280,180,600,600); break;
        }
    }
    
    /**
     * Retrieve all data collections.
     */
    private void getLists() {
        switch (PloteoFrame.iHypothesis) {
            case PloteoFrame.HYPO_TRANSACTIONS  : 
                if (Transactionstypedata.listtransactions.isEmpty()) type.getList();
                pTransactionsType = new TransactiontypePanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Transactions Type", pTransactionsType);
                setTitle("Transactions Type");
                break;
            case PloteoFrame.HYPO_SHARES        : 
                if (Sharesdata.listshares.isEmpty()) share.getList();
                pShares = new SharesPanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Transactions Shares", pShares);
                setTitle("Transactions Shares");
                break;
            case PloteoFrame.HYPO_PRICE         : 
                if (PricePerSharesdata.listpricepershares.isEmpty()) pps.getList();
                pPrice = new PricePanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Price per Share", pPrice);
                setTitle("Price per Share");
                break;
            case PloteoFrame.HYPO_EXERCISE      : 
                if (Exercisesdata.listpricepershares.isEmpty()) exer.getList();
                pDateFn = new ExercisePanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Date Function", pDateFn);
                setTitle("Date Function");
                break;
            case PloteoFrame.HYPO_TITLES        : 
                if (Titlesdata.listtitles.isEmpty()) title.getList();
                pTitle = new TitlePanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Security Title", pTitle);
                setTitle("Security Title");
                break;
            case PloteoFrame.HYPO_OWNERSHIP    : 
                if (Ownershipdata.listownerships.isEmpty()) own.getList();
                pOwnership = new OwnershipPanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Direct or Indirect Ownership", pOwnership);
                setTitle("Direct or Indirect Ownership");
                break;
            case PloteoFrame.HYPO_SWAPS         : 
                if (EquitySwapsdata.listequityswaps.isEmpty()) swaps.getList();
                pSwaps = new EquitySwapsPanel(head_clusters.getText(),head_epochs.getText());
                tabulator.addTab("Equity Swap", pSwaps);
                setTitle("Equity Swap");
                break;
        }

        
        IniciaPloteo();
    }
    
    private void initComponents() {
        pCanvas = new javax.swing.JPanel();
        pHead = new javax.swing.JPanel();
        tabulator = new javax.swing.JTabbedPane();
        head_clustersLb = new javax.swing.JLabel();
        head_clusters = new javax.swing.JTextField();
        head_epochsLb = new javax.swing.JLabel();
        head_epochs = new javax.swing.JTextField();
        jScrollPane1 = new javax.swing.JScrollPane();
        t_results = new javax.swing.JTextArea();

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        
        pCanvas.setBackground(new Color(50,50,50));
        pHead.setBackground(new Color(150,150,150));

        pHead.setBorder(javax.swing.BorderFactory.createEtchedBorder());
        head_clustersLb.setText("Clusters");

        head_clusters.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        head_clusters.setText("2");
        head_clusters.setEnabled(false);
        head_clusters.addFocusListener(new java.awt.event.FocusAdapter() {
            @Override
            public void focusGained(java.awt.event.FocusEvent evt) {
                t_clustersFocusGained(evt);
            }
        });
        head_clusters.addKeyListener(new java.awt.event.KeyAdapter() {
            @Override
            public void keyPressed(java.awt.event.KeyEvent evt) {
                t_clustersKeyPressed(evt);
            }
        });

        head_epochsLb.setText("Ephocs");

        head_epochs.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        head_epochs.setText("10");
        head_epochs.setEnabled(false);
        head_epochs.addFocusListener(new java.awt.event.FocusAdapter() {
            @Override
            public void focusGained(java.awt.event.FocusEvent evt) {
                t_epochsFocusGained(evt);
            }
        });
        head_epochs.addKeyListener(new java.awt.event.KeyAdapter() {
            @Override
            public void keyPressed(java.awt.event.KeyEvent evt) {
                t_epochsKeyPressed(evt);
            }
        });

        t_results.setColumns(20);
        t_results.setRows(5);
        jScrollPane1.setViewportView(t_results);
        
        getLists();
        
        javax.swing.GroupLayout pCabezaLayout = new javax.swing.GroupLayout(pHead);
        pHead.setLayout(pCabezaLayout);
        pCabezaLayout.setHorizontalGroup(
            pCabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCabezaLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(head_clustersLb)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(head_clusters, javax.swing.GroupLayout.PREFERRED_SIZE, 77, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(head_epochsLb)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(head_epochs, javax.swing.GroupLayout.PREFERRED_SIZE, 77, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        pCabezaLayout.setVerticalGroup(
            pCabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCabezaLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pCabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(head_clustersLb)
                    .addComponent(head_clusters, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(head_epochsLb)
                    .addComponent(head_epochs, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        javax.swing.GroupLayout pTodoLayout = new javax.swing.GroupLayout(pCanvas);
        pCanvas.setLayout(pTodoLayout);
        pTodoLayout.setHorizontalGroup(
            pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pTodoLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(tabulator)
                    .addComponent(pHead, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap())
        );
        pTodoLayout.setVerticalGroup(
            pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pTodoLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(pHead, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(tabulator, javax.swing.GroupLayout.DEFAULT_SIZE, 394, Short.MAX_VALUE)
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pCanvas, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pCanvas, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );

        pack();
    } //INIT
    
    private void t_clustersFocusGained(java.awt.event.FocusEvent evt) {                                       
        head_clusters.selectAll();
    }                                      

    private void t_clustersKeyPressed(java.awt.event.KeyEvent evt) {                                      
        if (evt.getKeyCode()==KeyEvent.VK_ENTER) head_epochs.requestFocus();
        if (evt.getKeyCode()==KeyEvent.VK_LEFT) head_epochs.requestFocus();
    }                                     

    private void t_epochsFocusGained(java.awt.event.FocusEvent evt) {                                     
        head_epochs.selectAll();
    }                                    

    private void t_epochsKeyPressed(java.awt.event.KeyEvent evt) {                                    
        if (evt.getKeyCode()==KeyEvent.VK_ENTER) {
            if (getT_clusters()==0 || getT_epochs()==0) {
                new MessageFrame("Invalid numbers (must be > 0)").setVisible(true);
            } else {
                head_clusters.requestFocus();
            }
        }
        if (evt.getKeyCode()==KeyEvent.VK_LEFT) head_clusters.requestFocus();
    }                                   

    
    //VARIABLES
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JPanel pCanvas;
    private javax.swing.JPanel pHead;
    
    private javax.swing.JTextField head_clusters;
    private javax.swing.JLabel head_clustersLb;
    private javax.swing.JTextField head_epochs;
    private javax.swing.JLabel head_epochsLb;
    private javax.swing.JTextArea t_results;
    private javax.swing.JTabbedPane tabulator;
    
    private TransactiontypePanel pTransactionsType;
    private SharesPanel pShares;
    private PricePanel pPrice;
    private ExercisePanel pDateFn;
    private TitlePanel pTitle;
    private OwnershipPanel pOwnership;
    private EquitySwapsPanel pSwaps;
    //VARIABLES FIN

    
    private Integer getT_clusters() {
        return Integer.valueOf(head_clusters.getText());
    }
    private Integer getT_epochs() {
        return Integer.valueOf(head_epochs.getText());
    }
    
    //PLOTEOPANEL
    
    List<Ellipse2D> milista=new ArrayList<>();
    
    public void IniciaPloteo() {
        Ellipse2D mipunto;
        Random rn=new Random();
        for (int i=0;i<100;i++) {
            double px=rn.nextDouble(0, 400);
            double py=rn.nextDouble(0, 400);
            mipunto=new Ellipse2D.Double(px, py, 2, 2);
            milista.add(mipunto);
        }
    }
    
    class PloteoPanel extends JPanel {
        List pointList;
        Color selectedColor;
        Ellipse2D selectedPoint;

        public PloteoPanel() {
            pointList = new ArrayList();
            selectedColor = Color.red;
            //addMouseListener(new PointLocater(this));
            setBackground(Color.white);
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D)g;
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                                RenderingHints.VALUE_ANTIALIAS_ON);
            Ellipse2D e;
            Color color;
            for(int j = 0; j < milista.size(); j++) {
                e = milista.get(j);
                if (e.getY() > f(e.getX())) 
                    color = Color.red;
                else color = Color.green;
                g2.setPaint(color);
                g2.fill(e);
            }
            for(int j = 0; j < 400; j++) {
                    double px=j;
                    double py=j+1;
                    e = new Ellipse2D.Double(px, py, 2, 2);
                    color = Color.blue;
                    g2.setPaint(color);
                    g2.fill(e);
            }
        }

        /**
         * y = 3 x + 5;
         * @param e el punto
         * @return abajo o arriba de la línea
         */
        private Double f(Double e) {
            return e * 1.2 + 50;
        }
        
        public List getPointList() {
            return pointList;
        }

        public void setSelectedPoint(Ellipse2D e) {
            selectedPoint = e;
            repaint();
        }

        public void addPoint(Point p) {
            Ellipse2D e = new Ellipse2D.Double(p.x - 3, p.y - 3, 6, 6);
            pointList.add(e);
            selectedPoint = null;
            repaint();
        }
    }

    class PointLocater extends MouseAdapter {
        PloteoPanel pointPanel;

        public PointLocater(PloteoPanel pp) {
            pointPanel = pp;
        }

        @Override
        public void mousePressed(MouseEvent e) {
            Point p = e.getPoint();
            boolean haveSelection = false;
            List list = pointPanel.getPointList();
            Ellipse2D ellipse;
            for(int j = 0; j < list.size(); j++) {
                ellipse = (Ellipse2D)list.get(j);
                if(ellipse.contains(p)) {
                    pointPanel.setSelectedPoint(ellipse);
                    haveSelection = true;
                    break;
                }
            }
            if(!haveSelection)
                pointPanel.addPoint(p);
        }
    }
    //PLOTEOPANEL FIN

    
}
