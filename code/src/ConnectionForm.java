import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import  java.io.*;
import java.sql.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.swing.filechooser.FileFilter;
import com.mysql.jdbc.PreparedStatement;

/**
 * Created by hexie on 2016/1/13.
 */
public class ConnectionForm extends JFrame {
    private JButton Clear;
    private JTextField tfSubjectContent;
    private JPanel Panel1;
    private JButton SubjectOK;
    private JButton FileOK;
    private JLabel TopLabel;
    private JLabel Subject;
    private JLabel Path;
    private JButton Save;
    private JTextField textField;
    private JLabel 查询结果;
    private JScrollPane scrollPane;
    private JComboBox combo;
    private JLabel label;
    JFileChooser fileChooser = null;
    File file = null;
    File file2;
    public static Connection conn;
    public static PreparedStatement pstmt;
    public static ArrayList res;
    public static Vector<Vector<Object>> data;
    public JTable jt;
    public ExecutorService threadPool;  //线程池，加快多线程的速度
    final int DataBaseNum = 31;//根据数据库的表的数目更改，决定了开的线程的数目
    static List<String> tables = new ArrayList<String>();

    public ConnectionForm() {
        //以下为文件名过滤器
        fileChooser = new JFileChooser();
        MyFileFilter txtFilter = new MyFileFilter(".txt", "txt 文件 (*.txt)");
        MyFileFilter savFilter = new MyFileFilter(".sav", "sav 文件 (*.sav)");
        MyFileFilter docFilter = new MyFileFilter(".doc", "doc 文件 (*.doc)");
        fileChooser.addChoosableFileFilter(txtFilter);
        fileChooser.addChoosableFileFilter(savFilter);
        fileChooser.addChoosableFileFilter(docFilter);

        label = new JLabel(" ", JLabel.CENTER);
        String[] s = {"Subject", "Predicate", "Object"};//表头
        Vector o2 = new Vector();
        Vector o3 = new Vector();
        Vector o4 = new Vector();
        o2.add(s[0]);
        o3.add(s[1]);
        o4.add(s[2]);
        final Vector<Vector> o1 = new Vector<Vector>();
        o1.add(o2);
        o1.add(o3);
        o1.add(o4);
        data = new Vector<Vector<Object>>();//显示数据，用vector<vector>存储

        this.setTitle("查询数据库");
        this.setContentPane(Panel1);
        this.setExtendedState(JFrame.MAXIMIZED_BOTH);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.pack();
        this.setVisible(true);
        Clear.addActionListener(new ActionListener() {  //清空按钮对应的事件监视器
            /**
             * Invoked when an action occurs.
             *
             * @param e
             */
            @Override
            public void actionPerformed(ActionEvent e) {
                ((DefaultTableModel) jt.getModel()).getDataVector().clear();
                ((DefaultTableModel) jt.getModel()).fireTableDataChanged();
                jt.updateUI();
                scrollPane.setViewportView(jt);
            }
        });


        SubjectOK.addActionListener(new ActionListener() {  //输入 subject后的查询按钮对应的事件监视器
                                        /**
                                         * Invoked when an action occurs.
                                         *
                                         * @param e
                                         */
                                        @Override
                                        public void actionPerformed(ActionEvent e) {
                                            fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
                                            fileChooser.setAcceptAllFileFilterUsed(false);
                                            fileChooser.setApproveButtonText("确定");
                                            fileChooser.setDialogTitle("选择输出文件的保存位置");
                                            int result = fileChooser.showSaveDialog(Panel1);
                                            if (result == JFileChooser.APPROVE_OPTION) {  // 如果选择确定
                                                file = fileChooser.getSelectedFile();
                                                if (file.isDirectory()) { //如果保存的是一个目录
                                                    JOptionPane.showMessageDialog(null, "文件名不合法", "Error", JOptionPane.ERROR_MESSAGE);
                                                    return;
                                                }
                                                if (!file.exists()) {
                                                    file = new File(file.getAbsolutePath());
                                                }
                                                //以下实现选择保存文件的类型然后自动加上后缀
                                                MyFileFilter filter = (MyFileFilter) fileChooser.getFileFilter();
                                                if (filter != null) {
                                                    String Ends = filter.getEnds();

                                                    File newfile = null;
                                                    if (file.getAbsolutePath().toUpperCase().endsWith(Ends.toUpperCase())) {
                                                        // 如果文件是以选定扩展名结束的，则使用原名
                                                        newfile = file;
                                                    } else {
                                                        // 否则加上选定的扩展名
                                                        newfile = new File(file.getAbsolutePath() + Ends);
                                                    }
                                                    file = newfile;
                                                }
                                                String SubjectContent = tfSubjectContent.getText().trim();//去掉文本前后空格
                                                threadPool = Executors.newCachedThreadPool();//创建线程池
                                                res = new ArrayList();
                                                String pre = null;
                                                String dir = file.getAbsolutePath();
                                                pre = SubjectContent;
                                                data.clear();
                                                JOptionPane.showMessageDialog(null, "开始查询！", "Success", JOptionPane.PLAIN_MESSAGE);
                                                try {
                                                    FileWriter fw = new FileWriter(dir);//分析结果存储
                                                    BufferedWriter bw = new BufferedWriter(fw);
                                                    CountDownLatch threadsignal = new CountDownLatch(tables.size());//为了在一个subject的所有thread查询完之后再开始下一个subject的查询，不然下一个subject的thread会干扰当前subject
                                                    for (String tblName : tables) {
                                                        String indexname = pre;
                                                        threadPool.execute(new mythread(bw, tblName, indexname, threadsignal));//多线程查询

                                                    }

                                                    threadsignal.await();//等待当前subject的线程都执行完

                                                } catch (IOException ioe) {
                                                    JOptionPane.showMessageDialog(null, "IO错误", "Error", JOptionPane.ERROR_MESSAGE);
                                                } catch (InterruptedException e3) {

                                                }
                                                jt = new JTable(data, o1);//在下方显示成表格形式
                                                scrollPane.setViewportView(jt);

                                                // System.out.println(SubjectContent);
                                                JOptionPane.showMessageDialog(null, "成功!", "Success", JOptionPane.PLAIN_MESSAGE);


                                                //  result.clear();
                                                // pre=br.readLine();


                                            }
                                        }
                                    }

        );

        FileOK.addActionListener(new ActionListener() {//当选择一个输入文件查询时的事件监听器
            /**
             * Invoked when an action occurs.
             *
             * @param e
             */
            @Override
            public void actionPerformed(ActionEvent e) {
                fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
                fileChooser.setDialogTitle("选择输入文件");
                int result = fileChooser.showOpenDialog(Panel1);
                threadPool = Executors.newCachedThreadPool();
                res = new ArrayList();

                if (result == JFileChooser.APPROVE_OPTION) {
                    file = fileChooser.getSelectedFile();
                    if (!file.exists()) {
                        JOptionPane.showMessageDialog(null, "文件不存在", "Error", JOptionPane.ERROR_MESSAGE);
                        return;
                    }
                    if (file.isDirectory())//注意：当输入是文件夹时，输出位置也应该是一个文件夹（一个输入文件对应一个输出文件）
                        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
                    fileChooser.setDialogTitle("选择输出文件的保存位置");
                    int result2 = fileChooser.showSaveDialog(Panel1);
                    if (result2 == JFileChooser.APPROVE_OPTION) {
                        file2 = fileChooser.getSelectedFile();
                        if (file.isDirectory() && file2.isFile()) {
                            JOptionPane.showMessageDialog(null, "当输入是一个文件夹时输出应该是一个文件夹而不应该是文件", "Error", JOptionPane.ERROR_MESSAGE);
                            return;
                        }
                        if (!file2.exists()) {
                            file2 = new File(file2.getAbsolutePath());
                        }
                        String pre = null;
                        FileWriter fw;
                        JOptionPane.showMessageDialog(null, "开始查询！", "Success", JOptionPane.PLAIN_MESSAGE);
                        if (file.isDirectory()) { // 当输入时一个文件夹时
                            String filePath = file.getAbsolutePath();
                            File nfile = new File(filePath);    //读取文件夹
                            String[] filelist = nfile.list();

                            for (int i = 0; i < filelist.length; i++) {
                                try {
                                    FileReader fr = new FileReader(filePath + "\\" + filelist[i]);
                                    BufferedReader br = new BufferedReader(fr);
                                    pre = br.readLine();
                                    String dir;
                                    if (file2.isDirectory()) {
                                        dir = file2.getAbsolutePath();
                                        fw = new FileWriter(dir + "Triple" + filelist[i]);
                                    } else {
                                        fw = new FileWriter(file2, true);
                                    }

                                    //分析结果存储

                                    while (pre != null) {
                                        BufferedWriter bw = new BufferedWriter(fw);
                                        CountDownLatch threadsignal = new CountDownLatch(tables.size());
                                        for (String tblName : tables) {
                                            String indexname = pre;
                                            threadPool.execute(new mythread(bw, tblName, indexname, threadsignal));//多线程查询

                                        }
                                        threadsignal.await();
                                        res.clear();
                                        pre = br.readLine();
                                    }
                                } catch (FileNotFoundException fnf) {
                                    JOptionPane.showMessageDialog(null, "找不到文件", "Error", JOptionPane.ERROR_MESSAGE);
                                } catch (IOException ioe) {
                                    JOptionPane.showMessageDialog(null, "IO错误", "Error", JOptionPane.ERROR_MESSAGE);

                                } catch (InterruptedException ine) {
                                    JOptionPane.showMessageDialog(null, "发生未知错误", "Error", JOptionPane.ERROR_MESSAGE);
                                }
                                System.gc();
                            }
                            JOptionPane.showMessageDialog(null, "成功!", "Success", JOptionPane.PLAIN_MESSAGE);
                            combo.removeAllItems();
                            for (int i = 0; i < filelist.length; i++)
                                combo.addItem(filelist[i]);//以下拉列表框的形式选择不同的输出文件来显示
                        } else {//当输入是文件时
                            try {
                                FileReader fr = new FileReader(file);
                                BufferedReader br = new BufferedReader(fr);
                                pre = br.readLine();
                                String dir;
                                if (file2.isDirectory()) {
                                    dir = file2.getAbsolutePath();
                                    fw = new FileWriter(dir + "Triple" + file.getName());
                                } else {
                                    fw = new FileWriter(file2);
                                }

                                //分析结果存储
                                while (pre != null) {
                                    BufferedWriter bw = new BufferedWriter(fw);
                                    CountDownLatch threadsignal = new CountDownLatch(tables.size());
                                    for (String tblName : tables) {
                                        String indexname = pre;
                                        threadPool.execute(new mythread(bw, tblName, indexname, threadsignal));//多线程查询

                                    }
                                    threadsignal.await();
                                    res.clear();
                                    pre = br.readLine();
                                }
                            } catch (FileNotFoundException fnf) {
                                JOptionPane.showMessageDialog(null, "找不到文件", "Error", JOptionPane.ERROR_MESSAGE);
                            } catch (IOException ioe) {
                                JOptionPane.showMessageDialog(null, "IO错误", "Error", JOptionPane.ERROR_MESSAGE);

                            } catch (InterruptedException ine) {
                                JOptionPane.showMessageDialog(null, "发生未知错误", "Error", JOptionPane.ERROR_MESSAGE);
                            }
                            combo.removeAllItems();
                            combo.addItem(file.getName());
                            jt = new JTable(data, o1);
                            scrollPane.setViewportView(jt);
                            JOptionPane.showMessageDialog(null, "成功!", "Success", JOptionPane.PLAIN_MESSAGE);

                        }
                    }
                }
            }
        });
        combo.addItemListener(new ItemListener() {//当选择下拉列表框时的事件监听器
            /**
             * Invoked when an item has been selected or deselected by the user.
             * The code written for this method performs the operations
             * that need to occur when an item is selected (or deselected).
             *
             * @param e
             */
            @Override
            public void itemStateChanged(ItemEvent e) {
                if (e.getStateChange() == ItemEvent.SELECTED) {
                    String SelectFile = (String) e.getItem();//得到选择的文件
                    //   System.out.println(SelectFile);

                    String path;
                    if (file2.isDirectory())
                        path = file2.getAbsolutePath() + "Triple" + SelectFile;
                    else
                        path = file2.getAbsolutePath();


                    String pre;
                    try {
                        //  System.out.println(path);
                        FileReader fr = new FileReader(path);//读取文件
                        BufferedReader br = new BufferedReader(fr);
                        pre = br.readLine();
                        data.clear();

                        while (pre != null) {
                            //  System.out.println("ok");
                            String[] slist = pre.split(" ");
                            Vector<Object> vs = new Vector<Object>();
                            for (int i = 0; i < slist.length; i++) {
                                vs.add(slist[i]);
                            }
                            data.add(vs);
                            pre = br.readLine();
                        }
                        jt = new JTable(data, o1);
                        scrollPane.setViewportView(jt);
                    } catch (FileNotFoundException fnf) {
                        JOptionPane.showMessageDialog(null, "找不到文件", "Error", JOptionPane.ERROR_MESSAGE);
                    } catch (IOException ioe) {
                        JOptionPane.showMessageDialog(null, "读文件出错", "Error", JOptionPane.ERROR_MESSAGE);
                    }
                }
            }
        });
    }

    public static void main(String[] args) {
        new login();
    }

    {
// GUI initializer generated by IntelliJ IDEA GUI Designer
// >>> IMPORTANT!! <<<
// DO NOT EDIT OR ADD ANY CODE HERE!
        $$$setupUI$$$();
    }

    /**
     * Method generated by IntelliJ IDEA GUI Designer
     * >>> IMPORTANT!! <<<
     * DO NOT edit this method OR call it in your code!
     *
     * @noinspection ALL
     */
    private void $$$setupUI$$$() {
        Panel1 = new JPanel();
        Panel1.setLayout(new com.intellij.uiDesigner.core.GridLayoutManager(7, 13, new Insets(0, 0, 0, 0), -1, -1));
        TopLabel = new JLabel();
        TopLabel.setText("查询数据库");
        Panel1.add(TopLabel, new com.intellij.uiDesigner.core.GridConstraints(0, 0, 1, 13, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_NONE, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        Subject = new JLabel();
        Subject.setText("在文本框中输入查询内容");
        Panel1.add(Subject, new com.intellij.uiDesigner.core.GridConstraints(1, 0, 1, 4, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        Path = new JLabel();
        Path.setText("在右侧选择输入文件（文件夹）的位置并保存");
        Panel1.add(Path, new com.intellij.uiDesigner.core.GridConstraints(2, 0, 1, 11, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        tfSubjectContent = new JTextField();
        tfSubjectContent.setText(" ");
        Panel1.add(tfSubjectContent, new com.intellij.uiDesigner.core.GridConstraints(1, 4, 1, 7, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_WEST, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_SHRINK | com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_WANT_GROW, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, new Dimension(150, -1), null, 0, false));
        SubjectOK = new JButton();
        SubjectOK.setText("查询并保存");
        Panel1.add(SubjectOK, new com.intellij.uiDesigner.core.GridConstraints(1, 11, 1, 2, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_SHRINK | com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_GROW, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        FileOK = new JButton();
        FileOK.setText("选择查询路径并保存");
        Panel1.add(FileOK, new com.intellij.uiDesigner.core.GridConstraints(2, 11, 1, 2, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_SHRINK | com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_GROW, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        scrollPane = new JScrollPane();
        Panel1.add(scrollPane, new com.intellij.uiDesigner.core.GridConstraints(5, 0, 1, 12, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_BOTH, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_SHRINK | com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_WANT_GROW, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_SHRINK | com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_WANT_GROW, null, null, null, 0, false));
        Clear = new JButton();
        Clear.setText("清空");
        Panel1.add(Clear, new com.intellij.uiDesigner.core.GridConstraints(6, 0, 1, 12, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_SHRINK | com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_GROW, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        combo = new JComboBox();
        final DefaultComboBoxModel defaultComboBoxModel1 = new DefaultComboBoxModel();
        combo.setModel(defaultComboBoxModel1);
        combo.setToolTipText("选择文件");
        Panel1.add(combo, new com.intellij.uiDesigner.core.GridConstraints(4, 0, 1, 1, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_WEST, com.intellij.uiDesigner.core.GridConstraints.FILL_HORIZONTAL, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_CAN_GROW, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        查询结果 = new JLabel();
        查询结果.setText(" 查询结果");
        Panel1.add(查询结果, new com.intellij.uiDesigner.core.GridConstraints(3, 6, 2, 1, com.intellij.uiDesigner.core.GridConstraints.ANCHOR_CENTER, com.intellij.uiDesigner.core.GridConstraints.FILL_NONE, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, com.intellij.uiDesigner.core.GridConstraints.SIZEPOLICY_FIXED, null, new Dimension(52, 27), null, 0, false));
    }

    /**
     * @noinspection ALL
     */
    public JComponent $$$getRootComponent$$$() {
        return Panel1;
    }
}
class mythread implements  Runnable {//线程类

    BufferedWriter bw;
    int id;
    String indexname;
    PreparedStatement pstmt;
    ResultSet rset;
    String tblname;
    private CountDownLatch threadsignal;
    private static  String sn=new String();
    public mythread(BufferedWriter bw,String tblname,String indexname,CountDownLatch threadsignal){
        this.bw=bw;
        this.tblname=tblname;
        this.indexname=indexname;
        this.threadsignal=threadsignal;

    }
    public void run() {
        try {
            String sql = "SELECT * from "+tblname+" where subject='<" + indexname + ">'";
            pstmt = (PreparedStatement) ConnectionForm.conn.prepareStatement(sql);
            rset = pstmt.executeQuery();
            while (rset.next()) {
                String sb = rset.getString("Subject") + " " + rset.getString("Predicate") + " " +rset.getString("Object") + "\r"+"\n";
                synchronized (sn) {//线程同步锁
                    if (!ConnectionForm.res.contains(sb)) {
                        ConnectionForm.res.add(sb);
                        Vector<Object> row = new Vector<Object>();
                        row.add( rset.getString("Subject"));
                        row.add(rset.getString("Predicate"));
                        row.add(rset.getString("Object"));
                        ConnectionForm.data.add(row);
                        try {
                            bw.write(sb.toString());
                            bw.flush();// Print col
                        }catch(IOException E){
                            System.out.println("读写错误");
                        }
                    }
                }
            }
        rset.close();
            pstmt.close();
        } catch (SQLException e) {
            System.out.println("MySQL操作错误");
            e.printStackTrace();

        }
        threadsignal.countDown();
    }
}
class MyFileFilter extends FileFilter {

    String ends; // 文件后缀
    String description; // 文件描述文字

    public MyFileFilter(String ends, String description) { // 构造函数
        this.ends = ends; // 设置文件后缀
        this.description = description; // 设置文件描述文字
    }

    @Override
    // 只显示符合扩展名的文件，目录全部显示
    public boolean accept(File file) {
        if (file.isDirectory()) return true;
        String fileName = file.getName();
        if (fileName.toUpperCase().endsWith(this.ends.toUpperCase())) return true;
        return false;
    }

    @Override
    // 返回这个扩展名过滤器的描述
    public String getDescription() {
        return this.description;
    }

    // 返回这个扩展名过滤器的扩展名
    public String getEnds() {
        return this.ends;
    }
}


