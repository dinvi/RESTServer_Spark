/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.sparkservice;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 *
 * @author daniele
 */
public class SparkService implements Serializable{

    private static final String PATH_MODEL = "hdfs://localhost:54310/data/myNaiveBayesModel";
    private static final String PATH_HAM_DATA = "hdfs://localhost:54310/data/ham.txt";
    private static final String PATH_SPAM_DATA = "hdfs://localhost:54310/data/spam.txt";

    private final Map<Integer, Mail> mails; 
    private final Map<Integer, Request> requests;
    private int idMail = 0;
    private int idReq = 0;

    private static SparkConf conf;
    private static JavaSparkContext jsc;    
 
    public SparkService() {
        mails = new TreeMap<>();
        requests = new TreeMap<>();                                 
        conf = new SparkConf()                                
                .setAppName("FilterMail")
                .setMaster("yarn-client")
                .setSparkHome("/usr/local/src/spark-1.6.1-bin-hadoop2.6/");                                
        jsc = new JavaSparkContext(conf);         
        train();
        System.out.println("Server is ready...");
    }    
    
    public Collection<Mail> listMail() {        
        return mails.values();
    }
    
    public Collection<Request> listReq() {        
        return requests.values();
    }
    
    public Request queryMail(Mail mail) {        
        Request req = new Request(idReq,"UPDATE MODEL","RUNNING");
        requests.put(idReq++, req);         
        new QueryMail(mail).start();
        return req;
    }
    
    public Mail getMailStatus(String id) {        
        Mail mail = mails.get(Integer.parseInt(id));
        return mail;
    }
    
    class QueryMail extends Thread{
        Mail mail;        
        public QueryMail(Mail mail){
            this.mail = mail;
        }        
        @Override
        public void run() {            
            mail.setId(idMail);
            HashingTF tf = new HashingTF(10000);
            Vector mailTF = tf.transform(Arrays.asList(mail.toString().split(" ")));
            NaiveBayesModel model = NaiveBayesModel.load(jsc.sc(), "/data/myNaiveBayesModel/");
            double prediction = model.predict(mailTF);        
            if (prediction == 0.0) {
                mail.setClassification("HAM");
            } else {
                mail.setClassification("SPAM");
            }            
            Request req = requests.get(idMail);
            req.setState("COMPLETE");
            requests.put(idMail, req);
            mails.put(idMail++, mail);
        }        
    }
    
    
    public boolean deleteModel(){        
        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(PATH_MODEL), config);
            fs.delete(new Path(PATH_MODEL), true);
            fs.close();
        } catch (IOException ex) {
            Logger.getLogger(SparkService.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
    
    public void appendData(String uri, String mail){                       
        Configuration config = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(URI.create(uri), config);
            FSDataOutputStream fsout = fs.append(new Path(uri));
            PrintWriter writer = new PrintWriter(fsout);
            writer.append("\n" + mail);
            writer.close();
            fs.close();
        } catch (IOException ex) {
            Logger.getLogger(SparkService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Request updateHam(Mail mail) {
        Request req = new Request(idReq,"UPDATE MODEL");               
        appendData(PATH_HAM_DATA, mail.toString());
        if(train())
            req.setState("SUCCESS");
        else
            req.setState("FAILED");
        requests.put(idReq++, req);
        return req;
    }
    
    public Request updateSpam(Mail mail) {        
        Request req = new Request(idReq,"UPDATE MODEL");        
        appendData(PATH_SPAM_DATA, mail.toString());
        if(train())
            req.setState("SUCCESS");
        else
            req.setState("FAILED");
        requests.put(idReq++, req);
        return req;
    }
    
    public static JavaRDD<LabeledPoint> dataset(){
        final HashingTF tf = new HashingTF(10000); 
        JavaRDD<String> ham = jsc.textFile(PATH_HAM_DATA);
        JavaRDD<String> spam = jsc.textFile(PATH_SPAM_DATA);
        JavaRDD<LabeledPoint> hamLabelledTF = ham.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) {
                return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> spamLabelledTF = spam.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) {
                return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> data = spamLabelledTF.union(hamLabelledTF);        
        return data;
    }
    
    public boolean train() {        
        if(deleteModel())
            System.out.println("Model Successfully deleted");
        else
            System.out.println("Model not deleted");

        try{
            JavaRDD<LabeledPoint> data = dataset();        
        
            NaiveBayesModel model = NaiveBayes.train(data.rdd(), 1.0, "multinomial");        
            model.save(jsc.sc(), PATH_MODEL);
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    /** METHOD NEED TO LOCAL DEPLOY **/
    /*
    public Request updateHam(Mail mail) {
        Request req = new Request(idReq,"UPDATE MODEL");
        System.out.println("Rotta Update Ham");                
        appendData(PATH_HAM_DATA, mail.toString());
        if(train())
            req.setState("SUCCESS");
        else
            req.setState("FAILED");
        requests.put(idReq++, req);
        return req;
    }

    public Request updateSpam(Mail mail) {        
        Request req = new Request(idReq++,"UPDATE MODEL");
        System.out.println("Rotta Update Spam");        
        appendData(PATH_SPAM_DATA, mail.toString());
        if(train())
            req.setState("SUCCESS");
        else
            req.setState("FAILED");
        requests.put(idReq++, req);
        return req;
    }

    public boolean train() {
        System.out.println("Rotta Training Model");
        File dir_model = new File(PATH_MODEL);
        if(deleteModel(dir_model))
            System.out.println("Model Successfully deleted");
        else
            System.out.println("Model not deleted");

        try{
            JavaRDD<LabeledPoint> data = dataset();        
        
            NaiveBayesModel model = NaiveBayes.train(data.rdd(), 1.0, "multinomial");        
            model.save(jsc.sc(), PATH_MODEL);
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void appendData(String path, String mail) {
        FileWriter fw = null;
        try {
            File data = new File(path);
            fw = new FileWriter(data, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            out.println(mail);
            out.close();
        } catch (IOException ex) {
            Logger.getLogger(SparkService.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                fw.close();
            } catch (IOException ex) {
                Logger.getLogger(SparkService.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public boolean deleteModel(File model) {
        if (model.isDirectory()) {
            String[] children = model.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteModel(new File(model, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return model.delete();
    }
    
    public static JavaRDD<LabeledPoint> dataset(){
        final HashingTF tf = new HashingTF(10000); 
        JavaRDD<String> ham = jsc.textFile("/home/hduser/BizProject/ham.txt");
        JavaRDD<String> spam = jsc.textFile("/home/hduser/BizProject/spam.txt");
        JavaRDD<LabeledPoint> hamLabelledTF = ham.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) {
                return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> spamLabelledTF = spam.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) {
                return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> data = spamLabelledTF.union(hamLabelledTF);        
        return data;
    }
    */
    
}
