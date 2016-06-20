/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.sparkservice;

import java.io.IOException;
import java.io.PrintWriter;
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
public class SparkService{

    /*
    Path relativi al modello e ai due dataset (mail classificate come HAM e 
    mail classificate come SPAM). I path sono relativi al filesystem HDFS in
    quanto la richiesta sarà deployata su Hadoop.    
    */
    private static final String PATH_MODEL = "hdfs://localhost:54310/data/myNaiveBayesModel";
    private static final String PATH_HAM_DATA = "hdfs://localhost:54310/data/ham.txt";
    private static final String PATH_SPAM_DATA = "hdfs://localhost:54310/data/spam.txt";

    /*
    Lista di Mail classificate dal Server.
    */
    private final Map<Integer, Mail> mails; 
    
    /*
    Lista di Request (query o aggiornamento del modello) inviate al Server.
    */
    private final Map<Integer, Request> requests;
        
    private int idMail = 0;
    private int idReq = 0;

    private static SparkConf conf;
    private static JavaSparkContext jsc;    
 
    public SparkService() {
        mails = new TreeMap<>();
        requests = new TreeMap<>();                                                 
        conf = new SparkConf()                                
                .setAppName("BizMail")
                //effettuaiamo il deploy su yarn
                .setMaster("yarn-client")
                //settiamo la directory in cui è presente SPARK
                .setSparkHome("/usr/local/src/spark-1.6.1-bin-hadoop2.6/");                                
        jsc = new JavaSparkContext(conf);         
        /*
        All'avvio il server effettuerà l'addestramento del modello        
        */
        train();        
    }    
        
    /**
     * Metodo richiamato dalla rotta "mail/listMail" - GET.
     * Permette la visualizzazione di tutte le email che sono state inviate al
     * server per essere classificate.                     
     * @return Lista di Mail
     */
    public Collection<Mail> listMail() {        
        return mails.values();
    }
   
    /**
     * Metodo richiamato dalla rotta "mail/listReq" - GET.
     * Permette la visualizzazione di tutte le richieste inviate al server. 
     * La richesta può essere o una semplice query per conoscere la 
     * classificazione di una mail o una richiesta di aggiornamento del modello 
     * in seguito ad una classificazione del modello errata.
     * @return Lista di richieste.
     */
    public Collection<Request> listReq() {        
        return requests.values();
    }
    
    
    /**
     * Metodo richiamato dalla rotta "mail/query" - POST.
     * Rotta che permette l'invio di una query al server per conoscere la 
     * classificazione di una mail.
     * @param mail Mail inviata al server da classificate
     * @return Request
     */
    //Da modificare commento
    public Mail queryMail(Mail mail) {        
        Request req = new Request(idReq,"QUERY","RUNNING");        
        HashingTF tf = new HashingTF(10000);
        /*
        La mail viene suddivisa in word eliminando tutti i caratteri di
        spazio. Viene in seguito calcolata la TF - Term Frequency delle 
        words che costituiscono la mail.
         */
        Vector mailTF = tf.transform(Arrays.asList(mail.toString().split(" ")));

        /*
        Viene caricato il modello precedentemente addestrato.
        */
        NaiveBayesModel model = NaiveBayesModel.load(jsc.sc(), "/data/myNaiveBayesModel/");

        /*
        Effettua la previsione della mail. 
        Il modello ritorna il valore:
        0.0 -> HAM
        1.0 -> SPAM 
        */
        double prediction = model.predict(mailTF);        
        if (prediction == 0.0) {
            mail.setClassification("HAM");
        } else {
            mail.setClassification("SPAM");
        }            

        /*
        Viene modificato lo stato, da RUNNING in COMPLETE, della richiesta 
        relativa alla classificazione della seguente mail.
        */            
        req.setState("COMPLETE");
        requests.put(idReq++, req); 
        mails.put(idMail++, mail);
        return mail;
    }

    /**
     * Metodo che permette l'eliminazione della directory in cui è salvato
     * il modello precedentemente addestrato.
     * Il server prima di effettuare un nuovo addestramento eliminerà tale
     * directory.
     * @return True se l'eliminazione viene eseguita con successo; 
     *         False se viene sollevata qualche eccezione.
     */
    public boolean deleteModel(){    
        //Configuration di HADOOP
        Configuration config = new Configuration();
        try {
            //File system HDFS
            FileSystem fs = FileSystem.get(URI.create(PATH_MODEL), config);
            fs.delete(new Path(PATH_MODEL), true);
            fs.close();
        } catch (IOException ex) {
            Logger.getLogger(SparkService.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
    
    /**
     * Metodo che permette l'append della mail classificata erroneamente dal 
     * server nel dataset corretto.
     * I dataset sono due file, uno contenente tutte le mail classificate come
     * HAM, l'altro tutte le mail classificate come SPAM.
     * @param uri Path del dataset 
     * @param mail Testo della mail
     */
    public void appendData(String uri, String mail){                       
        Configuration config = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(URI.create(uri), config);
            FSDataOutputStream fsout = fs.append(new Path(uri));
            PrintWriter writer = new PrintWriter(fsout);
            writer.append(mail + "\n");
            writer.close();
            fs.close();
        } catch (IOException ex) {
            Logger.getLogger(SparkService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Metodo richiamato dalla rotta "mail/updateHam" - POST.
     * Tale metodo permette l'aggiornamento del modello in seguito ad una 
     * classificazione errata. In questo caso il modello ha classificato 
     * erroneamente una mail come SPAM invece di HAM.            
     * @param mail Mail erroneamente classificata.
     * @return Request relativa.
     */
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
    
     /**
     * Metodo richiamato dalla rotta "mail/updateSpam" - POST.
     * Tale metodo permette l'aggiornamento del modello in seguito ad una 
     * classificazione errata. In questo caso il modello ha classificato 
     * erroneamente una mail come HAM invece di SPAM.            
     * @param mail Mail erroneamente classificata.
     * @return Request relativa.
     */
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
    
    /**
     * Metodo che elabora i dataset presenti.
     * Sono presenti due file, uno contenente tutte le mail correttamente
     * etichettate come HAM, l'altro quelle etichettate come SPAM.      
     * @return Ritorna un RDD in cui sono presenti tutte le mail con le 
     * loro relative etichette.
     */
    public static JavaRDD<LabeledPoint> dataset(){
        final HashingTF tf = new HashingTF(10000);         
        /*
        Importiamo il testo delle mail etichettate come HAM
        (testo inteso come Sender, Subject e Text).
        */
        JavaRDD<String> ham = jsc.textFile(PATH_HAM_DATA);
        /*
        Importiamo il testo delle mail etichettate come SPAM
        (testo inteso come Sender, Subject e Text).
        */
        JavaRDD<String> spam = jsc.textFile(PATH_SPAM_DATA);
        
        /*
        Effettua trasformazione testo di ogni singola mail in Hashing TF - Term 
        Frequency dopo aver rimosso gli spazi. La TF rappresenta il numero di 
        volte che il termine appare nel documento.
        Dopo la seguente trasformazione mapparemo le word in base alla label:
        1 - SPAM
        0 - HAM
        */
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
        
        /*
        Uniamo i due insiemi precedentemente ricavati ed elaborati.
        */
        JavaRDD<LabeledPoint> data = spamLabelledTF.union(hamLabelledTF);        
        return data;
    }
   
    /**
     * Metodo richiamato dalla rotta "mail/train" - POST.
     * Tale metodo permette l'addestramento del modello sulla base di un 
     * dataset iniziale di mail correttamente etichettate come SPAM o HAM. 
     * I dataset sono stati importati da SpamAssassin.
     * @return True se l'addestramento viene eseguita con successo; 
     *         False se viene sollevata qualche eccezione.
     */
    public boolean train() {        
        if(deleteModel())
            System.out.println("Model Successfully deleted");
        else
            System.out.println("Model not deleted");

        try{
            JavaRDD<LabeledPoint> data = dataset();        
        
            /*
            Effettua addestramento modello con datasets aggiornati.
            */
            NaiveBayesModel model = NaiveBayes.train(data.rdd(), 1.0, "multinomial");        
            /*
            Una volta completato l'addestramento viene salvato il modello.
            */
            model.save(jsc.sc(), PATH_MODEL);
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
}
