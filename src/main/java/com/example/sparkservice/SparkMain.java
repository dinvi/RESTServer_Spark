/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.sparkservice;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.camel.main.Main;

/**
 *
 * @author daniele
 */
public class SparkMain {
    
    public static void main(String[] args) {                
        
        try {            
            Main main = new Main();                                                
            main.bind("sparkServiceLauncher", new SparkService());       
            main.addRouteBuilder(new SparkRouteBuilder());
            main.run();
        } catch (Exception ex) {
            Logger.getLogger(SparkMain.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
}
