/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.sparkservice;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;

/**
 *
 * @author daniele
 */
public class SparkRouteBuilder extends RouteBuilder{
        
    @Override
    public void configure() throws Exception {
        restConfiguration().component("restlet").port(8080)
            .bindingMode(RestBindingMode.json);
        
        rest("/mail").consumes("application/json").produces("application/json")

            /*
            Rotta che permette la visualizzazione di tutte le email
            che sono state inviate al server per essere classificate.            
            */
            .get("/listMail").outTypeList(Mail.class)
                .to("bean:sparkServiceLauncher?method=listMail")
                
            /*
            Rotta che permette la visualizzazione di tutte le richieste
            inviate al server. La richesta può essere o una semplice
            query per conoscere la classificazione di una mail o una richiesta
            di aggiornamento del modello in seguito ad una classificazione
            del modello errata.
            */
            .get("/listReq").outTypeList(Request.class)
                .to("bean:sparkServiceLauncher?method=listReq")
                            
            /*
            Rotta che permette l'invio di una query al server per 
            conoscere la classificazione di una mail.
            */
            .post("/query").type(Mail.class).outType(Request.class)
                .to("bean:sparkServiceLauncher?method=queryMail")
            
            /*
            Rotta che permette l'addestramento del modello sulla base
            di un dataset iniziale di mail già classificate come SPAM
            o HAM. Dataset importati da SpamAssassin.
            */
            .post("/train")
                .to("bean:sparkServiceLauncher?method=train")                
        
            /*
            Rotta che permette l'aggiornamento del modello in seguito
            ad una classificazione errata. In questo caso il modello
            ha classificato erroneamente una mail come SPAM invece di
            HAM.            
            */
            .post("/updateHam").type(Mail.class).outType(Request.class)
                .to("bean:sparkServiceLauncher?method=updateHam")
            
            /*
            Rotta che permette l'aggiornamento del modello in seguito
            ad una classificazione errata. In questo caso il modello
            ha classificato erroneamente una mail come HAM invece di
            SPAM.            
            */
            .post("/updateSpam").type(Mail.class).outType(Request.class)
                .to("bean:sparkServiceLauncher?method=updateSpam")
                                
            /*
            Rotta che permette di conoscere la classificazione di una mail
            grazie all'id della richiesta.
            */
            .get("/{id}").outType(Mail.class)
                .to("bean:sparkServiceLauncher?method=getMailStatus(${header.id})");
            
    }
    
}