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

            .get("/listMail").outTypeList(Mail.class)
                .to("bean:sparkServiceLauncher?method=listMail")
                
            .get("/listReq").outTypeList(Request.class)
                .to("bean:sparkServiceLauncher?method=listReq")
                            
            .post("/query").type(Mail.class).outType(Request.class)
                .to("bean:sparkServiceLauncher?method=queryMail")
            
            .post("/train")
                .to("bean:sparkServiceLauncher?method=train")                
        
            .post("/updateHam").type(Mail.class).outType(Request.class)
                .to("bean:sparkServiceLauncher?method=updateHam")
                
            .post("/updateSpam").type(Mail.class).outType(Request.class)
                .to("bean:sparkServiceLauncher?method=updateSpam")
                                
            .get("/{id}").outType(Mail.class)
                .to("bean:sparkServiceLauncher?method=getMailStatus(${header.id})");
            
    }
    
}
