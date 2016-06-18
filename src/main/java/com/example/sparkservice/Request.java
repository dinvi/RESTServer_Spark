/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.sparkservice;

/**
 *
 * @author daniele
 */
public class Request {
    
    private int id;
    private String type;
    private String state;

    public Request() {
    }    
     
    public Request(int id, String type) {
        this.id = id;
        this.type = type;                
    }
    
    public Request(int id, String type, String state) {
        this.id = id;
        this.type = type;        
        this.state = state;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
    
}