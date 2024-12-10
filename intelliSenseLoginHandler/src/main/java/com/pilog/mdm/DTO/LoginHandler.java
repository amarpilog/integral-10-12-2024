/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DTO;

/**
 *
 * 
 */
public class LoginHandler {

    private String rsUsername;
    private String rsPassword;
    private String ssAutoStart;
    private String language;
    private String otp;

    public String getSsAutoStart() {
        return ssAutoStart;
    }

    public void setSsAutoStart(String ssAutoStart) {
        this.ssAutoStart = ssAutoStart;
    }

    public String getRsUsername() {
        return rsUsername;
    }

    public void setRsUsername(String rsUsername) {
        this.rsUsername = rsUsername;
    }

    public String getRsPassword() {
        return rsPassword;
    }

    public void setRsPassword(String rsPassword) {
        this.rsPassword = rsPassword;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getOtp() {
        return otp;
    }

    public void setOtp(String otp) {
        this.otp = otp;
    }
    
    

}
