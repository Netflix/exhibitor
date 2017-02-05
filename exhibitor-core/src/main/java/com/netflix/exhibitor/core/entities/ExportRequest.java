package com.netflix.exhibitor.core.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExportRequest {
    private String startPath;

    public ExportRequest() {
        this("/");
    }

    public ExportRequest(String startPath) {
        this.startPath = startPath;
    }

    public String getStartPath() {
        return startPath;
    }

    public void setStartPath(String startPath) {
        this.startPath = startPath;
    }
}
