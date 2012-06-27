package com.netflix.exhibitor.core.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class FieldValue
{
    private String      field;
    private String      value;

    public FieldValue()
    {
        this("", "");
    }

    public FieldValue(String field, String value)
    {
        this.field = field;
        this.value = value;
    }

    public String getField()
    {
        return field;
    }

    public void setField(String field)
    {
        this.field = field;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }
}
