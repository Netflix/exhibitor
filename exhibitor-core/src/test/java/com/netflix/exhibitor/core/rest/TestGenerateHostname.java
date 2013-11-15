package com.netflix.exhibitor.core.rest;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestGenerateHostname {
    @Test
    public void test() {
        System.setProperty(
                "EC2_PUBLIC_HOSTNAME",
                "ec2-54-228-141-31.eu-west-1.compute.amazonaws.com");

        GenerateHostname gen = new GenerateHostname();
        assertEquals(
                gen.getHostname("127.0.0.1"),
                "ec2-127-0-0-1.eu-west-1.compute.amazonaws.com");
        assertEquals(
                gen.getHostname("ec2-127-0-0-1.eu-west-1.compute.amazonaws.com"),
                "ec2-127-0-0-1.eu-west-1.compute.amazonaws.com");
    }
}
