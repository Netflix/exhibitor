package com.netflix.exhibitor.core.rest;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestGenerateHostname {
    @Test
    public void test() {
        GenerateHostname gen = new GenerateHostname("ec2-127-0-0-1.eu-west-1.compute.amazonaws.com");
        assertEquals(
                gen.getHostname("127.0.0.1"),
                "ec2-127-0-0-1.eu-west-1.compute.amazonaws.com");
        assertEquals(
                gen.getHostname("ec2-127-0-0-1.eu-west-1.compute.amazonaws.com"),
                "ec2-127-0-0-1.eu-west-1.compute.amazonaws.com");
    }

    @Test
    public void testNull() {
        GenerateHostname gen = new GenerateHostname(null);
        assertEquals(
                gen.getHostname("127.0.0.1"),
                "127.0.0.1");
        assertEquals(
                gen.getHostname("ec2-127-0-0-1.eu-west-1.compute.amazonaws.com"),
                "ec2-127-0-0-1.eu-west-1.compute.amazonaws.com");
    }
}
