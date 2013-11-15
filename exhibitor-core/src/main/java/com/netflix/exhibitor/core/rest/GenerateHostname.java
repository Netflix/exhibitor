package com.netflix.exhibitor.core.rest;

public class GenerateHostname {
    private final String suffix;

    public GenerateHostname() {
        String hostname = System.getenv("EC2_PUBLIC_HOSTNAME");
        if (hostname != null) {
            suffix = hostname.substring(hostname.indexOf('.'));
        } else {
            suffix = null;
        }
    }

    public String getHostname(String ipOrHostname) {
        if (suffix == null || ipOrHostname.contains(suffix)) {
            return ipOrHostname;
        } else {
            String[] numbers = ipOrHostname.split("\\.");
            StringBuilder sb = new StringBuilder("ec2");
            for (String n : numbers) {
                sb.append('-').append(n);
            }
            sb.append(suffix);

            return sb.toString();
        }
    }
}
