package com.gmail.benrcarver.distributed;

public class FollowerConfig {
    private String ip;
    private String user;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return "FollowerConfig(ip=" + ip + ", user=" + user + ")";
    }
}
