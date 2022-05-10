package com.gmail.benrcarver.distributed;

/**
 * Configuration file that defines a Follower.
 *
 * This isn't really used right now. I wanted to have the Commander automatically start followers, but
 * I haven't fully implemented that yet. This class would be used once that feature is supported, as the
 * FollowerConfig objects would define the Followers that need to be started by the Commander.
 */
public class FollowerConfig {
    /**
     * The IPv4 of the Follower. Could be public or private.
     */
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
