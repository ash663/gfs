package mserver;

import java.io.Serializable;

public class MutableInt implements Serializable {
    int misses = 0;

    public void setMisses(int value) {
        this.misses = value;
    }

    public int getMisses() {
        return misses;
    }
}
