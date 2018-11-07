package mserver;

import java.io.Serializable;

public class MutableLong implements Serializable {
    private long lastBeat = 0;

    public void setLong(long time) {
        this.lastBeat = time;
    }

    public long getLong() {
        return lastBeat;
    }

    MutableLong(long time) {
        this.lastBeat = time;
    }

    MutableLong() {}
}
