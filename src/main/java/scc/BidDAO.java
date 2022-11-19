package scc;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class BidDAO {
    private String _rid;
    private String _ts;
    private String id;
    private String auctionId;
    private String user;
    private float value;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    private Date time;

    public BidDAO(String id, String auctionId, String user, float value, Date time) {
        this.id = id;
        this.auctionId = auctionId;
        this.user = user;
        this.value = value;
        this.time = time;
    }

    public BidDAO() {

    }

    public String get_rid() {
        return _rid;
    }

    public void set_rid(String _rid) {
        this._rid = _rid;
    }

    public String get_ts() {
        return _ts;
    }

    public void set_ts(String _ts) {
        this._ts = _ts;
    }


    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAuction() {
        return auctionId;
    }

    public void setAuction(String asd) {
        this.auctionId = asd;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
