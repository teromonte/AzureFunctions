package scc;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class AuctionDAO {
    private String _rid;
    private String _ts;
    private String id;
    private String title;
    private String description;
    private String image;
    private String owner;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    private Date endTime;
    private float minimumPrice;
    private BidDAO bid;
    private String status;

    public AuctionDAO(String id, String title, String description, String image, String owner, Date endTime, float minimumPrice, BidDAO bid, String status) {
        this.id = id;
        this.title = title;
        this.description = description;
        this.image = image;
        this.owner = owner;
        this.endTime = endTime;
        this.minimumPrice = minimumPrice;
        this.bid = bid;
        this.status = status;
    }

    public AuctionDAO() {

    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "AuctionDAO{" + "id='" + id + '\'' + ", title='" + title + '\'' + ", description='" + description + '\'' + ", image='" + image + '\'' + ", owner='" + owner + '\'' + ", endTime=" + endTime + ", minimumPrice=" + minimumPrice + ", bid=" + bid + '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public float getMinimumPrice() {
        return minimumPrice;
    }

    public void setMinimumPrice(float minimumPrice) {
        this.minimumPrice = minimumPrice;
    }

    public BidDAO getBid() {
        return bid;
    }

    public void setBid(BidDAO bid) {
        this.bid = bid;
    }

}
