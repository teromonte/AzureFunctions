package scc;

public class QuestionDAO {
    private String _rid;
    private String _ts;
    private String id;
    private String auction;
    private String user;
    private String text;
    private String reply;


    public QuestionDAO(String id, String auction, String user, String text, String reply) {
        super();
        this.id = id;
        this.auction = auction;
        this.user = user;
        this.text = text;
        this.reply = reply;

    }

    public QuestionDAO() {

    }

    public String get_rid() {
        return _rid;
    }

    public void set_rid(String _rid) {
        this._rid = _rid;
    }

    public String getReply() {
        return reply;
    }

    public void setReply(String reply) {
        this.reply = reply;
    }

    @Override
    public String toString() {
        return "QuestionDAO{" + "_rid='" + _rid + '\'' + ", _ts='" + _ts + '\'' + ", id='" + id + '\'' + ", auction='" + auction + '\'' + ", user='" + user + '\'' + ", text='" + text + '\'' + ", reply='" + reply + '\'' + '}';
    }

    public String get_ts() {
        return _ts;
    }

    public void set_ts(String _ts) {
        this._ts = _ts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAuction() {
        return auction;
    }

    public void setAuction(String auction) {
        this.auction = auction;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
