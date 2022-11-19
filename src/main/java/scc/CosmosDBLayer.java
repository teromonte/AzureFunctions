package scc;

import com.azure.cosmos.*;

import java.util.Locale;

public class CosmosDBLayer {
    public static final String USER_CONTAINER = "users";
    public static final String BID_CONTAINER = "bids";
    public static final String QUESTION_CONTAINER = "questions";
    public static final String AUCTION_CONTAINER = "auctions";
    private static final String CONNECTION_URL = "https://scc222355355.documents.azure.com:443/";
    private static final String DB_KEY = "7MeuNBZBJYNkqB7orse0LBoWQkxZtRsSZsweb6lJ1QoLPb452hN4XjECI2mIfTIVA44Fyga6kAoPYeT8OkIuSQ==";
    private static final String DB_NAME = "scc2223db";
    private static CosmosDBLayer instance;
    private CosmosClient client;
    private CosmosDatabase db;
    private CosmosContainer container;

    private CosmosDBLayer(CosmosClient client) {
        this.client = client;
    }

    public static synchronized CosmosDBLayer getInstance() {

        Locale.setDefault(Locale.US);

        CosmosClient client = new CosmosClientBuilder()
                .endpoint(CONNECTION_URL)
                .key(DB_KEY)
                //.directMode()
                .gatewayMode()
                // replace by .directMode() for better performance
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .contentResponseOnWriteEnabled(true)
                .buildClient();
        instance = new CosmosDBLayer(client);
        return instance;

    }

    public synchronized void init(String containerID) {
        if (db != null)
            if (instance != null)
                return;
        db = client.getDatabase(DB_NAME);
        container = db.getContainer(containerID);
    }

    public CosmosContainer getContainer() {
        return container;
    }

    public void close() {
        client.close();
    }

}