package scc.serverless;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import scc.AuctionDAO;
import scc.CosmosDBLayer;

import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * Azure Functions with Timer Trigger.
 */
public class TimerFunction {

    private CosmosDBLayer getContainer() {
        CosmosDBLayer db = CosmosDBLayer.getInstance();
        db.init(CosmosDBLayer.AUCTION_CONTAINER);
        return db;
    }

    @FunctionName("periodic-compute")
    public void cosmosFunction(@TimerTrigger(name = "periodicSetTime", schedule = "30 */1 * * * *") String timerInfo, ExecutionContext context) {
        String open = "OPEN";
        List<AuctionDAO> res = getContainer().getContainer().queryItems("SELECT * FROM auctions WHERE auctions.auctions=\"" + open + "\"", new CosmosQueryRequestOptions(), AuctionDAO.class).stream().toList();
        for (AuctionDAO a : res) {
            if (a.getEndTime().after(Date.from(Instant.now()))) {
                a.setStatus("CLOSED");
                PartitionKey key = new PartitionKey(a.getId());
                CosmosItemResponse<AuctionDAO> res1 = getContainer().getContainer().replaceItem(a, a.getId(), key, new CosmosItemRequestOptions());
            }
        }
    }
}
