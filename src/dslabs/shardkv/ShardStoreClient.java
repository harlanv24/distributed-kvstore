package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.shardmaster.ShardMaster.ShardMasterResult;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import java.util.*;
import dslabs.kvstore.TransactionalKVStore.Transaction;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    // Your code here...
    private Result result;
    private int seqNum;
    //private Set<Address> currentGroup;
    //private int currentGroupID;
    private ShardConfig currentConfig;
    private dslabs.shardkv.ClientTimer ct;
    private dslabs.shardkv.ConfigTimer cfgt;
    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
    }

    @Override
    public synchronized void init() {
        // Your code here...
        //System.out.println("INITIALIZING");
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(0), this.address(), -1)));
        cfgt = new ConfigTimer();
        this.set(cfgt, dslabs.shardkv.ConfigTimer.CONFIG_RETRY_MILLIS);
        //System.out.println("SEQ NUM STARTS AT: " + seqNum);
        //System.out.println("INIT FINISHED");
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        //System.out.println("ENTERED SEND COMMAND FOR CMD: " + command);
        int cn = 0;
        if (currentConfig != null){
            cn = currentConfig.configNum();
        }
        ShardStoreRequest shardStoreRequest;
        if (command instanceof Transaction) {
            shardStoreRequest = new ShardStoreRequest(cn, new AMOCommand((Transaction) command, this.address(), seqNum));
        } else {
            shardStoreRequest = new ShardStoreRequest(cn, new AMOCommand(command, this.address(), seqNum));
        }
        if (currentConfig != null) {
            result = null;

            if (command instanceof Transaction) {
                Transaction trans = (Transaction) command;
                Set<String> keys = trans.keySet();
                broadcast(shardStoreRequest, currentConfig.groupInfo().get(getGroupIdForShard(this.keyToShard(keys.iterator().next()))).getLeft());
            } else {
                int key = getGroupIdForShard(this.keyToShard(((SingleKeyCommand) command).key()));
                //System.out.println("SENT REQUEST: " + shardStoreRequest);
                broadcast(shardStoreRequest, currentConfig.groupInfo().get(key).getLeft());
            }
            
        }
        if (ct == null){
            ct = new dslabs.shardkv.ClientTimer(shardStoreRequest);
            this.set(ct, dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS);
        }
        //System.out.println("EXITING SEND COMMAND");
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return this.result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (!this.hasResult()){
            this.wait();
        }
        return this.result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m,
                                                    Address sender) {
        // Your code here...
        if (m.result().seqID() == seqNum){
            //System.out.println("HANDLING REPLY: " + m + " with SEQ NUM: " + seqNum);
            this.result = m.result().result();
            this.notify();
            seqNum++;
            ct = null;
        }
    }

    // Your code here...
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        ShardMasterResult res = (ShardMasterResult) m.result().result();
        //System.out.println("SHARDMASTER RES: " + res);
        if(!(res instanceof Error)) {
            ShardConfig config = (ShardConfig) res;
            if(currentConfig == null || config.configNum() > currentConfig.configNum()) {
                currentConfig = config;
            }
        }
        else {
            //received error, try again
            broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(0), this.address(), -1)));
        }

    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        //System.out.println("CLIENT TIMER FIRES " + dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS);
        if(t.shardStoreRequest().command().seqID() == seqNum) {
            // ISSUE: CURRENT GROUP MIGHT HAVE CHANGED IN THE MEAN TIME
            if (currentConfig != null){
                Transaction trans = (Transaction) t.shardStoreRequest().command().command();
                Set<String> keys = trans.keySet();
                broadcast(t.shardStoreRequest(), currentConfig.groupInfo().get(getGroupIdForShard(this.keyToShard(keys.iterator().next()))).getLeft());
                
                //int key = getGroupIdForShard(this.keyToShard(((SingleKeyCommand) t.shardStoreRequest().command().command()).key()));
                //System.out.println("RESENDING REQUEST: " + t.shardStoreRequest());
                //broadcast(t.shardStoreRequest(), currentConfig.groupInfo().get(key).getLeft());
            }
            this.set(t, dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }

    private synchronized void onConfigTimer(ConfigTimer t) {
        // Your code here...
        //System.out.println("CONFIG TIMER FIRES " + dslabs.shardkv.ConfigTimer.CONFIG_RETRY_MILLIS);
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(0), this.address(), -1)));
        this.set(t, dslabs.shardkv.ConfigTimer.CONFIG_RETRY_MILLIS);
    }
    

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private int getGroupIdForShard(int shard) {
        for(int key : currentConfig.groupInfo().keySet()) {
            if (currentConfig.groupInfo().get(key).getRight().contains(shard)) {
                return key;
                //this.currentGroupID = key;
                //this.currentGroup = currentConfig.groupInfo().get(key).getLeft();
            }
        }
        return -1;
    }

}
