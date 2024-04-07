package dslabs.shardkv;

import java.util.*;

import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Application;
import dslabs.framework.Message;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.SingleKeyCommand;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;

import dslabs.paxos.PaxosServer;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosReply;

import dslabs.shardkv.ConfigTimer;
import dslabs.shardkv.ShardMoveCommand;
import dslabs.shardkv.ShardMoveAckCommand;
import dslabs.shardkv.NewConfigCommand;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardMasterResult;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.kvstore.TransactionalKVStore;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;
    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;
    //private Map<Integer, AMOApplication> amoApps;
    private AMOApplication amoApp;
    private ShardConfig currentConfig;
    private ConfigTimer cfgt;
    private Set<Integer> shardsNeeded;
    private Map<Integer, List<Integer>> shardsToMove;
    private boolean inReconfig;
    private List<Command> afterReconfig;

    // Your code here...

    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // Your code here...
        //amoApps = new HashMap<>();
        amoApp = new AMOApplication<>(new TransactionalKVStore(), new HashMap<>());
        shardsNeeded = new HashSet<>();
        shardsToMove = new HashMap<>();
        inReconfig = false;
        cfgt = new ConfigTimer();
    }

    @Override
    public void init() {
        // Your code here...
        paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        PaxosServer paxosServer =
            new PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        //System.out.println("SENDING INITIAL BROADCAST");
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(0), this.address(), -1)));
        this.set(cfgt, ConfigTimer.CONFIG_RETRY_MILLIS);
    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Your code here...
        //System.out.println("LANDED IN SHARDSTOREREQ HANDLER");
        //int shard = this.keyToShard(((SingleKeyCommand) m.command().command()).key());
        //System.out.println("inReconfig: "+inReconfig+" currentConfig == null: "+(currentConfig == null));
        if (!inReconfig && currentConfig != null && currentConfig.groupInfo().containsKey(groupId) && m.configNum() == currentConfig.configNum()){
            // Current Config agrees that this shardstoreserver handles this shard.
            process(m.command(), false);
        } else {
            AMOResult res = new AMOResult(new Error(), address(), -1);
            PaxosReply reply = new PaxosReply(res);
            this.send(reply, sender);
        }
    }

    private void handleShardMove(ShardMove m, Address sender) {
        if (currentConfig != null && m.configNum() == currentConfig.configNum() && shardsNeeded.containsAll(m.command().shards())){
            process(m.command(), false);
        } else if (currentConfig != null && m.configNum() == currentConfig.configNum() && currentConfig.groupInfo().get(groupId).getValue().containsAll(m.command().shards())) {
            ShardMoveAckCommand cmd = new ShardMoveAckCommand(currentConfig.configNum(), groupId);
            ShardMoveAck msg = new ShardMoveAck(currentConfig.configNum(), cmd);
            this.send(msg, sender);
        } else if (currentConfig != null && m.configNum() < currentConfig.configNum()){
            ShardMoveAckCommand cmd = new ShardMoveAckCommand(m.configNum(), groupId);
            ShardMoveAck msg = new ShardMoveAck(m.configNum(), cmd);
            this.send(msg, sender);
        }
    }

    private void handleShardMoveAck(ShardMoveAck m, Address sender) {
        if (currentConfig != null && m.configNum() == currentConfig.configNum() && shardsToMove.keySet().contains(m.command().groupId())){
            process(m.command(), false);
        }
    }

    private void handlePaxosDecision(PaxosDecision m, Address sender) {
        Command cmd = m.command().command();
        if ((cmd instanceof ShardMoveCommand) || (cmd instanceof ShardMoveAckCommand)) {
            process(cmd, true);
        } else if (!inReconfig) {
            if (cmd instanceof NewConfigCommand) {
                process(cmd, true);
            } else {
                process(m.command(), true);
            }
        } else {
            if (cmd instanceof NewConfigCommand) {
                afterReconfig.add(cmd);
            } else {
                afterReconfig.add(m.command());
            }
        }
        
    }

    private void handlePaxosReply(PaxosReply m, Address sender) {
        // This contains replies from ShardMaster that hold new Configs.
        ShardMasterResult res = (ShardMasterResult) m.result().result();
        if(!(res instanceof Error) && !inReconfig) {
            ShardConfig config = (ShardConfig) res;
            if((currentConfig == null && config.configNum() == 0)) { // || config.configNum() == currentConfig.configNum() + 1) {
                NewConfigCommand cmd = new NewConfigCommand(config);
                process(cmd, false);
            }
        }
    }


    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    private synchronized void onConfigTimer(ConfigTimer t) {
        // Your code here...
        int cn = 0;
        if (currentConfig != null){
            cn = currentConfig.configNum() + 1;
        }
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(cn), this.address(), -1)));
        this.set(t, ConfigTimer.CONFIG_RETRY_MILLIS);
    }

    private synchronized void onShardMoveTimer(ShardMoveTimer t) {
        ShardMove m = t.message();
        ShardMoveCommand cmd = m.command();
        int groupToSend = t.receivingGroupId();
        if (currentConfig != null && currentConfig.configNum() == m.configNum() && shardsToMove.containsKey(groupToSend)) {
            broadcast(m, currentConfig.groupInfo().get(groupToSend).getKey());
            this.set(t, ShardMoveTimer.SHARD_MOVE_RETRY_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void process(Command command, boolean replicated) {
        if (command instanceof ShardMoveCommand) {
            processShardMove((ShardMoveCommand) command, replicated);
        } else if (command instanceof ShardMoveAckCommand) {
            processShardMoveAck((ShardMoveAckCommand) command, replicated);
        } else if (command instanceof NewConfigCommand) {
            processNewConfig((NewConfigCommand) command, replicated);
        } else if (command instanceof AMOCommand) {
            processAMOCommand((AMOCommand) command, replicated);
        } 
        
        // else {
        //     LOG.severe("Got unknown command: " + command);
        // }
    }

    private void processAMOCommand(AMOCommand command, boolean replicated) {
        if (!replicated) {
            //System.out.println("ENTERING AMO REPLICATED FALSE");
            PaxosRequest request = new PaxosRequest(command);
            handleMessage(request, paxosAddress);
        } else {
            //System.out.println("ENTERING AMO REPLICATED TRUE");
            // int shard = keyToShard(((SingleKeyCommand) command.command()).key());
            // if (amoApps.containsKey(shard)) {
            //     AMOResult result = amoApps.get(shard).execute(command);
            //     ShardStoreReply reply = new ShardStoreReply(result);
            //     //System.out.println("PROCESSING RES: " + result + "WITH SEQ NUM: " + command.seqID());
            //     this.send(reply, command.clientID());
            // } else {
            //     AMOResult res = new AMOResult(new Error(), address(), -1);
            //     PaxosReply reply = new PaxosReply(res);
            //     this.send(reply, command.clientID());
            // }
            AMOResult res = amoApp.execute(command);
            ShardStoreReply reply = new ShardStoreReply(res);
            this.send(reply, command.clientID());
        }
    }

    private void processShardMove(ShardMoveCommand command, boolean replicated) {
        if (!replicated) {
            // Sequence ID?
           // System.out.println("ENTERING SHARD MOVE REPLICATED FALSE");
            AMOCommand cmd = new AMOCommand(command, address(), -1);
            PaxosRequest request = new PaxosRequest(cmd);
            handleMessage(request, paxosAddress);
        } else {
           // System.out.println("ENTERING SHARD MOVE REPLICATED FALSE, OUTSIDE IF");
            if (currentConfig != null && command.configNum() == currentConfig.configNum() && shardsNeeded.containsAll(command.shards())) {
               // System.out.println("INSIDE SHARD MOVE, REPLICATED TRUE");
                List<Integer> newShards = command.shards();
                List<AMOApplication> newApps = command.amoApps();

                assert newShards.size() == newApps.size();

                for (int i = 0; i < newShards.size(); i++) {
                    //System.out.println("MOVING OVER SHARD " + newShards.get(i) +" CONFIG NUM: " + currentConfig.configNum());
                    //amoApps.put(newShards.get(i), newApps.get(i));
                    shardsNeeded.remove(newShards.get(i));
                }

                ShardMoveAckCommand smac = new ShardMoveAckCommand(currentConfig.configNum(), groupId);
                ShardMoveAck sma = new ShardMoveAck(currentConfig.configNum(), smac);
                broadcast(sma, command.group());
                if (inReconfig && shardsToMove.keySet().size() == 0 && shardsNeeded.size() == 0) {
                    for (Command m : afterReconfig) {
                        process(m, true);
                    }
                    inReconfig = false;
                    afterReconfig = new ArrayList<>();
                }
            }
        }
    }

    private void processShardMoveAck(ShardMoveAckCommand command, boolean replicated) {
        if (!replicated) {
            //System.out.println("ENTERING SHARD MOVE ACK REPLICATED FALSE");

            AMOCommand cmd = new AMOCommand(command, address(), -1);
            PaxosRequest request = new PaxosRequest(cmd);
            handleMessage(request, paxosAddress);
        } else {
           // System.out.println("ENTERING SHARD MOVE ACK REPLICATED FALSE OUTSIDE IF");

            if (currentConfig != null && command.configNum() == currentConfig.configNum() && shardsToMove.keySet().contains(command.groupId())) {
                //System.out.println("ENTERING SHARD MOVE ACK REPLICATED FALSE INSIDE IF");
                shardsToMove.remove(command.groupId());
            }
            if (inReconfig && shardsToMove.keySet().size() == 0 && shardsNeeded.size() == 0) {
                for (Command m : afterReconfig) {
                    process(m, true);
                }
                inReconfig = false;
                afterReconfig = new ArrayList<>();
            }
        }
    }

    private void processNewConfig(NewConfigCommand command, boolean replicated) {
        if (!replicated) {
            //System.out.println("ENTERING NEWCONFIG REPLICATED FALSE");
            int cn = -1;
            if (currentConfig != null){
                cn = currentConfig.configNum();
            }
            AMOCommand cmd = new AMOCommand(command, address(), cn);
            PaxosRequest request = new PaxosRequest(cmd);
            handleMessage(request, paxosAddress);
        } else {
            //System.out.println("REPLICATED = TRUE NEW CONFIG");
            //System.out.println("ENTERING NEWCONFIG REPLICATED TRUE");

            // have to figure out shards needed and shardstomove
            ShardConfig nextConfig = command.shardConfig();
            Set<Integer> nextShards = new HashSet<>();
            if (nextConfig.groupInfo().containsKey(groupId)){
                nextShards = nextConfig.groupInfo().get(groupId).getValue();
            }

            if (currentConfig == null){
                currentConfig = nextConfig;
                // for (int shard : nextShards) {
                //     amoApps.put(shard, new AMOApplication<Application>(new KVStore(), new HashMap<>()));
                // }
                //System.out.println(address()+ " FOUND FIRST CONFIG, CONFIG NUM: " + currentConfig.configNum());
                return;
            } else if (nextConfig.configNum() == currentConfig.configNum() + 1){
                // loop through shards in next config. If they arent in current config add to shardsNeeded
                if (!currentConfig.groupInfo().containsKey(groupId)){
                    shardsNeeded.addAll(nextShards);
                } else {
                    for (int shard : nextShards) {
                        if (!currentConfig.groupInfo().get(groupId).getValue().contains(shard)) {
                            shardsNeeded.add(shard);
                        }
                    }
                }
                
                // loop through shards in current config. If they arent in next config add to shardsToMove
                if (currentConfig.groupInfo().containsKey(groupId)){
                    for (int shard : currentConfig.groupInfo().get(groupId).getValue()) {
                        if (!nextConfig.groupInfo().containsKey(groupId) || !nextConfig.groupInfo().get(groupId).getValue().contains(shard)) {
                            // shard is not in nextConfig so must be moved
                            for (int key : nextConfig.groupInfo().keySet()) {
                                if (nextConfig.groupInfo().get(key).getValue().contains(shard)){
                                    if (!shardsToMove.containsKey(key)){
                                        shardsToMove.put(key, new ArrayList<Integer>());
                                    }
                                    shardsToMove.get(key).add(shard);
                                    break;
                                }
                            }
                        }
                    }
                }

                // send all moves
                for (int key : shardsToMove.keySet()) {
                    List<Integer> shards = shardsToMove.get(key);
                    List<AMOApplication> appsToMove = new ArrayList<>();
                    for (int i = 0; i < shards.size(); i++){
                        //appsToMove.add(amoApps.get(shards.get(i)));
                        //amoApps.remove(shards.get(i));
                    }
                    
                    ShardMoveCommand cmd = new ShardMoveCommand(nextConfig.configNum(), groupId, group, shards, appsToMove);
                    ShardMove msg = new ShardMove(nextConfig.configNum(), cmd);
                    broadcast(msg, nextConfig.groupInfo().get(key).getKey());
                    this.set(new ShardMoveTimer(key, msg), ShardMoveTimer.SHARD_MOVE_RETRY_MILLIS);                
                }

                System.out.println("Address: " + address() + "\n" + "OLD CONFIG: " + currentConfig + "\n" + "NEW CONFIG: " + nextConfig + "\n" + "SHARDS NEEDED: " + shardsNeeded + "\n" + "SHARDS TO SEND: " + shardsToMove + "\n\n");
                currentConfig = nextConfig;

                // If we are waiting for things to arrive or send we are inReconfig
                if (shardsNeeded.size() > 0 || shardsToMove.keySet().size() > 0) {
                    inReconfig = true;
                    afterReconfig = new ArrayList<>();
                }

            }
        }
    }

}
