package dslabs.shardkv;

import dslabs.framework.Timer;
import dslabs.paxos.PaxosRequest;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 50;

    // Your code here...
    private final ShardStoreRequest shardStoreRequest;
}

// Your code here...
@Data
final class ConfigTimer implements Timer {
    static final int CONFIG_RETRY_MILLIS = 50;
}

@Data
final class ShardMoveTimer implements Timer {
    static final int SHARD_MOVE_RETRY_MILLIS = 100;
    private final int receivingGroupId;
    private final ShardMove message;
}