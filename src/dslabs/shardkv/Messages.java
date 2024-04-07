package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import lombok.Data;

import dslabs.shardkv.ShardMoveCommand;
import dslabs.shardkv.ShardMoveAckCommand;
import dslabs.shardkv.NewConfigCommand;

@Data
final class ShardStoreRequest implements Message {
    // Your code here...
    private final int configNum;
    private final AMOCommand command;
}

@Data
final class ShardStoreReply implements Message {
    // Your code here...
    private final AMOResult result;
}

// Your code here...
@Data
final class ShardMove implements Message {
    // Your code here...
    private final int configNum;
    private final ShardMoveCommand command;
}

@Data
final class ShardMoveAck implements Message {
    // Your code here...
    private final int configNum;
    private final ShardMoveAckCommand command;
}

// @Data
// final class NewConfig implements Message {
//     // Your code here...
//     private final NewConfigCommand command;
// }
