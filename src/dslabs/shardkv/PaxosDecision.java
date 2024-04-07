package dslabs.shardkv;

import dslabs.framework.Message;
import lombok.Data;
import dslabs.atmostonce.AMOCommand;

@Data
public final class PaxosDecision implements Message {
    private final AMOCommand command;
}