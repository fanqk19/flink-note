package com.wxwmd.util.model;

import lombok.Data;

/**
 * @author wxwmd
 * @description 用户事件，即某个用户在某时做了某事
 */
@Data
public class UserEvent {
    String userName;
    UserAction action;
    Long timeStamp;

    public UserEvent() {
    }

    public UserEvent(String userName, UserAction action, Long timeStamp) {
        this.userName = userName;
        this.action = action;
        this.timeStamp = timeStamp;
    }
}
