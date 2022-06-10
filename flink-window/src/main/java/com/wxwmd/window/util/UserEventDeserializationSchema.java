package com.wxwmd.window.util;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author wxwmd
 * @description 自定义反序列化器，将读入的字节数组反序列化为UserEvent对象
 */
public class UserEventDeserializationSchema implements DeserializationSchema<UserEvent> {
    @Override
    public UserEvent deserialize(byte[] message) {
        String str = new String(message, UTF_8);
        String[] props = str.split(",");

        UserAction userAction;
        String actionStr = props[1];
        switch (actionStr){
            case "LOGIN":{
                userAction = UserAction.LOGIN;
                break;
            }
            case "BUY":{
                userAction = UserAction.BUY;
                break;
            }
            case "LogOut":{
                userAction = UserAction.LOGOUT;
                break;
            }
            default:{
                userAction=null;
            }
        }
        UserEvent userEvent = new UserEvent(props[0], userAction, Long.parseLong(props[2]));
        return userEvent;
    }

    @Override
    public boolean isEndOfStream(UserEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(UserEvent.class);
    }
}
