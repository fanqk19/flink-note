package com.wxwmd.util.model;

import lombok.Data;

import java.util.Objects;

/**
 * @author wxwmd
 * @description 表示user的一些行为{登录，购物，退出}
 */
@Data
public class UserAction{
    Action action;

    public UserAction(Action action) {
        this.action = action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        UserAction that = (UserAction) o;
        return action == that.action;
    }

    @Override
    public int hashCode() {
        return Objects.hash(action);
    }
}





