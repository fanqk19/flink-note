package com.wxwmd.util.model;

import lombok.Data;

/**
 * @author wxwmd
 * @description 汽车的实体类
 */
@Data
public class Car {
    // 品牌
    private String brand;

    // 型号
    private String model;

    // 价格
    private double price;

    public Car() {
    }

    public Car(String brand, String model, double price) {
        this.brand = brand;
        this.model = model;
        this.price = price;
    }
}
