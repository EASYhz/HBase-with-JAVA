package org.example;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        /* 싱글 톤 */
        System.out.println("Start");
        String name = "NYC_MANHATTAN_SALES";
        CreateTable.create(name);
    }
}
