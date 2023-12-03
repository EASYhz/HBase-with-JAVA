package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateTable {
    /* 싱글톤 */
    private static CreateTable instance;

    private CreateTable() { }

    public static synchronized CreateTable getInstance() {
        if (instance == null) {
            instance = new CreateTable();
        }
        return instance;
    }
    public static void create(String name) throws IOException {
        /* 파일 명 (3년치) */
        List<String> list = Arrays.asList("refined_2020_manhattan", "refined_2021_manhattan", "refined_2022_manhattan");
        /* 컬럼 패밀리 분류 */
        List<String> salesFamily = Arrays.asList("TOTAL UNITS", "YEAR BUILT", "SALE PRICE");

        /* 헤더 초기화 */
        List<String> columnHeaders = initHeader(list.get(0));

        createTable(name, "SALES", "ETC");
        /* HBase 연결 */
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);

        /* HBase 테이블 선택 */
        TableName tableName = TableName.valueOf(name);
        Table table = connection.getTable(tableName);

        System.out.println(columnHeaders);
        Workbook workbook = null;

        /* 엑셀 파일 읽기 */
        for(String l : list) {
            FileInputStream excelFile = new FileInputStream("/Users/ijiyun/Downloads/"+ l +".xlsx");
            workbook = new XSSFWorkbook(excelFile);
            Sheet sheet = workbook.getSheetAt(0);
            /* 각 데이터 값을 Hbase 에 저장 */
            int rowNum = 0;
            for (Row row : sheet) {
                boolean hasColumns = false;
                Put put = new Put(Bytes.toBytes(Integer.toString(rowNum))); // row_key 설정
                for (Cell cell : row) {
                    if (cell.getRowIndex() == 0) continue;
                    String header = columnHeaders.get(cell.getColumnIndex());
                    String family = "ETC";
                    if (salesFamily.contains(header)) {
                        family = "SALES";
                    }
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(header), getValueAsBytes(cell));
                    hasColumns = true;
                }
                rowNum++;
                if (hasColumns) {
                    table.put(put);
                }
            }
        }

        scanTableData(table);

        /* 리소스 정리 */
        workbook.close();
        table.close();
        connection.close();
    }

    /**
     * 헤더 초기화
     * @param table
     * @return
     * @throws IOException
     */
    private static List<String> initHeader(String table) throws IOException {
        FileInputStream excelFile = new FileInputStream("/Users/ijiyun/Downloads/"+ table +".xlsx");
        Workbook workbook = new XSSFWorkbook(excelFile);
        Sheet sheet = workbook.getSheetAt(0);
        List<String> columnHeaders = new ArrayList<>();

        for(Row row: sheet) {
            for (Cell cell: row) {
                if (cell.getRowIndex() == 0) {
                    columnHeaders.add(cell.getStringCellValue().replace("\n", ""));
                }
            }
        }
        workbook.close();

        return columnHeaders;
    }

    /**
     * 테이블 생성
     * @param tableName
     * @param columnFamilies
     */
    public static void createTable(String tableName, String... columnFamilies) {
        try {
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);

            Admin admin = connection.getAdmin();

            TableName table = TableName.valueOf(tableName);
            if (!admin.tableExists(table)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(table);

                for (String columnFamily : columnFamilies) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(columnFamily));
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }

                admin.createTable(hTableDescriptor);
                System.out.println("Table created: " + tableName);
            } else {
                System.out.println("Table already exists: " + tableName);
            }
            admin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 셀 타입 확인
     * @param cell
     * @return
     */
    private static byte[] getValueAsBytes(Cell cell) {
        return switch (cell.getCellType()) {
            case NUMERIC -> Bytes.toBytes(Double.toString(cell.getNumericCellValue()));
            case STRING -> Bytes.toBytes(cell.getStringCellValue());
            case BOOLEAN -> Bytes.toBytes(Boolean.toString(cell.getBooleanCellValue()));
            // 빈 값 처리
            default -> Bytes.toBytes("");
        };
    }

    /***
     *  테이블 확인
     * @param table
     * @throws IOException
     */
    static void scanTableData(Table table) throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            printResult(result);
        }

        scanner.close();
    }

    /**
     * 결과 출력
     * @param result
     */
    private static void printResult(Result result) {
        for (org.apache.hadoop.hbase.Cell cell : result.listCells()) {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println("Row: " + row + ", Family: " + family + ", Qualifier: " + qualifier + ", Value: " + value);
        }
    }
}
