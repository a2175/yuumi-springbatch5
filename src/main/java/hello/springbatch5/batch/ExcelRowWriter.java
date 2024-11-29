package hello.springbatch5.batch;

import hello.springbatch5.entity.BeforeEntity;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.FileOutputStream;
import java.io.IOException;

public class ExcelRowWriter implements ItemStreamWriter<BeforeEntity> {

    private final String filePath;
    private Workbook workbook;
    private Sheet sheet;
    private int currentRowNumber;
    private boolean isClosed;

    public ExcelRowWriter(String filePath) throws IOException {

        this.filePath = filePath;
        this.isClosed = false;
        this.currentRowNumber = 0;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        workbook = new XSSFWorkbook();
        sheet = workbook.createSheet("Sheet1");
    }

    @Override
    public void write(Chunk<? extends BeforeEntity> chunk) {
        for (BeforeEntity entity : chunk) {
            Row row = sheet.createRow(currentRowNumber++);
            row.createCell(0).setCellValue(entity.getUsername());
        }
    }

    @Override
    public void close() throws ItemStreamException {

        if (isClosed) {
            return;
        }

        try (FileOutputStream fileOut = new FileOutputStream(filePath)) {

            ItemStreamException originalException = null;

            try {
                workbook.write(fileOut);
            } catch (IOException e) {
                originalException = new ItemStreamException(e);
                throw originalException;
            } finally {
                try {
                    workbook.close();
                } catch (IOException e) {
                    // 원래 예외가 있으면 suppressed로 추가
                    if (originalException != null) {
                        originalException.addSuppressed(new ItemStreamException(e));
                    } else {
                        // 원래 예외가 없으면 새 예외를 던짐
                        throw new ItemStreamException(e);
                    }
                }
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        } finally {
            isClosed = true;
        }
    }
}
