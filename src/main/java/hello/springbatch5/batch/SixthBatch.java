package hello.springbatch5.batch;

import hello.springbatch5.entity.AfterEntity;
import hello.springbatch5.entity.BeforeEntity;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

@Configuration
public class SixthBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final DataSource dataDBSource;

    private final JobExecutionTimeListener jobExecutionTimeListener;

    public SixthBatch(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("dataDBSource") DataSource dataDBSource,
            JobExecutionTimeListener jobExecutionTimeListener
    ) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.dataDBSource = dataDBSource;
        this.jobExecutionTimeListener = jobExecutionTimeListener;
    }

    @Bean
    public Job sixthJob() {

        System.out.println("sixthJob");

        return new JobBuilder("sixthJob", jobRepository)
                .start(sixthStep())
                .listener(jobExecutionTimeListener)
                .build();
    }

    @Bean
    public Step sixthStep() {

        System.out.println("sixthStep");

        return new StepBuilder("sixthStep", jobRepository)
                .<BeforeEntity, AfterEntity> chunk(10, platformTransactionManager)
                .reader(beforeSixthReader())
                .processor(sixthProcessor())
                .writer(afterSixthWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader<BeforeEntity> beforeSixthReader() {

        return new JdbcPagingItemReaderBuilder<BeforeEntity>()
                .name("beforeSixthReader")
                .dataSource(dataDBSource)
                .selectClause("SELECT id, username")
                .fromClause("FROM BeforeEntity")
                .sortKeys(Map.of("id", Order.ASCENDING))
                .rowMapper(beforeEntityRowMapper())
                .pageSize(10)
                .build();
    }

    @Bean
    public ItemProcessor<BeforeEntity, AfterEntity> sixthProcessor() {

        return new ItemProcessor<BeforeEntity, AfterEntity>() {

            @Override
            public AfterEntity process(BeforeEntity item) throws Exception {

                AfterEntity afterEntity = new AfterEntity();
                afterEntity.setUsername(item.getUsername());

                return afterEntity;
            }
        };
    }

    @Bean
    public JdbcBatchItemWriter<AfterEntity> afterSixthWriter() {

        String sql = "INSERT INTO AfterEntity (username) VALUES (:username)";

        return new JdbcBatchItemWriterBuilder<AfterEntity>()
                .dataSource(dataDBSource)
                .sql(sql)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

    private RowMapper<BeforeEntity> beforeEntityRowMapper() {
        return (rs, rowNum) -> {
            BeforeEntity beforeEntity = new BeforeEntity();
            beforeEntity.setId(rs.getLong("id"));
            beforeEntity.setUsername(rs.getString("username"));
            return beforeEntity;
        };
    }
}
