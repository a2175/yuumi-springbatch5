package hello.springbatch5.repository;

import hello.springbatch5.entity.AfterEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AfterRepository extends JpaRepository<AfterEntity, Long> {
}
