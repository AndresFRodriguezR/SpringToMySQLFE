package pe.joedayz.etl.mysql.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import pe.joedayz.etl.mysql.domain.Bar;



public interface BarRepository extends JpaRepository<Bar, Long> {

}