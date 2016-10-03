package pe.joedayz.etl.sqlserver.repo;


import org.springframework.data.jpa.repository.JpaRepository;

import pe.joedayz.etl.sqlserver.domain.Foo;



public interface FooRepository extends JpaRepository<Foo, Long> {

}