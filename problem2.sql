CREATE OR REPLACE FUNCTION LIMPIA_REPETIDOS() 
        RETURNS VOID AS $$
        
        DECLARE 
            REP RECORD;
            devolucion TIMESTAMP;
        
            cursor1 CURSOR FOR 
            SELECT DISTINCT usuario, fecha_hora_ret FROM auxi
            GROUP BY usuario, fecha_hora_ret
            HAVING count(usuario) > 1;  
           
        
        BEGIN
            OPEN cursor1;
            LOOP
                FETCH cursor1 INTO REP;
                EXIT WHEN NOT FOUND;
                PERFORM SEGUNDO(REP.usuario, REP.fecha_hora_ret);
            END LOOP;
            CLOSE cursor1;
            
        END;
        
        $$ LANGUAGE PLPGSQL;

        
        CREATE OR REPLACE FUNCTION SEGUNDO
        (myid auxi.usuario%TYPE, my_time auxi.fecha_hora_ret%type) RETURNS VOID AS $$
        
         
        DECLARE
            pointer auxi;
            firstFetch auxi;
            devolucion TIMESTAMP;
            cursor2 CURSOR FOR
            SELECT * FROM auxi
            WHERE usuario = myid and fecha_hora_ret = my_time
            ORDER BY tiempo_uso ASC;            
        
        BEGIN

                OPEN cursor2;

                FETCH cursor2 INTO firstFetch;
                FETCH cursor2 INTO pointer;

                CLOSE cursor2;

                devolucion = crear_fecha_hora_devolucion(pointer.tiempo_uso, pointer.fecha_hora_ret);
                INSERT INTO auxi2 VALUES(pointer.periodo, pointer.usuario, pointer.fecha_hora_ret, pointer.est_origen, pointer.est_destino, devolucion);
       END;
       $$ LANGUAGE PLPGSQL;
       
       
       
       CREATE OR REPLACE FUNCTION agrego_faltantes() 
        RETURNS VOID AS $$
        
        DECLARE

        
        BEGIN
                INSERT INTO auxi2
                SELECT periodo, usuario, fecha_hora_ret, est_origen, est_destino, crear_fecha_hora_devolucion(tiempo_uso, fecha_hora_ret)
                FROM auxi
                WHERE (usuario, fecha_hora_ret) NOT IN (SELECT usuario, fecha_hora_ret FROM auxi2);
          END;
       $$ LANGUAGE PLPGSQL;



        
CREATE OR REPLACE FUNCTION cond2()
RETURNS VOID AS $$
BEGIN  
        PERFORM LIMPIA_REPETIDOS();
        PERFORM agrego_faltantes();   
END;
$$ LANGUAGE PLPGSQL;


DO $$
BEGIN
  PERFORM migracion();
END;
$$
