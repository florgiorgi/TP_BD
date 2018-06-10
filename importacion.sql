-- Importación --

-- Guia para importar a una tabla auxiliar los datos que estan en los csv --

-- 1) Abrir DBVisualizer
-- 2) Descargar los archivos csv de Campus
-- 3) Mandar los archivos csv descargados a pampero. En Windows usar WinSCP. En Linux ejecutar el comando: 
-- scp 'path-archivo-localhost' usuario@pampero.itba.edu.ar:path-archivo-remoto 
-- Ejemplo: scp '/home/rocio/Descargas/test2.csv' rdella@pampero.itba.edu.ar:/home/rdella
-- 4) Conectarse a la base de datos de pampero: ssh usuario@pampero.itba.edu.ar -L 5432:bd1.it.itba.edu.ar:5432
-- 5) Una vez en la terminal de pampero, ejecutar psql -h bd1.it.itba.edu.ar -U usuario PROOF
-- 6) En SQL Commander (DBVisualizer) y correrlo:

SET datestyle = "ISO, DMY";

CREATE TABLE datos_recorrido
(periodo                TEXT,
id_usuario              INTEGER,
fecha_hora_retiro       TEXT,
origen_estacion         INTEGER,
nombre_origen           TEXT,
destino_estacion        INTEGER,
nombre_destino          TEXT,
tiempo_uso              TEXT,
fecha_creacion          DATE,
PRIMARY KEY(id_usuario,fecha_hora_retiro, tiempo_uso));

-- 7) En la terminal que tenemos abierta: PROOF => \copy datos_recorrido FROM '/home/rdella/test1.csv' csv header delimiter ';' NULL 'NULL';

\copy datos_recorrido FROM 'path-archivo-remoto' csv header delimiter ';' NULL 'NULL';
