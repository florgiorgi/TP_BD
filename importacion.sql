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
fecha_creacion          TEXT
);

CREATE TABLE auxi
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
tiempo_uso TIME,
PRIMARY KEY(periodo, usuario,fecha_hora_ret,est_origen, est_destino, tiempo_uso));

CREATE TABLE auxi2
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >=
fecha_hora_ret),
PRIMARY KEY(usuario,fecha_hora_ret));

CREATE TABLE recorrido_final
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >=
fecha_hora_ret),
PRIMARY KEY(usuario,fecha_hora_ret));

-- 7) En la terminal que tenemos abierta: PROOF => \copy datos_recorrido FROM '/home/rdella/test1.csv' csv header delimiter ';' NULL 'NULL';
HACER
 SET datestyle = dmy;
 
 
\copy datos_recorrido FROM 'path-archivo-remoto' header delimiter ';' csv;