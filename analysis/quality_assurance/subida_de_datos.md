# Subida de datos de secuenciación

<!-- TOC -->
* [Subida de datos de secuenciación](#subida-de-datos-de-secuenciación)
* [Listado de pruebas a realizar en la demostración a cliente](#listado-de-pruebas-a-realizar-en-la-demostración-a-cliente)
  * [Subida de datos a la landing zone](#subida-de-datos-a-la-landing-zone)
  * [Catalogación inicial de ficheros, runs y carpetas de resultados en la plataforma](#catalogación-inicial-de-ficheros-runs-y-carpetas-de-resultados-en-la-plataforma)
  * [Asociación de ficheros a peticiones](#asociación-de-ficheros-a-peticiones)
  * [Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake](#movimiento-de-ficheros-de-landing-zone-a-ubicación-definitiva-en-thealthlake)
  * [Creación de caso clínico, pacientes/individuos, muestras](#creación-de-caso-clínico-pacientesindividuos-muestras)
  * [Errores en la identificación de la muestra asociada a un paciente y petición](#errores-en-la-identificación-de-la-muestra-asociada-a-un-paciente-y-petición)
<!-- TOC -->

# Listado de pruebas a realizar en la demostración a cliente

DUDAS:

- ¿Se quiere mostrar un caso de subida de run de secuenciación con dato bruto y otro de subida de carpeta de resultados de secundario
  generado por Variant Interpreter de Illumina (empleado con muestras TruSight)?
- ¿Se quieren mostrar errores en la asociación de muestras a peticiones (por ejemplo, muestra no encontrada en catálogo)?

## Subida de datos a la landing zone

Detalles de la prueba:

1. Se copiará una carpeta (`{run_id}`) de dato bruto en la carpeta de red compartida donde se copian los runs de secuenciación (`\\Dc1gpronas007\MISEQ_PRE\MiSeq`).
  Se creará el fichero COMPLETED para simular la finalización de la subida.
2. Se verificará que el sistema detecta la subida del run y comienza el procesamiento, desencadenando mensajería en Kafka y ejecutando pipeline de Nifi.
3. Una vez finalizada la transferencia, se comprobará que los ficheros se copian correctamente a la landing zone en S3 (`agent / {agent_id} / {run_id} /`).
4. Se mostrarán los registros en Kafka y se verificará que los eventos contienen la información correcta (metadatos del run, ficheros, etc.).

## Catalogación inicial de ficheros, runs y carpetas de resultados en la plataforma

Detalles de la prueba:

1. Se verificará que el sistema consume los eventos de subida de ficheros desde Kafka, desencadenando el proceso de catalogación (pipeline de Nifi).
2. Se comprobará que se crea la estructura de carpetas en catálogo correspondiente a la ruta del fichero en THealthLake S3.
3. Se mostrarán en la aplicación de catálogo de TSuPreMe los ficheros y carpetas creados, verificando que la información es correcta (nombres, rutas, metadatos).
4. Se mostrarán evidencias de la asociación de los ficheros subidos a los identificadores de muestra obtenidos a partir de los nombres de los ficheros.

## Asociación de ficheros a peticiones

Detalles de la prueba:

1. Se accederá a TPI Request y se creará una nueva petición de forma manual.
2. Se asociarán los ficheros a la petición, comprobando que el sistema realiza consulta al catálogo y muestra aquellos ficheros
  que se han asociado a la muestra en la subida.
3. Se seleccionarán los ficheros para que queden asociados a la muestra incluida en la petición.

## Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake

Detalles de la prueba:

1. Se verificará que el sistema mueve los ficheros desde la landing zone a la ubicación definitiva en THealthLake S3, con la estructura de carpetas
  esperada (`data/sample/{sample_id}/rawdata/`).
2. Se comprobará que los ficheros se copian correctamente a la ubicación definitiva, manteniendo la estructura de carpetas.
3. Se mostrarán evidencias de que los ficheros se encuentran en la ubicación definitiva y que los ficheros se eliminan de
  la landing zone tras el movimiento.
4. Se mostrará la estructura de carpetas del catálogo en TSuPreMe, verificando que los ficheros están correctamente organizados bajo la muestra correspondiente.

## Creación de caso clínico, pacientes/individuos, muestras

Detalles de la prueba:

1. Se verificará la creación del caso clínico, el paciente y la muestra en catálogo, accediendo a la aplicación de catálogo de TSuPreMe.
2. Se comprobará que la muestra creada tiene los metadatos correctos y que está asociada al paciente y al caso clínico correspondiente.

## Errores en la identificación de la muestra asociada a un paciente y petición

Detalles de la prueba: ????