# Orquestación de flujos y procesos en TSuPreMe

Documento del Departamento de Genómica
Fecha de última modificación: 13/1/2026

<!-- TOC -->
* [Orquestación de flujos y procesos en TSuPreMe](#orquestación-de-flujos-y-procesos-en-tsupreme)
* [Introducción](#introducción)
* [Objetivo](#objetivo)
* [Visión general de procesos a orquestar](#visión-general-de-procesos-a-orquestar)
* [Descripción funcional de procesos de orquestación](#descripción-funcional-de-procesos-de-orquestación)
  * [Registro de peticiones](#registro-de-peticiones)
  * [Subida de datos a la landing zone / antesala](#subida-de-datos-a-la-landing-zone--antesala)
    * [Descripción del proceso](#descripción-del-proceso)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse)
  * [Catalogación inicial de ficheros, runs y carpetas de resultados en la plataforma](#catalogación-inicial-de-ficheros-runs-y-carpetas-de-resultados-en-la-plataforma)
    * [Descripción del proceso](#descripción-del-proceso-1)
    * [Detalle de acciones](#detalle-de-acciones)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-1)
  * [Asociación de ficheros a peticiones](#asociación-de-ficheros-a-peticiones)
    * [Descripción del proceso](#descripción-del-proceso-2)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-2)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-1)
  * [Transformación de resultados provenientes de Nasertic](#transformación-de-resultados-provenientes-de-nasertic)
    * [Descripción del proceso](#descripción-del-proceso-3)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-3)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-2)
  * [Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake](#movimiento-de-ficheros-de-landing-zone-a-ubicación-definitiva-en-thealthlake)
    * [Descripción del proceso](#descripción-del-proceso-4)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-4)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-3)
  * [Creación de caso clínico, pacientes/individuos, muestras](#creación-de-caso-clínico-pacientesindividuos-muestras)
    * [Descripción del proceso](#descripción-del-proceso-5)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-5)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-4)
  * [Ejecución de análisis secundario](#ejecución-de-análisis-secundario)
    * [Descripción del proceso](#descripción-del-proceso-6)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-6)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-5)
  * [Indexado de variantes, anotación, estadísticas e índices](#indexado-de-variantes-anotación-estadísticas-e-índices)
    * [Descripción del proceso](#descripción-del-proceso-7)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-7)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-6)
  * [Publicación de hallazgos e informe](#publicación-de-hallazgos-e-informe)
    * [Descripción del proceso](#descripción-del-proceso-8)
    * [Componente que realiza la tarea](#componente-que-realiza-la-tarea-8)
    * [Procesos que deben desencadenarse](#procesos-que-deben-desencadenarse-7)
* [Alcance para entrega de Enero de](#alcance-para-entrega-de-enero-de)
* [Anexos](#anexos)
  * [Orígenes de datos para subida](#orígenes-de-datos-para-subida)
  * [Ejemplo de fichero sample sheet](#ejemplo-de-fichero-sample-sheet)
  * [Propuesta de estructura de carpetas y ficheros para la información del servicio de genética](#propuesta-de-estructura-de-carpetas-y-ficheros-para-la-información-del-servicio-de-genética)
<!-- TOC -->

# Introducción

La Suite para Medicina Personalizada de Precisión y Genómica de T-Systems, (TSuPreMe), ofrece una
solución corporativa para gestionar todo el ciclo de vida de los análisis de los estudios genómicos y de otras
ciencias ómicas. La plataforma ha sido diseñada de manera modular, lo que garantiza un control total sobre
su funcionalidad y su capacidad para adaptarse y evolucionar mediante la incorporación de futuros servicios
o módulos que puedan dar respuesta a necesidades futuras, minimizando el impacto que pueda producir a
las organizaciones sobre la soberanía y propiedad de los datos ómicos.

La solución de T-Systems está basada en desarrollos propios, así como en diversas funcionalidades de
componentes OpenSource de soluciones de terceros (OpenCB, Nextflow, nf-core, CSVS, GenomeMaps,
etc.), de amplia aceptación en proyectos y desarrollos punteros (como Ciberer, Genomics England, etc.) a
nivel nacional e internacional, en los ámbitos de las tecnologías de la información, la bioinformática y en el
empleo de estas soluciones por la industria farmacéutica y empresas biotecnológicas (como AstraZeneca,
Janssen Pharmaceuticals, Oxford Nanopore, etc.).

# Objetivo

Este documento tiene como principal objetivo detallar la estructura y el flujo de datos de la plataforma
genómica TSuPreMe en su integración mediante los componentes de TPI Server (Apache Nifi, Apache
Airflow, Apache Kafka y microservicios específicos que puedan requerirse).

<TO-DO: ¿sigue siendo esto así? Pretende clarificar la interacción entre el módulo tpi-agent y el servicio tpi-
event-hub, optimizar la gestión de eventos y establecer bases claras para la orquestación de procesos en
TSuPreMe. Además, busca definir la estructura de datos propuesta, garantizar la calidad y la integridad de
la información genómica, y servir como guía de referencia para facilitar una comprensión del proceso de
orquestación.>

# Visión general de procesos a orquestar

Pese a que serán muchos los procesos y tipos de análisis que se orquestarán en la plataforma, actualmente
las aplicaciones más ampliamente desarrolladas se corresponden con análisis genómicos. Para la
comprensión de las siguientes secciones se incluye la Figura 1 que ilustra esquemáticamente los procesos
que tienen lugar en los citados análisis genómicos que se orquestarán en la plataforma (omitiendo el análisis
primario que a partir de ficheros BCL o similares tiene como resultado la generación de los datos crudos o
rawdata):

```
Figura 1. Análisis genómico en TSuPreMe
```
En la Figura 2 se ilustra el recorrido de datos dentro de la plataforma TSuPreMe, que comienza cuando
se registra una petición de servicio genético, ya sea mediante integración con el LIS o creación manual en
la aplicación de gestión de peticiones TPI Request, y su persistencia en el catálogo de la plataforma
(TCatalog) con los servicios de TPI Request. Además, será necesario recopilar información clínica (a
partir del HIS o sistema de información de laboratorio) y demográfica (a partir de Leire en el SNS-O).


Por otro lado, TPI Request se encarga también de la subida de datos crudos procedentes de los
secuenciadores (o resultados de análisis secundario o terciario en caso de migración de datos históricos),
almacenándolos en THealthLake.

A continuación, se orquesta la creación del caso clínico asociado a la petición, y se procede al análisis
secundario, generándose como resultados ficheros de alineamiento (BAM, BAI, etc.) y variantes (VCF),
también almacenados en THealthLake.

Para poder realizar las tareas de filtrado, priorización y selección de variantes significativas, se lleva a cabo
la indexación de variantes en TKnowledge, la anotación a partir de la información de TBioBytes, el
cálculo de estadísticas de variantes y la generación de índices en TIndex para búsquedas avanzadas.

Finalmente, una vez los expertos en diagnóstico seleccionan las variantes significativas y cumplimentan los
textos justificativos para las evidencias (informe), se procede a publicar los hallazgos e informe en el
sistema de información hospitalaria (HIS).

```
Figura 2. Ciclo de vida del dato en TSuPreMe
```
# Descripción funcional de procesos de orquestación

En las siguientes secciones del presente apartado se enumeran y explican los diferentes procesos de
orquestación requeridos en la plataforma, incluyendo para cada orquestación una descripción del proceso,
el componente que realiza la tarea y los procesos que deben desencadenarse a continuación.

## Registro de peticiones

Considerando el estado actual de los sistemas de información del SNS-O, se asume que, al menos inicialmente, no se
dispondrá de conexión con el LIS Progeny para la creación automatizada de peticiones en TSuPreMe. No se abordará, por lo
tanto, la integración relativa a recibir publicaciones (en Mirth o Kafka) por parte de Progeny.

En las peticiones generadas de forma manual con TPI Request se ha implementado ya la consulta a los
endpoints REST del HIS de SNS-O y SOAP de Leire. También se ha generado un endpoint de TPI Request
que permite la creación de la petición en TCatalog. Si se considerase que estas aproximaciones no son
lo suficientemente robustas, se procedería a su redefinición con empleo de servicios de TPI Request.

## Subida de datos a la landing zone / antesala

### Descripción del proceso

Se lleva a cabo la subida de datos existente en un conjunto de carpetas o unidades de red en las que se
volcarán los datos que se desea subir a la plataforma. En [Orígenes de datos para subida](#orígenes-de-datos-para-subida) se incluyen
los detalles sobre las unidades o carpetas de red previstas para SNS-O, así como las diferentes lógicas y
acciones a realizar en función de cada origen de datos.

Los datos que se suben a la plataforma son de varios tipos, incluyendo:

- Datos crudos o brutos de secuenciación.
- Resultados de análisis secundario (ficheros de alineamiento o bam, ficheros de variantes o vcf, etc.).
- Resultados de análisis terciario (normalmente ficheros de variantes significativas o hallazgos, en
  formato vcf).
- Ficheros sample-sheet con información de las muestras incluidas en la carrera o run de
  secuenciación (ver detalles en 6.2 Ejemplo de fichero sample sheet).

Se llevará a cabo la subida de datos a THealthLake implementando una monitorización de las unidades
o localizaciones de red consensuadas con el SNS-O, detectando cuando se escriben nuevos datos y
procediendo a su subida a THealthLake, mediante estrategias de multipart y checkpoints de avance de
las transferencias, que serán gestionadas y trazadas mediante eventos de mensajería en tópicos Kafka.

En este proceso se subirán:

- Las carpetas de carrera/run completas con datos crudos/brutos en caso de tratarse de datos
  generados por los secuenciadores del SNS-O.
- Las carpetas de carrera/run completas con datos crudos/brutos y los resultados en caso de análisis
  y secuenciación externalizados a Nasertic
- Las carpetas de resultados en caso de tratarse de migración de resultados.

Los ficheros se depositarán en el landing zone o antesala de THealthLake, que será un bucket en S3 para
el almacenamiento inicial de los datos, al que el servicio de genética del SNS-O dispondrá de acceso como
si se tratara de un sistema de archivos (mediante montaje o cualquier solución provista por la cabina de
disco en la que residirán los datos).

Las carpetas de carrera/run se generarán en la unidad de red `\\Dc1gpronas007\MISEQ_PRE` con la siguiente
estructura:

```tree
{{sequencer_id}}
├── {{run_or_folder_id}}
│   ├── ficheros de dato bruto en formato fastq.gz
│   ├── SampleSheetUsed.csv
│   ├── RunCompletionStatus.xml
```

En el sistema de archivos de la unidad de red `\\Dc1gpronas007\MISEQ_PRE` se dispondrá de las siguientes
carpetas para gestionar la transferencia:

- `{{agent_id}} / source / {{run_or_folder_id}}`: una vez está completa la carpeta de run/carrera
  generada en `\\Dc1gpronas007\MISEQ_PRE` (se sabe a partir de la generación del fichero
  RunCompletionStatus.xml por parte del secuenciador), se mueve a esta ubicación para proceder a
  la transferencia.
- `{{agent_id}} / completed / {{run_or_folder_id}}`: las carpetas de run ya transferidas se mueven
  a esta ubicación. Si quieren evitarse datos duplicados, deberá eliminarse el contenido de esta
  carpeta. La eliminación podría realizarse de forma periódica mediante un daemon, si se prefiere.
- `{{agent_id}} / failed / {{run_or_folder_id}}`: se mueven a esta ubicación las carpetas de run
  con problemas o errores en la transferencia a la plataforma.
- `{{agent_id}} / logs / {{run_or_folder_id}}`: los logs e información relativos a la transferencia se
  escriben en esta ubicación.

En cuanto a la estructura de carpetas de la landing zone en THealthLake será la siguiente:

```tree
agent / {{agent_id}} / {{run_or_folder_id}}
```
donde {{run_or_folder_id}} es el identificador de la carpeta de resultados o run de secuenciación.

Se aprovechará para **trazar la información de transferencias**, publicando la información en MongoDB. **Se
trata de una mejora, no se incluye en el alcance para la entrega de Enero de 2026**.

### Componente que realiza la tarea

La subida de datos se realiza mediante aplicaciones daemon (TPI Agent) que se ejecutan en máquinas o
servidores del cliente y que disponen de acceso a las carpetas o unidades de red.

Para la gestión de la mensajería y eventos se emplea el componente Apache Kafka de TPI Server.

### Procesos que deben desencadenarse

Tras la finalización de la subida exitosa de los ficheros, debe procederse a su catalogación según lo descrito
en 4.3 Catalogación inicial de ficheros, runs y carpetas de resultados en la plataforma, llamando al endpoint
de catalogación y pasando los parámetros definidos en el jsonschema.

## Catalogación inicial de ficheros, runs y carpetas de resultados en la plataforma

### Descripción del proceso

Para disponer de trazabilidad y capacidades de búsqueda sobre los runs, carpetas y ficheros subidos a la
plataforma, se procede a la catalogación de los mismos. Este proceso consiste en la generación de
entidades en TCatalog con datos y metadatos que permitan la localización y búsqueda.

Además, el proceso de catalogación debe encargarse de:

- Obtener los identificadores de muestra a los que deben vincularse los ficheros, a partir de los
  nombres de los ficheros. Los detalles de la lógica a implementar para extraer los identificadores de
  muestra se recogen en [Orígenes de datos para subida](#orígenes-de-datos-para-subida). Se asume por el momento que todos los
  resultados proporcionados pertenecerán a muestras, aunque se sabe que en análisis familiares,
  habrá resultados a nivel de análisis (por ejemplo, el fichero vcf que incluye variantes de todas las
  muestras del caso familiar).
- Identificar la extensión y tipo de fichero: la lógica está ya incluida en tomic-engine (opencga), que
  infiere el tipo de fichero extrayendo previamente la extensión.
- Catalogación (inclusión en MongoDB) de carpeta (de run o resultados) y ficheros incluidos
  en la carpeta.

### Detalle de acciones

1. Login en TCatalog (tomic-engine) para obtención de token de autenticación.
2. Por cada carpeta o run a catalogar:
   1. Creación de la entidad folder o run en TCatalog, mediante endpoint `files.create` con los atributos:
      1. Payload:
         1. `path`: ruta en TCatalog, siguiendo la estructura `agent/{{tpi-agent-service-id}}/{{run_or_folder_id}}`.
         2. `type`: 'DIRECTORY'.
      2. Parámetros:
         1. `study`: identificador del estudio o proyecto en TCatalog. Se asume temporalmente que habrá una única 
         organización, un único proyecto y un único estudio, por lo que será `{org_id}@{project_id}:{study_id}` o para SNS-O `demo@SNSO:casos`.
   2. Por cada fichero incluido en la carpeta o run:
      1. Extracción del identificador de muestra (`sampleId`) a partir del nombre de fichero, según lo descrito en [Orígenes de datos para subida](#orígenes-de-datos-para-subida).
      2. Creación de la entidad file en TCatalog, incluyendo los atributos:
      3. Payload:
         1. `path`: ruta en TCatalog, siguiendo la estructura `agent/{{tpi-agent-service-id}}/{{run_or_folder_id}}/{{file_name}}`.
         2. `type`: 'FILE'.
      3. - relatedSampleId: identificador de muestra extraído del nombre de fichero.
               - type: tipo de fichero (fastq, vcf, bam, etc.).
               - extension: extensión del fichero (.fastq.gz, .vcf, .bam, etc.).
               - source: origen de datos (carpeta de red del secuenciador MiSeq, carpeta
                 compartida para secuenciaciones externalizadas a Nasertic, etc.).
         b. Creación de la entidad folder o run en TCatalog, incluyendo los atributos:
            - source: origen de datos (carpeta de red del secuenciador MiSeq, carpeta
              compartida para secuenciaciones externalizadas a Nasertic, etc.).

### Componente que realiza la tarea

Se realizará mediante pipeline en Nifi (inicialmente se planteó un único endpoint en la REST API
de TCatalog, repositorio tpi-request para la catalogación de carpetas y runs de secuenciación). El endpoint
deberá recibir la información descrita y estructurada según el siguiente jsonschema (el original se incluye en
el repositorio tomic-engine, en https://setools.t-systems.es/gitlab/health/genomica/tsupreme/tpi/tpi-agent-service/-/blob/main/src/main/resources/avro/Folder.avsc?ref_type=heads).

A partir de la información recibida, el endpoint se encargará de obtener la información incluida en el apartado
anterior (Descripción del proceso), que se persistirá en catálogo (TCatalog) incluyendo los siguientes
atributos y entidades:

- En ficheros de dato crudo/bruto (fastq o fastq.gz) para los que aún no se han creado muestras
  en catálogo (esto se realiza en el proceso de Asociación de ficheros a peticiones), se deberá incluir
  el atributo relatedSampleId, que contendrá el identificador de muestra obtenido a partir del nombre
  de fichero, según lo descrito en [Orígenes de datos para subida](#orígenes-de-datos-para-subida)). <TO-DO: ya existe campo sampleId en los objetos file de
  catálogo; pueden incluirse sampleId que no existen en catálogo? Conviene probar si la REST API de
  tomic-engine permite crear files con muestras asociar> Se plantea finalmente emplear el campo tag
  de los objetos file para incluir el identificador de muestra asociada. Como probablemente se emplee
  también el tag para los identificadores de runs, podría ser necesario incluir un string previo, del tipo
  sampleId_24_250 o runId_28258_205205_200.
  ▪ Para aquellos orígenes de datos organizados a partir de runs o carreras de secuenciación, se
  creará la entidad run en catálogo. Dentro del run se listarán los ficheros generados por el
  secuenciador en la carrera de secuenciación, siguiendo el esquema descrito en el repositorio tomic-
  engine, en https://setools.t-systems.es/gitlab/health/genomica/tsupreme/tpi/tpi-agent-service/-
  /blob/main/src/main/resources/avro/Folder.avsc?ref_type=heads). Otra opción es abordarlo con
  tags dentro de la entidad file (colección file), que podría tener problemas para el indexado de los
  identificadores de run.
- Tanto en runs como carpetas se incluirá el atributo source, que contendrá el nombre del origen de
  datos, por ejemplo, carpeta de red de secuenciador MiSeq, carpeta compartida para secuenciaciones
  externalizadas a Nasertic, etc.

Procesos que deben desencadenarse

Dependerán de los Orígenes de datos para subida considerados:

- Para los datos crudos/brutos generados por secuenciadores del SNS-O (sin considerar las
  externalizaciones a Nasertic) no se desencadenarán aquí otros procesos, quedando a la espera de
  la asociación o vinculación manual de ficheros crudos a la petición (ver 4.4 Asociación de ficheros a
  peticiones).
- <TO-DO: el resto de casos se irán definiendo próximamente, no afectan a la entrega de Enero>

## Asociación de ficheros a peticiones

### Descripción del proceso

Dentro del proceso de creación manual de peticiones, se incluye una acción relativa a la vinculación o
asociación de ficheros a la petición. Para ello, se consultarán en TCatalog los ficheros asociados a la muestra
incluida en la petición, mostrándose el listado en la aplicación web de gestión de peticiones, para que el
usuario pueda seleccionar aquellos ficheros que deben asociarse con la petición. Para posibilitar la
vinculación de los ficheros a las peticiones, habrá que contar con identificadores de muestras asociados con
los ficheros, extraídos previamente a partir de los nombres de los ficheros (todo ello realizado en la
orquestación 4.3). De esta forma podrá consultarse a TCatalog para recabar los ficheros candidatos a
asociarse con la petición (aquellos que van asociados a la misma muestra que la petición).

### Componente que realiza la tarea

La asociación se realiza mediante TPI Request, la aplicación web de gestión de peticiones, que realizará
consultas a TCatalog (mediante la REST API incluida en tomic-engine).

### Procesos que deben desencadenarse

Se deberá:

- Mover los ficheros y carpetas desde su ubicación inicial en la landing zone hasta sus ubicaciones
  definitivas en THealthLake, según se refleja en 4.6 Movimiento de ficheros de landing zone a
  ubicación definitiva en THealthLake.
- Generar las entidades relativas a la petición en catálogo, según lo descrito en 4.7 Creación de caso
  clínico.

## Transformación de resultados provenientes de Nasertic

<TO-DO: pendiente de saber cómo se funcionará exactamente, si se consensuará con Nasertic el cambio
de identificadores de muestra para emplear los de SNS-O o si se deberán transformar los resultados en
TSuPreMe disponiendo del Excel que relacione los identificadores de muestra de Nasertic con los de
SNS-O.>

### Descripción del proceso

Actualmente los análisis externalizados a Nasertic se realizan sustituyendo el identificador de muestra del
SNS-O (por ejemplo, 17-2041) por un nuevo identificador de muestra específico de Nasertic (por ejemplo,
Ex22_00053). Para vincular los resultados generados por Nasertic con las muestras del SNS-O, se
proporciona un Excel con una tabla relacional entre los identificadores de Nasertic y SNS-O.

Para poder cargar los resultados de Nasertic en TSuPreMe es necesario que los ficheros de resultados
generados (bam, vcf, etc.) contengan en sus cabeceras los identificadores de muestra del SNS-O, que sirven
para, por ejemplo, asociar las variantes del fichero a la muestra. Es necesario, por lo tanto, reemplazar los
identificadores de muestra de Nasertic por los identificadores de muestra asociados del SNS-O, tarea
que se realiza mediante la ejecución de un pipeline con TWOK.

### Componente que realiza la tarea

Un pipeline de TWOK se encarga de procesar los ficheros de resultados de Nasertic reemplazando los
identificadores de muestra por los empleados por el SNS-O.

### Procesos que deben desencadenarse

Una vez se ha ejecutado exitosamente el pipeline de TWOK para reemplazar los identificadores de muestras,
se debe desencadenar el Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake,
descrito en [Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake](movimiento-de-ficheros-de-landing-zone-a-ubicación-definitiva-en-thealthlake).

## Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake

### Descripción del proceso

La estructura de carpetas final en la ubicación definitiva en THealthLake seguirá la Propuesta de estructura
de carpetas y ficheros para la información del servicio de genética detallada en 6.3.

### Componente que realiza la tarea

### Procesos que deben desencadenarse

## Creación de caso clínico, pacientes/individuos, muestras

### Descripción del proceso

<TO-DO: >

### Componente que realiza la tarea

### Procesos que deben desencadenarse


## Ejecución de análisis secundario

<TO-DO: no entra en la entrega de Enero>

### Descripción del proceso

### Componente que realiza la tarea

### Procesos que deben desencadenarse

## Indexado de variantes, anotación, estadísticas e índices

<TO-DO: no entra en la entrega de Enero>

### Descripción del proceso

### Componente que realiza la tarea

### Procesos que deben desencadenarse

## Publicación de hallazgos e informe

<TO-DO: no entra en la entrega de Enero>

### Descripción del proceso

### Componente que realiza la tarea

### Procesos que deben desencadenarse

# Alcance para entrega de Enero de

La entrega de funcionalidad de la plataforma TSuPreMe para el SNS-O prevista para Enero de 2026, se
centra en la subida automática de ficheros de dato crudo/bruto generados por los secuenciadores del SNS-O
y la vinculación de los ficheros con peticiones creadas manualmente en la plataforma. Esta funcionalidad
queda cubierta con las orquestaciones siguientes:

- Registro de peticiones (descrito en 4.1), de forma manual inicialmente y ya realizado en la anterior
  entrega de funcionalidad al SNS-O, realizada en Noviembre de 2025.
- Subida de datos a la landing zone / antesala, descrito en 4.2.
- Catalogación inicial de ficheros, runs y carpetas de resultados en la plataforma, descrito en 4.3.
- Asociación de ficheros a peticiones, descrito en 4.4.
- Movimiento de ficheros de landing zone a ubicación definitiva en THealthLake, descrito en 4.
- Creación de caso clínico, con todas las entidades asociadas (muestra, paciente/individuo, caso
  clínico, etc.) en la plataforma TSuPreMe, descrito en 4.7.


# Anexos

## Orígenes de datos para subida

Deberá alcanzarse consenso con el SNS-O para las diferentes unidades o carpetas de red. El disponer
de diferentes unidades, localizaciones o carpetas permitirá diferenciar entre los diferentes tipos de datos a
subir a TSuPreMe, conociendo así el origen y pudiendo aplicar diferentes reglas de negocio. Se proponen
las siguientes unidades o carpetas de red, que se recogen en la siguiente figura y se listan a continuación:

```
Figura 3. Orígenes de datos para subida a TSuPreMe
```
- Datos generados por los secuenciadores del SNS-O y externalización (Nasertic):
  o Carpeta de red para datos crudos de secuenciación (secuenciador): desde el SNS-O se
  lleva a cabo la copia a una unidad de red de los ficheros resultantes del análisis primario de
  sus secuenciadores.
  ▪ Idealmente cada secuenciador del SNS-O debería disponer de una carpeta de red
  exclusiva en la que se volcarán los datos crudos generados por el secuenciador.
  ▪ Los datos deben incluirse en una carpeta con el nombre del run de secuenciación, y
  dicho nombre se indexará como “identificador de run” junto a los ficheros.
  Idealmente la carpeta de datos crudos debería tener la siguiente estructura:
  - {carpeta_datos_crudos} / {run_o_carrera_de_secuenciación} /
  {ficheros}
  ▪ Los datos crudos incluidos en la carpeta de run serán de tipo fastq.gz y generados
  por los secuenciadores del SNS-O, con nombres que verificarán la expresión regular
  {id_muestra}_
  S{sample_number}_L{lane_number}_R{read_group}_{set_number}.fastq.gz, donde:
- {id_muestra}: identificador de la muestra del SNS-O, por ejemplo, 17-2041,
  19 - 2958, etc.
- {sample_number} serán de 1 a 3 dígitos.
- {lane_number} son 3 dígitos.
- {read_group} es 1 dígito (1 para forward reads y 2 para reverse reads).
- {set_number} son 3 dígitos.
  ▪ Junto con los ficheros de datos crudos se incluirá el sample-sheet de secuenciación
  (ver 6.2 Ejemplo de fichero sample sheet). Este fichero también se indexará en
  TCatalog.
  ▪ Ejemplo de nombre completo de fichero de dato crudo: 17 -
  2041_S2_L001_R1_001.fastq.gz, 19 - 2043_S1_L002_R2_002.fastq.gz
  ▪ Se obtendrá así el nombre de la carrera o run de secuenciación y el identificador de
  muestra, ambos necesarios para la asociación del dato a una muestra perteneciente
  a una petición.
  o Carpeta de red para análisis externalizados a Nasertic: los resultados de los análisis y
  experimentos de secuenciación externalizados a Nasertic son depositados en una carpeta de
  red exclusiva.
  ▪ <TO-DO: no se dispone de acceso a datos recientes para ver los detalles; tampoco
  se ha aclarado aún si se consensuará con Nasertic el empleo de identificadores de
  muestra de SNS-O o se seguirán empleando identificadores propios, con lo que
  seguirá siendo necesario procesar los resultados para sustituir los nombres de
  muestras incluidos en los ficheros>
  ▪ La estructura de nombres de ficheros y carpetas se describe en el entregable
  https://telekom.sharepoint.de/:w:/r/sites/ESPRGENNAV2/Freigegebene%20Dokume
  nte/Gral/00_Working%20Docs%20(Project)/02%20Funcional/E01-07/E01-
  07_plan_de_migraci%C3%B3n.docx?d=wa396e0c3b319431f81dd84972a18cf5b&csf
  =1&web=1&e=Q4ynd7.
  ▪ Los datos se incluirán en una carpeta con el nombre del run de secuenciación de
  Nasertic, y dicho nombre se indexará como “identificador de run” junto a los ficheros.
  Idealmente la carpeta de datos crudos debería tener la siguiente estructura:
- {carpeta_datos_crudos} / {run_o_carrera_de_secuenciación} /
  {ficheros}
  ▪ En la carpeta se incluyen tanto ficheros de dato crudo (fastq.gz) como todos aquellos
  ficheros de resultados generados durante el análisis secundario. En el documento de
  migración (E01-07_plan_de_migración.docx) se especifican los ficheros aportados,
  que deberán incluirse en THealthLake e indexarse en TCatalog,
  ▪ Se obtendrá así el nombre de la carrera o run de secuenciación y el identificador de
  muestra, ambos necesarios para la asociación del dato a una muestra perteneciente
  a una petición.
- Datos del SNS-O a migrar a TSuPreMe: <TO-DO: pendiente de decidir si se dispondrá de diferentes
  carpetas para cada origen o si se inferirá el origen a partir de los ficheros>
  o Carpeta de red para migración de resultados de TruSight (Illumina Variant Interpreter):
  incluyen los resultados de análisis realizados con Variant Interpreter de Illumina.
  ▪ Para cada
  ▪ <TO-DO>: pendiente de revisión de carpetas de red proporcionadas por SNS-O para
  ver detalles y de dónde debe extraerse el identificador de run, origen, etc.
  o Carpeta de red para migración de resultados de Nasertic:
  ▪ <TO-DO>
  o Carpeta de red para migración de resultados de SOPhiA:
  ▪ <TO-DO>

## Ejemplo de fichero sample sheet

Los secuenciadores habitualmente empleados generan un fichero sample sheet que contiene los
identificadores de las muestras incluidas en el run o carrera de secuenciación, así como los índices
empleados para poder asignar las lecturas de secuenciación a cada una de las muestras en el proceso de
demultiplexado. Este fichero juega un papel importante, ya que permite conocer las muestras incluidas en
el run. A continuación, se lista un ejemplo:

```bash
$ cat /var/lib/rancher/MISEQ/250613_TSHC/SampleSheetUsed.csv

[Header]
Local Run Manager Analysis Id,
Experiment Name,
Date,2025- 06 - 13
Module,DNA Enrichment - 3.1.
Workflow,Enrichment
Library Prep Kit,Illumina DNA Prep with Enrichment
Index Kit,Ilmn DNA-RNA UD Indexes SetA Tagmentation
Chemistry,Amplicon

[Manifests]
manifest0,TruSight_Hereditary_Cancer_TargetedRegions_v2.txt

[Reads]
151
151

[Settings]
runbwaaln,
variantcaller,GATK
manifestpaddingsize,
flagpcrduplicates,
indelrealignment,GATK
picardhsmetrics,
adapter,CTGTCTCTTATACACATCT
variantannotation,MARS

[Data]
Sample_ID,Sample_Name,Description,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_
Project,Manifest,GenomeFolder
25 - 2199,25-
2199,,H01,UDP0008,GATCAAGGCA,UDP0008,CCTTGTTAAT,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2200,25-
2200,,H02,UDP0016,CGGTTACGGC,UDP0016,AAGACTATAG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2276,25-
2276,,H03,UDP0024,TTCTACATAC,UDP0024,CTAACTGTAA,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2217,25-
2217,,H04,UDP0032,ACAGTGTATG,UDP0032,GAACATACGG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2273,25-
2273,,H05,UDP0040,TATGTAGTCA,UDP0040,CGCACTAATG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2272,25-
2272,,H06,UDP0048,CAGTGGCACT,UDP0048,TAACAATAGG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2278,25-
2278,,H07,UDP0056V3,ACTCTATTGT,UDP0056V3,ATCGCATATG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\
WholeGenomeFasta


25 - 2342,25-
2342,,H08,UDP0064,TGGCGCGAAC,UDP0064,AACTGATACT,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2345,25-
2345,,H09,UDP0072V3,TACGAGTCCA,UDP0072V3,TAGCGAAGCA,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\
WholeGenomeFasta
25 - 2394,25-
2394,,H10,UDP0080,GGCAAGCCAG,UDP0080,CAAGCATCCG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2395,25-
2395,,H11,UDP0088,GGAGCGTGTA,UDP0088,ATCCGTAAGT,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
25 - 2354,25-
2354,,H12,UDP0096,CCTCCGTCCA,UDP0096,CACATCGGTG,,manifest0,Homo_sapiens\UCSC\hg19\Sequence\Whol
eGenomeFasta
```

En la sección `[Data]` del fichero sample sheet se recoge el listado de muestras del run o carrera de
secuenciación, cuyos identificadores se incluyen en el parámetro Sample_ID de cada fila.

## Propuesta de estructura de carpetas y ficheros para la información del servicio de genética

Tras conversación con Amaya, se manifestó la necesidad de disponer de carpetas con los identificadores
de muestra del SNS-O fácilmente accesibles. Por ello se proponen cambios a la estructura inicialmente
presentada en el entregable del análisis as-is (E01-04_análisis as-is.docx o E01-04 SNSO_CIRCUITO AS
IS V0.1.pdf), que quedaría de la siguiente forma:

```tree
/{{ruta_a_carpeta_compartida}}/
├── sample
│ ├── {{id de muestra 1}}
│ │ ├── {{id de muestra 1}}_R1.fastq.gz
│ │ ├── {{id de muestra 1}}_R2.fastq.gz
│ │ ├── {{id de muestra 1}}.bam
│ │ ├── {{id de muestra 1}}.bam.bai
│ │ ├── {{id de muestra 1}}.bam.md5sum
│ │ └── {{id de muestra 1}}.vcf.gz
│ ├── ...
│ └── {{id de muestra n}}
│ ├── {{id de muestra n}}_R1.fastq.gz
│ ├── {{id de muestra n}}_R2.fastq.gz
│ ├── {{id de muestra n}}.bam
│ ├── {{id de muestra n}}.bam.bai
│ ├── {{id de muestra n}}.bam.md5sum
│ └── {{id de muestra n}}.vcf.gz
├── patient
│ ├── {{id de paciente 1}}
│ │ ├── file_ 1
│ │ ├── ...
│ │ └── file_n
│ ├── ...
│ └── {{id de paciente n}}
│ ├── file_
│ ├── ...
│ └── file_n
└── case
├── {{id de caso 1}}
│ ├── file_
│ ├── ...
│ └── file_n
└── ...
```

La estructura aquí propuesta prevé la existencia de ficheros (crudos/brutos o resultados) asociados tanto a
muestras como pacientes, casos o cualquier otra entidad de información manejada por el SNS-O en el futuro.
Se conserva así la versatilidad de la estructura propuesta inicialmente en cuanto a la asociación de ficheros
a diferentes tipos de entidades, posibilitando además un acceso rápido a los ficheros por muestra, paciente
o caso, directamente desde la propia estructura de ficheros, y también a partir del propio catálogo de
TSuPreMe.
