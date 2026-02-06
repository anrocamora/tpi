# Estructura documental de TSuPreMe

<!-- TOC -->
* [Estructura documental de TSuPreMe](#estructura-documental-de-tsupreme)
* [Documentos de requerimientos](#documentos-de-requerimientos)
  * [Contenido de documentos de requerimientos](#contenido-de-documentos-de-requerimientos)
* [Documentos de análisis funcional](#documentos-de-análisis-funcional)
  * [Contenido de documentos de análisis funcional](#contenido-de-documentos-de-análisis-funcional)
    * [Diagrama de actores](#diagrama-de-actores)
    * [Catálogo de casos de uso](#catálogo-de-casos-de-uso)
    * [Interfaces de usuario](#interfaces-de-usuario)
    * [Modelo de datos](#modelo-de-datos)
    * [Integraciones con otros sistemas](#integraciones-con-otros-sistemas)
    * [Descripción de los casos de uso](#descripción-de-los-casos-de-uso)
    * [Modelo de procesos](#modelo-de-procesos)
  * [Plantilla para casos de uso](#plantilla-para-casos-de-uso)
* [Documentos de diseño técnico](#documentos-de-diseño-técnico)
  * [Contenido de documentos de diseño técnico](#contenido-de-documentos-de-diseño-técnico)
    * [Arquitectura del sistema](#arquitectura-del-sistema)
    * [Diseño de APIs](#diseño-de-apis)
    * [Modelo de datos](#modelo-de-datos-1)
    * [Estrategia de despliegue](#estrategia-de-despliegue)
    * [Consideraciones de seguridad](#consideraciones-de-seguridad)
    * [Mantenimiento y escalabilidad](#mantenimiento-y-escalabilidad)
    * [Pruebas y validación](#pruebas-y-validación)
    * [Documentación de código](#documentación-de-código)
    * [Herramientas y tecnologías](#herramientas-y-tecnologías)
    * [Diagramas y esquemas](#diagramas-y-esquemas)
    * [Consideraciones de rendimiento](#consideraciones-de-rendimiento)
    * [Gestión de versiones](#gestión-de-versiones)
    * [Anexos](#anexos)
  * [Plantilla para documentos de pruebas técnicas de validación](#plantilla-para-documentos-de-pruebas-técnicas-de-validación)
<!-- TOC -->

# Documentos de requerimientos

## Contenido de documentos de requerimientos

Los documentos de requerimientos se incluyen en la carpeta `requirements` y deben incluir al menos los siguientes apartados:

** TODO: completar **

# Documentos de análisis funcional

## Contenido de documentos de análisis funcional

Los documentos de análisis funcional se incluyen en la carpeta `specifications` y deben incluir al menos los siguientes apartados:

### Diagrama de actores

Usuarios o actores que interactúan con el sistema TSuPreMe.

### Catálogo de casos de uso

Listado de casos de uso que describen las interacciones entre los actores y el sistema TSuPreMe.

### Interfaces de usuario
Interfaces gráficas o de línea de comandos que permiten a los usuarios interactuar con el sistema TSuPreMe.

### Modelo de datos

Se incluirá un diagrama entidad-relación (ER) o similar que describa las entidades, atributos y relaciones del modelo de datos de TSuPreMe.

### Integraciones con otros sistemas

Descripción de las integraciones con sistemas externos que interactúan con TSuPreMe.
Se incluirán como casos de uso.

### Descripción de los casos de uso

Se enumerarán y describirán los casos de uso necesarios.

Para cada caso de uso se debe incluir:
- Identificador único del caso de uso.
- Requisitos que satisface.
- Actores involucrados.
- Activación: cómo se inicia el caso de uso.
- Seguridad: consideraciones de seguridad relevantes.
- Descripción: pasos detallados del caso de uso.
- Flujo principal del caso de uso: diagrama o descripción funcional del flujo.
- Flujos alternativos: posibles variaciones o excepciones en el flujo principal.

### Modelo de procesos

Descripción de los procesos, que debe incluir:
- Identificador único del proceso.
- Nombre del proceso.
- Descripción del proceso.
- Flujo del proceso: diagrama o descripción funcional del flujo.
- Reglas de negocio asociadas al proceso.

Los procesos no incluyen actores, ya que son definidos en los casos de uso.

## Plantilla para casos de uso

Copia/pega este bloque para nuevos casos (recomendación: mantener una tabla por UC y una subsección `###` por nombre de caso de uso).

**Identificador único del caso de uso:** **UC-XX-XXX**

**Requerimientos que satisface:** **REQ-XX-XXX**

**Actores involucrados:**

- Listado de actores involucrados en el caso de uso.

**Activación o desencadenante:**

- Listado de acciones o eventos que inician el caso de uso.

**Seguridad:**

- Listado de consideraciones de seguridad relevantes.

**Descripción:**

- Descripción o listado en pasos del caso de uso.

**Flujo principal del caso de uso:**

1. Listado enumerado de los pasos del flujo principal del caso de uso.

# Documentos de diseño técnico

## Contenido de documentos de diseño técnico

Los documentos de diseño técnico se incluyen en la carpeta `technical_design` y deben incluir al menos los siguientes apartados:

### Arquitectura del sistema

Descripción de la arquitectura general del sistema TSuPreMe, incluyendo diagramas de componentes y su interacción.

### Diseño de APIs

Descripción de las APIs utilizadas en TSuPreMe, incluyendo especificaciones de endpoints, métodos, parámetros y respuestas.

### Modelo de datos

Descripción detallada del modelo de datos utilizado en TSuPreMe, incluyendo diagramas y definiciones de tablas, campos y relaciones.

### Estrategia de despliegue

Descripción de la estrategia de despliegue del sistema TSuPreMe, incluyendo entornos, herramientas y procesos utilizados.

### Consideraciones de seguridad

Descripción de las medidas de seguridad implementadas en TSuPreMe, incluyendo autenticación, autorización y protección de datos.

### Mantenimiento y escalabilidad

Descripción de las estrategias para el mantenimiento y escalabilidad del sistema TSuPreMe, incluyendo prácticas recomendadas y herramientas utilizadas.

### Pruebas y validación

Descripción de los enfoques y metodologías de pruebas utilizadas para validar el diseño técnico de TSuPreMe, incluyendo tipos de pruebas y criterios de aceptación.

### Documentación de código

Descripción de las prácticas y estándares de documentación de código utilizados en TSuPreMe, incluyendo ejemplos y herramientas recomendadas.

### Herramientas y tecnologías

Listado y descripción de las herramientas y tecnologías utilizadas en el desarrollo y diseño técnico de TSuPreMe.

### Diagramas y esquemas

Incluir diagramas y esquemas relevantes que apoyen la comprensión del diseño técnico de TSuPreMe.

### Consideraciones de rendimiento

Descripción de las estrategias y prácticas implementadas para optimizar el rendimiento del sistema TSuPreMe, incluyendo monitoreo y ajustes de rendimiento.

### Gestión de versiones

Descripción de las prácticas y herramientas utilizadas para la gestión de versiones del diseño técnico y del código fuente de TSuPreMe.

### Anexos

Incluir cualquier información adicional relevante que apoye el diseño técnico de TSuPreMe, como referencias, glosarios o documentación complementaria.

## Plantilla para documentos de pruebas técnicas de validación
Copia/pega este bloque para nuevos documentos de pruebas técnicas de validación.
**Título:** Título del documento de pruebas técnicas de validación.
**Área/Propietario:** Nombre del área o propietario del documento.
**Fecha de última modificación:** DD/MM/AAAA
**Versión:** X.X
**Resumen:** Breve resumen del contenido del documento.
**Objetivo de la prueba:**
**Criterios de aceptación:**
**Plan de pruebas:**
**Resultados esperados:**
**Registro de resultados:**
**Conclusiones y recomendaciones:**
