# Documentación de análisis y desarrollo de TSuPreMe

Repositorio de documentación del proyecto **TSuPreMe** (*T-Systems Suite for Precision Medicine*), donde se recopilan 
**requerimientos**, **especificaciones** (análisis funcional) y documentos de **diseño técnico** del sistema.

<!-- TOC -->
* [Documentación de análisis y desarrollo de TSuPreMe](#documentación-de-análisis-y-desarrollo-de-tsupreme)
* [Estructura del repositorio](#estructura-del-repositorio)
* [Organización de la documentación](#organización-de-la-documentación)
* [Documentos de trazabilidad](#documentos-de-trazabilidad)
  * [Requerimientos](#requerimientos)
  * [Especificaciones o análisis funcional](#especificaciones-o-análisis-funcional)
  * [Diseño técnico de sistema](#diseño-técnico-de-sistema)
* [Documentos de apoyo](#documentos-de-apoyo)
* [Convenciones](#convenciones)
* [Generación de documentación](#generación-de-documentación)
<!-- TOC -->

# Estructura del repositorio

- `requirements/`: requerimientos (funcionales, no funcionales y restricciones de cliente).
- `specifications/`: especificaciones funcionales y de proceso.
- `technical_design/`: diseño técnico (arquitectura, APIs, modelos de datos, despliegue, etc.).
- `supporting_documents/`: documentos de apoyo (glosarios, referencias, etc.).

> Nota: si alguna carpeta aún no existe en el repositorio, se creará conforme se incorporen documentos.

# Organización de la documentación

La estructura y convenciones documentales del repositorio se describen en:

- [Estructura documental de TSuPreMe](estructura_documental.md)

# Documentos de trazabilidad

## Requerimientos

- *(pendiente de añadir documentos en `requirements/`)*

## Especificaciones o análisis funcional

- [Subida de datos](specifications/subida_de_datos.md)

## Diseño técnico de sistema

- [Subida de datos](system_design/subida_de_datos.md)

# Documentos de apoyo

- [Orquestación de flujos y procesos en TSuPreMe](supporting_documents/orquestacion_de_flujos.md)

# Convenciones

- Los documentos deben estar en **Markdown** (`.md`).
- Recomendación: incluir al inicio del documento el **título**, **área/propietario** y **fecha de última modificación**.
- Recomendación: incluir una **tabla de contenidos (TOC)** cuando el documento sea largo.

# Generación de documentación

```bash
pandoc subida_de_datos.md --from markdown --template eisvogel \
  --pdf-engine=xelatex --variable graphics=true --toc --strip-comments \
  --include-in-header=../styles/pandoc-logo.tex \
  --include-in-header=../styles/pandoc-custom.tex \
  --metadata title="Subida de datos a TSuPreMe" \
  --metadata author="Genómica - Health - T-Systems" \
  -o /home/takeshigitano/repository/commons/analysis/_build/subida_de_datos.pdf
```