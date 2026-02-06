# Seguridad desactivada temporalmente

Este proyecto tenía configurado Spring Security como Resource Server (JWT) para proteger `/api/v1/**`.

Por petición del equipo, se ha desactivado temporalmente:
- Dependencias Maven de `spring-boot-starter-security`, `spring-boot-starter-oauth2-resource-server` y `spring-security-test`.
- Configuración YAML relacionada con JWT/issuer.
- Tests específicos de seguridad.

Para reactivarla, descomentar dependencias en `pom.xml` y restaurar una configuración `SecurityFilterChain`.
