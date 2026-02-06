FROM alpine:3.19

# Metadata
LABEL maintainer="genomica"
LABEL description="Alpine Linux con git, curl, jq y bash para scripts de automatización"
LABEL version="1.0.0"

# Instalar herramientas necesarias en una sola capa para optimizar el tamaño
RUN apk add --no-cache \
    git \
    curl \
    jq \
    bash \
    ca-certificates \
    && rm -rf /var/cache/apk/*

# Configurar bash como shell por defecto
SHELL ["/bin/bash", "-c"]

# Verificar que todas las herramientas estén instaladas
RUN git --version && \
    curl --version && \
    jq --version && \
    bash --version

# Usuario no root para seguridad (opcional, comentado por si necesitas root)
# RUN adduser -D -u 1000 appuser
# USER appuser

WORKDIR /workspace

CMD ["/bin/bash"]

