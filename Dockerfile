# syntax=docker/dockerfile:1

FROM node:20-bookworm-slim

WORKDIR /app

# Dependências primeiro (cache)
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Código
COPY . .

# Diretórios padrão (podem ser sobrescritos por env no Coolify)
ENV NODE_ENV=production \
    TEMP_DIR=/data/temp \
    LOG_DIR=/data/logs

RUN mkdir -p /data/temp /data/logs && chmod -R 777 /data

# Entrypoint configurável: full/delta/discovery
RUN chmod +x /app/docker/entrypoint.sh

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["full"]
