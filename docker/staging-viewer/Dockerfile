FROM node:26-bookworm AS build
WORKDIR /app
COPY --from=viewer package.json package-lock.json ./
RUN git config --global url.https://github.com/.insteadOf ssh://git@github.com/ && npm ci --no-audit --no-fund
COPY --from=viewer . ./
COPY cjloupe-link.patch ./
RUN patch -p1 < cjloupe-link.patch
COPY manifest.json ./
ARG RELEASE_VERSION
ARG TILES_DIR
RUN sed -i "s/@RELEASE_VERSION@/${RELEASE_VERSION}/g; s/@TILES_DIR@/${TILES_DIR}/g" manifest.json \
    && MANIFEST_PATH=manifest.json npm run build

FROM node:26-bookworm AS loupe-build
WORKDIR /loupe
RUN npm install --global bun@1.3.13
COPY --from=loupe package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY --from=loupe . .
RUN bun run build

FROM nginx:stable-alpine
ARG VIEWER_REVISION
LABEL org.opencontainers.image.source="https://github.com/3DBAG/3dbag-viewer" \
      org.opencontainers.image.revision=$VIEWER_REVISION
COPY default.conf /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
COPY --from=loupe-build /loupe/dist /usr/share/nginx/html/CJLoupe
