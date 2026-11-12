# The optional UI: Node builds the static app, Caddy serves it and proxies the API to HS on one port.
FROM node:26-alpine AS build
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ui/.npmrc ./
RUN npm ci
COPY ui ./
RUN npm run build

FROM caddy:2.11.9-alpine
COPY docker/Caddyfile /etc/caddy/Caddyfile
COPY --from=build /ui/dist /srv
