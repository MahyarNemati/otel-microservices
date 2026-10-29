# The Node worker. Two stages so the TypeScript compiler and the dev
# dependencies do not ship in the runtime image.
FROM node:26-alpine AS build

WORKDIR /app

# npm ci needs both files and respects the lockfile exactly, which is the point
# of using it over npm install in a build.
COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:26-alpine AS runtime

WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=build /app/dist ./dist

USER node
EXPOSE 8083
ENTRYPOINT ["node", "dist/main.js"]
