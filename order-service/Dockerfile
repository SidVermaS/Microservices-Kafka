FROM node:24-alpine

WORKDIR /app

# copy manifests first so npm ci is cached until deps actually change
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src

EXPOSE 3000
CMD ["node", "src/index.js"]
