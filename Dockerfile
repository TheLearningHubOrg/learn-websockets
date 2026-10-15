# Multi-stage Dockerfile for WebSockets Server
FROM node:20-alpine AS runner

WORKDIR /app
COPY package.json ./
RUN npm install

COPY . .

EXPOSE 8080

CMD ["npm", "start"]
