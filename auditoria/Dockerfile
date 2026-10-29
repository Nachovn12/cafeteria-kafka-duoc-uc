FROM node:20-alpine

WORKDIR /app

# Instalar dependencias
COPY package*.json ./
RUN npm install --omit=dev

# Copiar código del microservicio
COPY . .

# Iniciar proceso de auditoría
CMD ["node", "server.js"]

