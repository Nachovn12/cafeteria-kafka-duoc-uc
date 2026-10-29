FROM node:20-alpine

WORKDIR /app

# Instalar dependencias exactas usando package-lock.json
COPY package*.json ./
RUN npm ci --omit=dev

# Copiar el resto del código de la aplicación
COPY . .

# Variables de entorno por defecto
ENV NODE_ENV=production \
    PORT=8080

# Exponer el puerto del servidor HTTP interno
EXPOSE 8080

# Iniciar servidor
CMD ["node", "server.js"]
