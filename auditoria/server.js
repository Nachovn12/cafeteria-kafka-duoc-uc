/**
 * server.js - Microservicio de Auditoría Independiente (Requisito R5)
 * Cafetería Kafka — Duoc UC
 * 
 * Lee el historial completo desde el offset inicial (fromBeginning: true)
 * utilizando un Consumer Group independiente ('grupo-auditoria')
 * sin interferir con las cocinas ('grupo-cocina').
 */

const { Kafka, logLevel } = require('kafkajs');

// Configuración mediante variables de entorno
const rawBrokers = process.env.KAFKA_BROKERS || 'kafka:29092';
const kafkaBrokers = rawBrokers.split(',').map(b => b.trim()).filter(Boolean);
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'grupo-auditoria';
const TOPIC = process.env.KAFKA_TOPIC || 'pedidos';
const CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'cafeteria-auditoria';

console.log('================================================================');
console.log('📋 INICIANDO SERVICIO DE AUDITORÍA INDEPENDIENTE (R5)');
console.log('================================================================');
console.log(`- Brokers: ${kafkaBrokers.join(', ')}`);
console.log(`- Consumer Group: ${GROUP_ID}`);
console.log(`- Topic a auditar: ${TOPIC}`);
console.log(`- Client ID: ${CLIENT_ID}`);
console.log('================================================================\n');

// Instancia del cliente Kafka
const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: kafkaBrokers,
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

let consumer = null;
let auditStats = {
  totalLeidos: 0,
  pedidosValidos: 0,
  mensajesMalformados: 0,
  particiones: { 0: 0, 1: 0, 2: 0 },
  primerOffsetPorParticion: {},
  ultimoOffsetPorParticion: {}
};

/**
 * Valida si un objeto cumple con la estructura de un pedido real del caso
 * @param {any} obj - Objeto decodificado
 * @returns {boolean}
 */
function esPedidoEstructuradoValido(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  if (!obj.cliente || typeof obj.cliente !== 'string' || !obj.cliente.trim()) return false;
  if (!obj.producto || typeof obj.producto !== 'string' || !obj.producto.trim()) return false;
  const cant = Number(obj.cantidad);
  if (isNaN(cant) || cant <= 0) return false;
  return true;
}

/**
 * Espera y verifica la disponibilidad del broker y del topic 'pedidos'
 */
async function verificarBrokerYTopic(maxIntentos = 30, intervaloMs = 2000) {
  const admin = kafka.admin();
  let intento = 0;
  let conectado = false;

  while (!conectado && intento < maxIntentos) {
    intento++;
    try {
      console.log(`[Auditoría Admin] Verificando broker Kafka (intento ${intento}/${maxIntentos})...`);
      await admin.connect();
      conectado = true;
      console.log('✅ [Auditoría Admin] Conexión establecida con el broker.');
    } catch (err) {
      console.warn(`⏳ [Auditoría Admin] Broker no disponible (${err.message}). Reintentando en ${intervaloMs / 1000}s...`);
      await new Promise(r => setTimeout(r, intervaloMs));
    }
  }

  if (!conectado) {
    throw new Error(`No fue posible conectar con el broker tras ${maxIntentos} intentos.`);
  }

  try {
    const topics = await admin.listTopics();
    if (!topics.includes(TOPIC)) {
      console.warn(`⚠️ [Auditoría Admin] Topic '${TOPIC}' no encontrado. Esperando a que el backend principal lo inicialice...`);
    } else {
      const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] });
      const topicInfo = metadata.topics.find(t => t.name === TOPIC);
      const cantParticiones = topicInfo ? topicInfo.partitions.length : 0;
      console.log(`✅ [Auditoría Admin] Topic '${TOPIC}' confirmado con ${cantParticiones} particiones.`);
    }
  } finally {
    await admin.disconnect();
  }
}

/**
 * Inicia el consumidor de auditoría con fromBeginning: true
 */
async function iniciarConsumidorAuditoria() {
  consumer = kafka.consumer({
    groupId: GROUP_ID,
    retry: {
      initialRetryTime: 300,
      retries: 10
    }
  });

  consumer.on('consumer.connect', () => {
    console.log(`✅ [Auditoría Consumer] Conectado exitosamente con groupId '${GROUP_ID}'`);
  });

  consumer.on('consumer.disconnect', () => {
    console.warn(`⚠️ [Auditoría Consumer] Desconectado del broker`);
  });

  consumer.on('consumer.crash', (err) => {
    console.error(`💥 [Auditoría Consumer] Error fatal en consumidor:`, err);
  });

  consumer.on('consumer.group_join', (e) => {
    console.log(`🤝 [Auditoría GroupJoin] Miembro ${e.payload.memberId} unido a '${GROUP_ID}'. Asignación:`, JSON.stringify(e.payload.memberAssignment));
  });

  await consumer.connect();

  // Suscripción con lectura desde el offset inicial (fromBeginning: true)
  // Al ser un grupo nuevo sin offsets previos, lee todo el historial disponible
  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: true
  });
  console.log(`📖 [Auditoría Consumer] Suscrito a '${TOPIC}' con 'fromBeginning: true'. Leyendo historial completo...`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const offset = Number(message.offset);
        const rawValue = message.value ? message.value.toString('utf-8') : '';
        const key = message.key ? message.key.toString('utf-8') : null;
        const kafkaTimestamp = message.timestamp ? Number(message.timestamp) : Date.now();
        const fechaMsg = new Date(kafkaTimestamp).toISOString();

        auditStats.totalLeidos++;
        auditStats.particiones[partition] = (auditStats.particiones[partition] || 0) + 1;

        if (auditStats.primerOffsetPorParticion[partition] === undefined) {
          auditStats.primerOffsetPorParticion[partition] = offset;
        }
        auditStats.ultimoOffsetPorParticion[partition] = offset;

        let parsed = null;
        let esValido = false;

        try {
          parsed = JSON.parse(rawValue);
          esValido = esPedidoEstructuradoValido(parsed);
        } catch (e) {
          esValido = false;
        }

        if (esValido) {
          auditStats.pedidosValidos++;
          const id = parsed.id || `P-${partition}-${offset}`;
          const cliente = parsed.cliente;
          const producto = parsed.producto;
          const cantidad = parsed.cantidad;
          const nota = parsed.nota ? ` | Nota: "${parsed.nota}"` : '';

          console.log(
            `[AUDITORÍA] #ID: ${id.padEnd(8)} | Topic: ${topic} | P${partition} @ Offset ${String(offset).padEnd(4)} | ` +
            `Clave: ${String(key).padEnd(12)} | Cliente: ${cliente.padEnd(12)} | Item: ${cantidad}x ${producto}${nota} | Tipo: PEDIDO_VALIDO`
          );
        } else {
          auditStats.mensajesMalformados++;
          const rawMuestra = rawValue.replace(/\r?\n|\r/g, ' ').substring(0, 40);
          console.log(
            `[AUDITORÍA] ALERTA R7   | Topic: ${topic} | P${partition} @ Offset ${String(offset).padEnd(4)} | ` +
            `Clave: ${String(key).padEnd(12)} | Raw: "${rawMuestra}" | Tipo: MENSAJE_MALFORMADO (No es un pedido)`
          );
        }
      } catch (errProceso) {
        console.error('❌ [Auditoría] Error procesando registro:', errProceso.message);
      }
    }
  });
}

// Cierre ordenado
async function apagarServicio(senal) {
  console.log(`\n🛑 Recibido ${senal}. Cerrando servicio de auditoría...`);
  console.log('📊 RESUMEN FINAL DE AUDITORÍA:');
  console.log(`   - Total registros leídos: ${auditStats.totalLeidos}`);
  console.log(`   - Pedidos válidos: ${auditStats.pedidosValidos}`);
  console.log(`   - Mensajes malformados: ${auditStats.mensajesMalformados}`);
  console.log(`   - Por partición: P0: ${auditStats.particiones[0]}, P1: ${auditStats.particiones[1]}, P2: ${auditStats.particiones[2]}`);
  console.log(`   - Rangos de offset procesados:`, JSON.stringify({
    primeros: auditStats.primerOffsetPorParticion,
    ultimos: auditStats.ultimoOffsetPorParticion
  }));

  try {
    if (consumer) {
      console.log('Desconectando Consumidor Kafka...');
      await consumer.disconnect();
    }
  } catch (err) {
    console.error('Error al desconectar consumidor:', err.message);
  }

  process.exit(0);
}

process.on('SIGTERM', () => apagarServicio('SIGTERM'));
process.on('SIGINT', () => apagarServicio('SIGINT'));

// Arranque con reintentos
async function main() {
  let iniciado = false;
  while (!iniciado) {
    try {
      await verificarBrokerYTopic();
      await iniciarConsumidorAuditoria();
      iniciado = true;
      console.log('\n🚀 [Auditoría] Servicio activo y escuchando eventos continuamente en tiempo real.\n');
    } catch (err) {
      console.error('⚠️ [Auditoría] Falla al inicializar. Reintentando en 3s...', err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

main();

