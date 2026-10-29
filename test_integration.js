/**
 * test_integration.js
 * Suite de verificación automatizada para R1, R2, R6 y R7
 * Cafetería Kafka — Duoc UC
 */

const http = require('http');

const BASE_URL = process.env.TEST_URL || 'http://localhost:8081';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Cliente SSE simple con EventEmitter
class SimpleSSEClient {
  constructor(url) {
    this.url = url;
    this.events = [];
    this.listeners = [];
    this.req = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.req = http.get(this.url, (res) => {
        if (res.statusCode !== 200) {
          return reject(new Error(`SSE connection failed: ${res.statusCode}`));
        }

        let buffer = '';
        res.on('data', (chunk) => {
          buffer += chunk.toString('utf-8');
          const lines = buffer.split('\n\n');
          buffer = lines.pop(); // guardar fragmento incompleto

          for (const block of lines) {
            const dataLines = block.split('\n').filter(l => l.startsWith('data: '));
            for (const line of dataLines) {
              const jsonStr = line.replace(/^data: /, '').trim();
              try {
                const parsed = JSON.parse(jsonStr);
                this.events.push(parsed);
                for (const listener of this.listeners) {
                  listener(parsed);
                }
              } catch (e) {
                // Ignore parse errors on comments/heartbeats
              }
            }
          }
        });

        res.on('end', () => {});
        res.on('error', (err) => reject(err));

        // Esperar primer evento de handshake
        const checkReady = setInterval(() => {
          if (this.events.some(e => e.tipo === 'conexion')) {
            clearInterval(checkReady);
            resolve();
          }
        }, 50);

        setTimeout(() => {
          clearInterval(checkReady);
          resolve(); // continuar de todos modos tras 1s
        }, 1500);
      });

      this.req.on('error', reject);
    });
  }

  onEvent(cb) {
    this.listeners.push(cb);
  }

  waitForEvent(predicate, timeoutMs = 8000) {
    return new Promise((resolve, reject) => {
      // Revisar si ya llegó
      const existing = this.events.find(predicate);
      if (existing) return resolve(existing);

      const timer = setTimeout(() => {
        reject(new Error(`Timeout (${timeoutMs}ms) esperando evento SSE`));
      }, timeoutMs);

      const listener = (event) => {
        if (predicate(event)) {
          clearTimeout(timer);
          const idx = this.listeners.indexOf(listener);
          if (idx !== -1) this.listeners.splice(idx, 1);
          resolve(event);
        }
      };

      this.listeners.push(listener);
    });
  }

  close() {
    if (this.req) {
      this.req.destroy();
    }
  }
}

async function postPedido(pedidoData) {
  const res = await fetch(`${BASE_URL}/api/pedidos`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pedidoData)
  });
  const data = await res.json();
  return { status: res.status, ok: res.ok, data };
}

async function runTests() {
  console.log('================================================================');
  console.log(' INICIANDO VERIFICACIÓN AUTOMATIZADA: R1, R2, R6 y R7');
  console.log('================================================================\n');

  // 1. Health check
  console.log('[TEST 0] Verificando salud del servidor y estado Kafka...');
  const healthRes = await fetch(`${BASE_URL}/api/health`);
  const health = await healthRes.json();
  console.log('  -> Health response:', JSON.stringify(health));
  if (!health.kafka.productorConectado || !health.kafka.consumidorConectado) {
    throw new Error('El productor o consumidor KafkaJS no están conectados.');
  }
  console.log('  ✅ Health OK: Productor y Consumidor conectados al Broker\n');

  // Iniciar cliente SSE para pruebas en vivo
  console.log('[SSE] Abriendo canal SSE con /api/eventos...');
  const sse = new SimpleSSEClient(`${BASE_URL}/api/eventos`);
  await sse.connect();
  console.log('  ✅ Canal SSE conectado y listo\n');

  // 2. R1 — Productor y comprobante con datos reales
  console.log('----------------------------------------------------------------');
  console.log('[TEST R1] Productor y comprobante con metadatos reales de Kafka');
  console.log('----------------------------------------------------------------');
  const pedidoR1 = {
    cliente: 'Ana',
    producto: 'Café Latte Doble',
    cantidad: 2,
    nota: 'sin azúcar'
  };

  const resR1 = await postPedido(pedidoR1);
  console.log('  -> HTTP Status:', resR1.status);
  console.log('  -> Respuesta:', JSON.stringify(resR1.data, null, 2));

  if (!resR1.ok || !resR1.data.resultados || resR1.data.resultados.length === 0) {
    throw new Error(`R1 falló: Respuesta inválida ${JSON.stringify(resR1.data)}`);
  }

  const resultR1 = resR1.data.resultados[0];
  if (typeof resultR1.particion !== 'number' || typeof resultR1.offset !== 'number') {
    throw new Error(`R1 falló: Partición u offset no son numéricos reales`);
  }
  if (!resultR1.pedido.id || resultR1.pedido.cliente !== 'Ana') {
    throw new Error('R1 falló: Datos del pedido no coinciden');
  }

  console.log(`  ✅ R1 EXITOSO: Pedido confirmado en partición ${resultR1.particion}, offset ${resultR1.offset}`);
  console.log(`     ID Pedido: #${resultR1.pedido.id}, Creado en: ${resultR1.pedido.creado_en}\n`);

  // 3. R2 — Cocina en vivo con medición de latencia real
  console.log('----------------------------------------------------------------');
  console.log('[TEST R2] Cocina en vivo vía SSE y medición de latencia');
  console.log('----------------------------------------------------------------');
  const pedidoR2 = {
    cliente: 'Beatriz',
    producto: 'Espresso Ristretto',
    cantidad: 1,
    nota: 'taza caliente'
  };

  const t0 = performance.now();
  const resR2 = await postPedido(pedidoR2);
  const pedidoR2Id = resR2.data.resultados[0].pedido.id;

  // Esperar llegada en SSE
  const sseEventoR2 = await sse.waitForEvent(e => e.pedido && e.pedido.id === pedidoR2Id);
  const t1 = performance.now();
  const latenciaMedidaMs = Math.round(t1 - t0);

  console.log(`  -> Pedido emitido por Tienda: #${pedidoR2Id}`);
  console.log(`  -> Pedido recibido por SSE en Cocina: #${sseEventoR2.pedido.id}`);
  console.log(`  -> Metadatos Kafka recibidos: P${sseEventoR2.particion}@${sseEventoR2.offset}`);
  console.log(`  -> Tiempo total medido (POST -> Kafka -> SSE): ${latenciaMedidaMs} ms`);

  if (latenciaMedidaMs >= 2000) {
    throw new Error(`R2 falló: La latencia medida (${latenciaMedidaMs} ms) supera el umbral de 2000 ms.`);
  }

  console.log(`  ✅ R2 EXITOSO: Latencia real de ${latenciaMedidaMs} ms (< 2000 ms exigidos por la pauta)\n`);

  // 4. R6 — Afinidad de partición y preservación de orden por clave
  console.log('----------------------------------------------------------------');
  console.log('[TEST R6] Claves: Afinidad de partición y orden garantizado');
  console.log('----------------------------------------------------------------');
  const clienteR6 = 'CarlosMendoza';

  const resR6_1 = await postPedido({
    cliente: clienteR6,
    producto: 'Americano Intenso',
    cantidad: 1,
    nota: 'orden 1'
  });
  const resR6_2 = await postPedido({
    cliente: clienteR6,
    producto: 'Capuccino Vainilla',
    cantidad: 2,
    nota: 'orden 2'
  });

  const r6_1 = resR6_1.data.resultados[0];
  const r6_2 = resR6_2.data.resultados[0];

  console.log(`  -> Pedido 1 (${clienteR6}): Partición ${r6_1.particion}, Offset ${r6_1.offset}`);
  console.log(`  -> Pedido 2 (${clienteR6}): Partición ${r6_2.particion}, Offset ${r6_2.offset}`);

  if (r6_1.particion !== r6_2.particion) {
    throw new Error(`R6 falló: Misma clave '${clienteR6}' aterrizó en particiones distintas (${r6_1.particion} vs ${r6_2.particion})`);
  }
  if (r6_2.offset <= r6_1.offset) {
    throw new Error(`R6 falló: El offset no avanzó secuencialmente (${r6_1.offset} -> ${r6_2.offset})`);
  }

  console.log(`  ✅ R6 EXITOSO: Ambos pedidos compartieron la Partición ${r6_1.particion} y mantuvieron el orden estricto (Offset ${r6_1.offset} < ${r6_2.offset})\n`);

  // 5. R7 — Mensaje mal formado y tolerancia a fallos
  console.log('----------------------------------------------------------------');
  console.log('[TEST R7] Mensaje mal formado "hola" y resiliencia del consumidor');
  console.log('----------------------------------------------------------------');
  console.log('  -> Inyectando "hola" en topic pedidos mediante Kafka Producer...');

  const { execSync } = require('child_process');
  execSync('echo hola | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic pedidos');

  console.log('  -> Mensaje mal formado inyectado. Esperando detección en SSE...');
  const eventoMalformado = await sse.waitForEvent(e => e.tipo === 'malformado' && e.raw && e.raw.includes('hola'), 8000);

  console.log('  -> Evento SSE recibido para mensaje mal formado:');
  console.log(`     - Tipo: "${eventoMalformado.tipo}"`);
  console.log(`     - Mensaje: "${eventoMalformado.mensaje}"`);
  console.log(`     - Raw: "${eventoMalformado.raw}"`);
  console.log(`     - Partición: ${eventoMalformado.particion}`);

  if (eventoMalformado.mensaje !== 'No es un pedido') {
    throw new Error(`R7 falló: Mensaje esperado 'No es un pedido', recibido '${eventoMalformado.mensaje}'`);
  }

  console.log('  -> Verificando que el consumidor continúa vivo enviando un pedido válido posterior...');
  const resR7_post = await postPedido({
    cliente: 'DavidResiliente',
    producto: 'Mocha Chocolate',
    cantidad: 1,
    nota: 'post-malformado'
  });
  const idPostMalformado = resR7_post.data.resultados[0].pedido.id;

  const eventoPostMalformado = await sse.waitForEvent(e => e.pedido && e.pedido.id === idPostMalformado, 8000);
  console.log(`  -> Pedido posterior #${eventoPostMalformado.pedido.id} consumido exitosamente en P${eventoPostMalformado.particion}@${eventoPostMalformado.offset}`);
  console.log('  ✅ R7 EXITOSO: Mensaje mal formado identificado como "No es un pedido" y consumidor 100% operativo sin colapsar\n');

  sse.close();

  console.log('================================================================');
  console.log(' ¡TODAS LAS PRUEBAS DE CALIDAD SUPERADAS SATISFACTORIAMENTE!');
  console.log(' Requisitos validados: R1 (OK), R2 (OK), R6 (OK), R7 (OK)');
  console.log('================================================================');
}

runTests().catch((err) => {
  console.error('\n❌ ERROR EN QUALITY GATE:', err);
  process.exit(1);
});

