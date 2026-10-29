/**
 * cocina.js - Lógica del Monitor de Cocina (KDS)
 * Cafetería Kafka — Duoc UC
 * 
 * Cumple con principios de interface-design, frontend-design y better-ui
 */

document.addEventListener('DOMContentLoaded', () => {
  const statusPill = document.getElementById('status-pill');
  const statusIndicator = document.getElementById('status-indicator');
  const statusText = document.getElementById('status-text');
  const contadorPedidos = document.getElementById('contador-pedidos');
  const horaActualizacion = document.getElementById('hora-actualizacion');
  const pedidosGrid = document.getElementById('pedidos-grid');
  const emptyState = document.getElementById('empty-state');
  const btnTogglePreview = document.getElementById('btn-toggle-preview');

  let totalPedidos = 0;
  let eventSource = null;
  let previewActive = false;
  const eventosRegistrados = new Set();

  // Identificar instancia visual de Cocina según puerto activo
  const activePort = window.location.port;
  const brandTag = document.querySelector('.brand-tag');
  if (brandTag) {
    if (activePort === '8083') {
      brandTag.textContent = 'KDS Cocina 2 (:8083)';
    } else if (activePort === '8081') {
      brandTag.textContent = 'KDS Cocina 1 (:8081)';
    }
  }

  // Función de escape para neutralizar cualquier inyección HTML en contenido no confiable
  function escaparHtml(cadena) {
    if (cadena === null || cadena === undefined) return '';
    return String(cadena)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // ==========================================================================
  // GESTIÓN DE CONEXIÓN SSE (DIFERENCIADA DE KAFKA)
  // ==========================================================================

  function actualizarEstadoConexion(estado, mensaje) {
    if (!statusPill) return;

    statusPill.className = `conn-pill status-${estado}`;
    statusText.innerHTML = mensaje;
  }

  function conectarSSE() {
    actualizarEstadoConexion('connecting', 'Conectando canal SSE...');

    eventSource = new EventSource('/api/eventos');

    eventSource.onopen = () => {
      actualizarEstadoConexion(
        'connected',
        'Canal SSE Activo &bull; Servidor Express conectado (Sincronizado con Kafka)'
      );
    };

    eventSource.onerror = (err) => {
      console.warn('Interrupción en canal SSE de Cocina:', err);
      actualizarEstadoConexion(
        'disconnected',
        'Desconectado de Express &bull; Reintentando conexión...'
      );
    };

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        procesarEvento(data);
      } catch (err) {
        console.error('Error al decodificar evento SSE:', err, event.data);
      }
    };
  }

  // ==========================================================================
  // PROCESAMIENTO DE EVENTOS EN TIEMPO REAL
  // ==========================================================================

  function procesarEvento(evento) {
    const ahora = new Date().toLocaleTimeString();
    if (horaActualizacion) horaActualizacion.textContent = ahora;

    // Mensaje de saludo/handshake del servidor
    if (evento.tipo === 'conexion' || evento.tipo === 'sistema') {
      console.log('Evento de sistema SSE recibido:', evento.mensaje);
      return;
    }

    // Deduplicación para evitar duplicar tarjetas si el cliente se reconecta o recibe replay del buffer
    const idEvento = `${evento.particion ?? 'x'}-${evento.offset ?? 'x'}-${evento.pedido?.id || evento.raw || Math.random()}`;
    if (eventosRegistrados.has(idEvento)) {
      return;
    }
    eventosRegistrados.add(idEvento);

    // Manejar mensajes mal formados (Requisito R7)
    if (evento.tipo === 'malformado' || evento.valido === false) {
      ocultarEstadoVacio();
      totalPedidos++;
      if (contadorPedidos) contadorPedidos.textContent = totalPedidos;
      renderizarTarjetaMalformada(evento, ahora);
      return;
    }

    // Pedido confirmado y recibido desde Kafka (Requisitos R2, R3, R4, R6)
    if (evento.pedido || evento.producto) {
      const pedido = evento.pedido || evento;
      ocultarEstadoVacio();
      totalPedidos++;
      if (contadorPedidos) contadorPedidos.textContent = totalPedidos;
      renderizarTarjetaComanda(pedido, evento.particion, evento.offset, ahora, false, evento);
    }
  }

  function ocultarEstadoVacio() {
    if (emptyState && emptyState.style.display !== 'none') {
      emptyState.style.display = 'none';
    }
  }

  // ==========================================================================
  // RENDERIZADO VISUAL DE TARJETAS KDS (better-ui)
  // ==========================================================================

  function renderizarTarjetaComanda(pedido, particion, offset, horaLlegada, esMuestra = false, eventoContexto = {}) {
    const card = document.createElement('article');
    card.className = esMuestra ? 'kds-card kds-sample-preview' : 'kds-card';

    const idTexto = pedido.id ? `#${escaparHtml(pedido.id)}` : '#---';
    const clienteTexto = escaparHtml(pedido.cliente || 'Cliente Anónimo');
    const productoTexto = escaparHtml(pedido.producto || 'Producto');
    const cantidadTexto = escaparHtml(String(pedido.cantidad || 1));
    const notaHtml = pedido.nota ? `<div class="kds-note-box">Nota: "${escaparHtml(pedido.nota)}"</div>` : '';

    const iconClock = window.LucideIcons ? window.LucideIcons.get('clock', { size: 12 }) : '';
    const iconLayers = window.LucideIcons ? window.LucideIcons.get('layers', { size: 12 }) : '';

    const metaKafka = (particion !== undefined && offset !== undefined)
      ? `<span class="kds-partition-tag">${iconLayers} Partición ${particion} &bull; Offset ${offset}</span>`
      : `<span class="kds-partition-tag">Broker: En cola</span>`;

    // Distinción visual: Muestra de diseño vs Histórico vs Nuevo en vivo
    let badgeTipo = '';
    if (esMuestra) {
      badgeTipo = `<span style="font-size:0.65rem; background:#eee; padding:2px 5px; border-radius:3px; color:#555;">[Muestra Diseño]</span>`;
    } else if (pedido.historico || eventoContexto.historico) {
      badgeTipo = `<span style="font-size:0.65rem; background:#e0e7ff; color:#3730a3; padding:2px 5px; border-radius:3px; font-weight:600;">[Histórico]</span>`;
    }

    // Latencia visible (R2) si está disponible y es un pedido en vivo
    const latenciaTag = (eventoContexto.latencia_ms !== undefined && !eventoContexto.historico && !esMuestra)
      ? `<span style="font-size:0.68rem; color:var(--text-muted); margin-left:6px;" title="Tiempo de entrega">${eventoContexto.latencia_ms}ms</span>`
      : '';

    card.innerHTML = `
      <div class="kds-card-header">
        <span class="kds-order-badge">${idTexto}</span>
        <div class="kds-time-chip">
          ${badgeTipo}
          ${iconClock}
          <span class="tabular-nums">${horaLlegada}</span>
          ${latenciaTag}
        </div>
      </div>
      <div class="kds-card-body">
        <div class="kds-customer-name">${clienteTexto}</div>
        <div class="kds-item-row">
          <span class="kds-qty-badge tabular-nums">${cantidadTexto}x</span>
          <span class="kds-product-name">${productoTexto}</span>
        </div>
        ${notaHtml}
      </div>
      <div class="kds-card-footer">
        <span>Canal: SSE</span>
        ${metaKafka}
      </div>
    `;

    pedidosGrid.prepend(card);
  }

  function renderizarTarjetaMalformada(evento, horaLlegada, esMuestra = false) {
    const card = document.createElement('article');
    card.className = esMuestra ? 'kds-card card-malformed kds-sample-preview' : 'kds-card card-malformed';

    const textoBruto = escaparHtml(evento.raw || evento.mensaje || '(cadena inválida)');
    const iconAlert = window.LucideIcons ? window.LucideIcons.get('alert-triangle', { size: 16 }) : '';
    const iconClock = window.LucideIcons ? window.LucideIcons.get('clock', { size: 12 }) : '';

    const badgeTipo = esMuestra 
      ? `<span style="font-size:0.65rem; background:#eee; padding:2px 5px; border-radius:3px; color:#555;">[Muestra]</span>`
      : (evento.historico ? `<span style="font-size:0.65rem; background:#fee2e2; color:#991b1b; padding:2px 5px; border-radius:3px; font-weight:600;">[Histórico]</span>` : '');

    card.innerHTML = `
      <div class="kds-card-header">
        <span class="kds-order-badge">ALERTA R7</span>
        <div class="kds-time-chip">
          ${badgeTipo}
          ${iconClock}
          <span class="tabular-nums">${horaLlegada}</span>
        </div>
      </div>
      <div class="kds-card-body">
        <div class="kds-malformed-alert">
          ${iconAlert}
          <span>Mensaje Mal Formado (No es un pedido)</span>
        </div>
        <div class="kds-malformed-payload">
          <code>${textoBruto}</code>
        </div>
        <div style="font-size: 0.78rem; color: var(--text-muted); margin-top: 0.5rem;">
          El servicio KDS continúa operativo sin interrupción.
        </div>
      </div>
      <div class="kds-card-footer">
        <span>Estado: Descartado</span>
        <span class="kds-partition-tag">Partición: ${evento.particion ?? 'N/A'}</span>
      </div>
    `;

    pedidosGrid.prepend(card);
  }



  // ==========================================================================
  // MODO INSPECCIÓN VISUAL (FIXTURES AISLADOS DE PRODUCCIÓN)
  // ==========================================================================

  if (btnTogglePreview) {
    btnTogglePreview.addEventListener('click', () => {
      previewActive = !previewActive;

      const muestrasExistentes = document.querySelectorAll('.kds-sample-preview');
      muestrasExistentes.forEach((el) => el.remove());

      if (previewActive) {
        btnTogglePreview.innerHTML = `
          <span class="icon" data-lucide="sparkles" data-size="14"></span>
          <span>Ocultar tarjetas de muestra</span>
        `;

        const ahora = new Date().toLocaleTimeString();

        // 1. Muestra de comanda regular
        renderizarTarjetaComanda(
          {
            id: 'A3F2',
            cliente: 'Ana Valenzuela',
            producto: 'Café Latte Doble',
            cantidad: 2,
            nota: 'Leche de avena, sin azúcar'
          },
          1,
          42,
          ahora,
          true
        );

        // 2. Muestra de comanda simple
        renderizarTarjetaComanda(
          {
            id: 'B8C1',
            cliente: 'Carlos Mendoza',
            producto: 'Medialuna de Manteca',
            cantidad: 3,
            nota: ''
          },
          0,
          89,
          ahora,
          true
        );

        // 3. Muestra de alerta R7
        renderizarTarjetaMalformada(
          {
            raw: 'hola (mensaje de texto plano)',
            particion: 2
          },
          ahora,
          true
        );

      } else {
        btnTogglePreview.innerHTML = `
          <span class="icon" data-lucide="sparkles" data-size="14"></span>
          <span>Verificar tarjetas de muestra (Modo Diseño)</span>
        `;
      }

      if (window.LucideIcons) window.LucideIcons.render();
    });
  }

  // Iniciar conexión SSE
  conectarSSE();
});
