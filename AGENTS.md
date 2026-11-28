# Convenciones del adaptador Home Assistant

## Arquitectura

- Este proyecto usa únicamente sus contratos y formatos actuales. No añadir
  migraciones, adopciones ni fallbacks para datos o comportamientos retirados.
  La recuperación del formato actual, la identidad de cada reporte y el ACK
  durable siguen siendo requisitos obligatorios.
- Seguir `custom-components/blueprint` y las APIs de la versión mínima declarada
  en `hacs.json`. La atribución de la plantilla está en `NOTICE`.
- Configuración mediante config entry y flujos nativos; cliente HTTP con la sesión
  compartida de HA. Guardar runtime tipado en `entry.runtime_data`.
- Snapshots de entidades mediante `DataUpdateCoordinator`/`CoordinatorEntity`.
  Los fallos de lectura representan indisponibilidad, nunca stock o límites cero.
- Comandos de negocio solo a través de la API versionada del servidor Alchemy.
  Conservar UUID, revisión y payload al reintentar; devolver el recibo aceptado
  aunque falle el refresco posterior. Mantener el control de administrador de HA.
- El adaptador Python no implementa reglas de producción, incidencias o alertas.
  No accede a D1 ni incorpora un runtime de servidor paralelo.

## Actualizaciones y recursos

- Escuchar tanto cambios de estado como reportes idénticos con helpers nativos.
  Copiar inmediatamente `State`, valor, unidad y tiempo. Nunca coalescer por valor,
  entidad o timestamp; un lote es solo una agrupación de transporte.
- Persistencia local exclusivamente con `homeassistant.helpers.storage.Store`
  versionado, privado y atómico. UUID, payload, muestras y cursores se conservan
  juntos. Verificar guardados antes de retirar datos: Store puede registrar un
  fallo sin propagarlo. No añadir una base SQL manual.
- Las tareas pertenecen a la config entry; retirar suscripciones al descargar.
  Esperar escrituras en curso y usar el ciclo `FINAL_WRITE` de HA al apagar.
  Conservar la reserva acotada para vaciar RAM cuando la cola esté llena.
- Recorder es recuperación complementaria, no fuente de reportes idénticos que
  no guardó. Consultar páginas acotadas, excluir estados sintéticos y mantener
  el tiempo original y modo histórico.
- Usar errores/diagnósticos seguros: no incluir claves, URL privada, cuerpos de
  respuesta ni datos de lotes. Validar contrato, ACK y redirecciones HTTP.

## Verificación

- `uv sync --locked` y `sh scripts/check.sh`: Ruff, compilación, pruebas con HA
  real y paquete. Para cambios de ciclo de vida o entrega, comprobar descarga,
  reinicio, ACK perdido y fallos reales de almacenamiento/transporte.
- Mantener Hassfest y HACS en CI. Validar también contra la versión mínima,
  especialmente manifiesto, config flows, traducciones y dependencias.
- Versiones de `manifest.json`, `pyproject.toml` y lock consistentes. El ZIP de
  instalación manual conserva estructura `custom_components/registros_camaras`
  y atribuciones. HACS usa esa carpeta del repositorio.
- Registrar trabajo durable en Beads del repositorio principal. No publicar
  releases ni instalar en HA operativo sin una instrucción explícita.
