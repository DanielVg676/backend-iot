import { getRedisClient } from "../config/redis";
import {
  TelemetryHistoryQuery,
  TelemetryHistoryResponse,
  TelemetrySnapshot,
} from "../types/telemetry.types";
import {
  getLatestTelemetryForCollar,
  getTelemetryForCollar,
  insertTelemetry,
} from "./db/telemetry.db.service";
import { getCollarByCollarId } from "./db/collar.db.service";

const REALTIME_TTL_SECONDS = 60 * 5; // 5 minutos

export async function getRealtimeTelemetryByCollar(
  collarUuid: string,
  tenantId?: string
): Promise<TelemetrySnapshot | null> {
  // 1) Intentar leer desde Redis
  try {
    const redis = getRedisClient();
    const key = getRealtimeKey(collarUuid, tenantId);
    const raw = await redis.get(key);
    if (raw) {
      return JSON.parse(raw) as TelemetrySnapshot;
    }
  } catch (err) {
    console.error("[TelemetryService] Error leyendo Redis:", err);
  }

  // 2) Fallback a última medición en BD
  const latest = await getLatestTelemetryForCollar(collarUuid, tenantId);
  if (!latest) return null;

  const snapshot: TelemetrySnapshot = {
    collarId: latest.collar_id,
    animalId: latest.animal_id ?? undefined,
    position:
      latest.latitude != null && latest.longitude != null
        ? {
            lat: latest.latitude,
            lng: latest.longitude,
            alt: latest.altitude ?? undefined,
          }
        : undefined,
    battery: {
      percent: latest.bat_percent ?? undefined,
      voltage: latest.bat_voltage ?? undefined,
    },
    activity: latest.activity ?? undefined,
    rssi: latest.rssi ?? undefined,
    snr: latest.snr ?? undefined,
    timestamp: latest.timestamp,
  };

  // Guardar en Redis para siguientes lecturas
  try {
    const redis = getRedisClient();
    const key = getRealtimeKey(collarUuid, tenantId);
    await redis.set(key, JSON.stringify(snapshot), {
      EX: REALTIME_TTL_SECONDS,
    });
  } catch (err) {
    console.error("[TelemetryService] Error escribiendo snapshot en Redis:", err);
  }

  return snapshot;
}

export async function getRealtimeTelemetryFromCacheByCollarId(
  collarId: string,
  tenantId?: string
): Promise<TelemetrySnapshot | null> {
  try {
    const redis = getRedisClient();
    const key = getRealtimeKeyByCollarId(collarId, tenantId);
    const raw = await redis.get(key);
    if (!raw) return null;
    return JSON.parse(raw) as TelemetrySnapshot;
  } catch (err) {
    console.error("[TelemetryService] Error leyendo solo desde Redis (por collar_id):", err);
    return null;
  }
}

export async function getHistoricalTelemetryByCollar(
  collarUuid: string,
  query: TelemetryHistoryQuery,
  tenantId?: string
): Promise<TelemetryHistoryResponse> {
  const page = query.page && query.page > 0 ? query.page : 1;
  const pageSize = query.limit && query.limit > 0 ? query.limit : 100;
  const offset = (page - 1) * pageSize;

  let { from, to } = query;

  // Si no se envían fechas, usar últimas 24h por defecto
  if (!from && !to) {
    const toDate = new Date();
    const fromDate = new Date(toDate.getTime() - 24 * 60 * 60 * 1000);
    from = fromDate.toISOString();
    to = toDate.toISOString();
  }

  const rows = await getTelemetryForCollar({
    collarUuid,
    tenantId,
    from,
    to,
    limit: pageSize + 1, // pedir uno más para saber si hay siguiente página
    offset,
  });

  const hasNextPage = rows.length > pageSize;
  const slice = rows.slice(0, pageSize);

  const data: TelemetrySnapshot[] = slice.map((row) => ({
    collarId: row.collar_id,
    animalId: row.animal_id ?? undefined,
    position:
      row.latitude != null && row.longitude != null
        ? {
            lat: row.latitude,
            lng: row.longitude,
            alt: row.altitude ?? undefined,
          }
        : undefined,
    battery: {
      percent: row.bat_percent ?? undefined,
      voltage: row.bat_voltage ?? undefined,
    },
    activity: row.activity ?? undefined,
    rssi: row.rssi ?? undefined,
    snr: row.snr ?? undefined,
    timestamp: row.timestamp,
  }));

  return {
    data,
    page,
    pageSize,
    hasNextPage,
  };
}

interface ProcessIncomingTelemetryInput {
  collarId: string; // identificador IoT (collar_id)
  tenantId?: string | null;
  animalId?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  altitude?: number | null;
  speed?: number | null;
  temperature?: number | null;
  activity?: string | null;
  batVoltage?: number | null;
  batPercent?: number | null;
  accelX?: number | null;
  accelY?: number | null;
  accelZ?: number | null;
  gyroX?: number | null;
  gyroY?: number | null;
  gyroZ?: number | null;
  rssi?: number | null;
  snr?: number | null;
  timestamp?: string; // opcional, si no viene usamos now
}

export async function processIncomingTelemetry(input: ProcessIncomingTelemetryInput): Promise<void> {
  if (!input.collarId) {
    throw new Error("collarId es obligatorio en la telemetría entrante");
  }

  const timestamp = input.timestamp ?? new Date().toISOString();

  // Resolver UUID del collar a partir del identificador IoT
  const collar = await getCollarByCollarId(input.collarId, input.tenantId ?? undefined);
  if (!collar) {
    throw new Error("Collar no encontrado para el identificador proporcionado");
  }

  await insertTelemetry({
    collarUuid: collar.id,
    collarId: collar.collar_id,
    tenantId: input.tenantId ?? null,
    animalId: input.animalId ?? null,
    latitude: input.latitude ?? null,
    longitude: input.longitude ?? null,
    altitude: input.altitude ?? null,
    speed: input.speed ?? null,
    temperature: input.temperature ?? null,
    activity: input.activity ?? null,
    batVoltage: input.batVoltage ?? null,
    batPercent: input.batPercent ?? null,
    accelX: input.accelX ?? null,
    accelY: input.accelY ?? null,
    accelZ: input.accelZ ?? null,
    gyroX: input.gyroX ?? null,
    gyroY: input.gyroY ?? null,
    gyroZ: input.gyroZ ?? null,
    rssi: input.rssi ?? null,
    snr: input.snr ?? null,
    timestamp,
  });

  const snapshot: TelemetrySnapshot = {
    collarId: collar.collar_id,
    animalId: input.animalId ?? undefined,
    position:
      input.latitude != null && input.longitude != null
        ? {
            lat: input.latitude,
            lng: input.longitude,
            alt: input.altitude ?? undefined,
          }
        : undefined,
    battery: {
      percent: input.batPercent ?? undefined,
      voltage: input.batVoltage ?? undefined,
    },
    activity: input.activity ?? undefined,
    rssi: input.rssi ?? undefined,
    snr: input.snr ?? undefined,
    timestamp,
  };

  try {
    const redis = getRedisClient();
    const keyUuid = getRealtimeKey(collar.id, input.tenantId ?? undefined);
    await redis.set(keyUuid, JSON.stringify(snapshot), {
      EX: REALTIME_TTL_SECONDS,
    });

    const keyCollarId = getRealtimeKeyByCollarId(collar.collar_id, input.tenantId ?? undefined);
    await redis.set(keyCollarId, JSON.stringify(snapshot), {
      EX: REALTIME_TTL_SECONDS,
    });
  } catch (err) {
    console.error("[TelemetryService] No se pudo actualizar Redis con snapshot:", err);
  }
}

function getRealtimeKey(collarUuid: string, tenantId?: string) {
  return tenantId
    ? `telemetry:tenant:${tenantId}:collarUuid:${collarUuid}`
    : `telemetry:collarUuid:${collarUuid}`;
}

function getRealtimeKeyByCollarId(collarId: string, tenantId?: string) {
  return tenantId
    ? `telemetry:tenant:${tenantId}:collarId:${collarId}`
    : `telemetry:collarId:${collarId}`;
}

// Lectura directa de la telemetría "raw" almacenada en Redis por el pipeline IoT,
// usando la clave cow:{collarId} como HASH. Esta función NO consulta la base de datos.
export async function getHighFreqTelemetryFromRedis(
  collarId: string
): Promise<unknown | null> {
  try {
    const redis = getRedisClient();
    const key = `cow:${collarId}`;
    const hash = await redis.hGetAll(key);

    // hGetAll devuelve {} si la key no existe o no es un hash
    if (!hash || Object.keys(hash).length === 0) {
      return null;
    }

    return hash;
  } catch (err) {
    console.error("[TelemetryService] Error leyendo telemetría high-freq desde Redis:", err);
    return null;
  }
}
