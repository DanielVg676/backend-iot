import { query } from "../../config/db";
import { TelemetryRecord } from "../../types/telemetry.types";

interface TelemetryDbFilters {
  collarUuid: string;
  tenantId?: string;
  from?: string; // ISO
  to?: string;   // ISO
  limit?: number;
  offset?: number;
}

export async function getTelemetryForCollar(filters: TelemetryDbFilters): Promise<TelemetryRecord[]> {
  const { collarUuid, tenantId, from, to, limit = 100, offset = 0 } = filters;

  const params: any[] = [collarUuid];
  const conditions: string[] = ["collar_uuid = $1"]; // usamos siempre UUID
  let paramIndex = params.length;

  if (tenantId) {
    params.push(tenantId);
    paramIndex = params.length;
    conditions.push(`tenant_id = $${paramIndex}`);
  }
  if (from) {
    params.push(from);
    paramIndex = params.length;
    conditions.push(`timestamp >= $${paramIndex}`);
  }
  if (to) {
    params.push(to);
    paramIndex = params.length;
    conditions.push(`timestamp <= $${paramIndex}`);
  }

  params.push(limit);
  const limitIndex = params.length;
  params.push(offset);
  const offsetIndex = params.length;

  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const sql = `
    SELECT
      id,
      collar_uuid,
      collar_id,
      tenant_id,
      animal_id,
      latitude,
      longitude,
      altitude,
      speed,
      temperature,
      activity,
      bat_voltage,
      bat_percent,
      accel_x,
      accel_y,
      accel_z,
      gyro_x,
      gyro_y,
      gyro_z,
      rssi,
      snr,
      timestamp
    FROM telemetry
    ${whereClause}
    ORDER BY timestamp DESC
    LIMIT $${limitIndex}
    OFFSET $${offsetIndex}
  `;

  const { rows } = await query<TelemetryRecord>(sql, params);
  return rows;
}

export async function getLatestTelemetryForCollar(collarUuid: string, tenantId?: string): Promise<TelemetryRecord | null> {
  const params: any[] = [collarUuid];
  const conditions: string[] = ["collar_uuid = $1"]; // usamos siempre UUID
  let paramIndex = params.length;

  if (tenantId) {
    params.push(tenantId);
    paramIndex = params.length;
    conditions.push(`tenant_id = $${paramIndex}`);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const sql = `
    SELECT
      id,
      collar_uuid,
      collar_id,
      tenant_id,
      animal_id,
      latitude,
      longitude,
      altitude,
      speed,
      temperature,
      activity,
      bat_voltage,
      bat_percent,
      accel_x,
      accel_y,
      accel_z,
      gyro_x,
      gyro_y,
      gyro_z,
      rssi,
      snr,
      timestamp
    FROM telemetry
    ${whereClause}
    ORDER BY timestamp DESC
    LIMIT 1
  `;

  const { rows } = await query<TelemetryRecord>(sql, params);
  return rows[0] || null;
}

interface InsertTelemetryInput {
  collarUuid: string;
  collarId: string; // identificador IoT
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
  timestamp: string; // ISO
}

export async function insertTelemetry(input: InsertTelemetryInput): Promise<void> {
  const sql = `
    INSERT INTO telemetry (
      collar_uuid,
      collar_id,
      tenant_id,
      animal_id,
      latitude,
      longitude,
      altitude,
      speed,
      temperature,
      activity,
      bat_voltage,
      bat_percent,
      accel_x,
      accel_y,
      accel_z,
      gyro_x,
      gyro_y,
      gyro_z,
      rssi,
      snr,
      timestamp
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
    )
  `;

  const params = [
    input.collarUuid,
    input.collarId,
    input.tenantId ?? null,
    input.animalId ?? null,
    input.latitude ?? null,
    input.longitude ?? null,
    input.altitude ?? null,
    input.speed ?? null,
    input.temperature ?? null,
    input.activity ?? null,
    input.batVoltage ?? null,
    input.batPercent ?? null,
    input.accelX ?? null,
    input.accelY ?? null,
    input.accelZ ?? null,
    input.gyroX ?? null,
    input.gyroY ?? null,
    input.gyroZ ?? null,
    input.rssi ?? null,
    input.snr ?? null,
    input.timestamp,
  ];

  await query(sql, params);
}
