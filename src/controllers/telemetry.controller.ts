import { Request, Response, NextFunction } from "express";
import {
  getHistoricalTelemetryByCollar,
  getRealtimeTelemetryByCollar,
  getRealtimeTelemetryFromCache,
  processIncomingTelemetry,
} from "../services/telemetry.service";
import { ensureString, optionalNumber } from "../utils/validation";

export async function postTelemetry(req: Request, res: Response, next: NextFunction) {
  try {
    const body = req.body || {};

    const collarId = ensureString(body.collarId ?? body.collar_id, "collarId");

    await processIncomingTelemetry({
      collarId,
      tenantId: body.tenantId ?? body.tenant_id ?? null,
      animalId: body.animalId ?? body.animal_id ?? null,
      latitude: body.latitude != null ? Number(body.latitude) : null,
      longitude: body.longitude != null ? Number(body.longitude) : null,
      altitude: body.altitude != null ? Number(body.altitude) : null,
      speed: body.speed != null ? Number(body.speed) : null,
      temperature: body.temperature != null ? Number(body.temperature) : null,
      activity: body.activity ?? null,
      batVoltage: body.batVoltage ?? body.bat_voltage ?? null,
      batPercent: body.batPercent ?? body.bat_percent ?? null,
      accelX: body.accelX ?? body.accel_x ?? null,
      accelY: body.accelY ?? body.accel_y ?? null,
      accelZ: body.accelZ ?? body.accel_z ?? null,
      gyroX: body.gyroX ?? body.gyro_x ?? null,
      gyroY: body.gyroY ?? body.gyro_y ?? null,
      gyroZ: body.gyroZ ?? body.gyro_z ?? null,
      rssi: body.rssi ?? null,
      snr: body.snr ?? null,
      timestamp: body.timestamp,
    });

    res.status(201).json({ ok: true });
  } catch (err) {
    next(err);
  }
}

export async function getRealtimeByCollar(req: Request, res: Response, next: NextFunction) {
  try {
    const collarId = ensureString(req.params.collarId, "collarId (UUID)");
    const tenantId = req.query.tenantId ? String(req.query.tenantId) : undefined;

    const snapshot = await getRealtimeTelemetryByCollar(collarId, tenantId);

    if (!snapshot) {
      return res.status(404).json({
        error: true,
        message: "No hay datos de telemetría para este collar",
      });
    }

    res.json(snapshot);
  } catch (err) {
    next(err);
  }
}

export async function getHistoricalByCollar(req: Request, res: Response, next: NextFunction) {
  try {
    const collarId = ensureString(req.params.collarId, "collarId (UUID)");
    const tenantId = req.query.tenantId ? String(req.query.tenantId) : undefined;

    const from = req.query.from ? String(req.query.from) : undefined;
    const to = req.query.to ? String(req.query.to) : undefined;
    const limit = optionalNumber(req.query.limit);
    const page = optionalNumber(req.query.page);

    const result = await getHistoricalTelemetryByCollar(
      collarId,
      {
        from,
        to,
        limit,
        page,
      },
      tenantId
    );

    res.json(result);
  } catch (err) {
    next(err);
  }
}

// Server-Sent Events: stream de telemetría en tiempo real
export async function streamRealtimeByCollar(req: Request, res: Response, next: NextFunction) {
  try {
    const collarId = ensureString(req.params.collarId, "collarId (UUID)");
    const tenantId = req.query.tenantId ? String(req.query.tenantId) : undefined;

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");

    // Enviar primer snapshot inmediato SOLO desde Redis (si existe)
    const initial = await getRealtimeTelemetryFromCache(collarId, tenantId);
    let lastTimestamp: string | undefined = undefined;

    if (initial) {
      lastTimestamp = initial.timestamp;
      res.write(`data: ${JSON.stringify(initial)}\n\n`);
    }

    const interval = setInterval(async () => {
      try {
        // Consultar únicamente Redis para no cargar la BD
        const snapshot = await getRealtimeTelemetryFromCache(collarId, tenantId);
        if (!snapshot) return;

        if (snapshot.timestamp !== lastTimestamp) {
          lastTimestamp = snapshot.timestamp;
          res.write(`data: ${JSON.stringify(snapshot)}\n\n`);
        }
      } catch (err) {
        console.error("[SSE] Error obteniendo telemetría:", err);
      }
    }, 30000); // cada 30 segundos

    req.on("close", () => {
      clearInterval(interval);
    });
  } catch (err) {
    next(err);
  }
}
