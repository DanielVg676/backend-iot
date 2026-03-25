import { Router } from "express";
import {
  getHistoricalByCollar,
  getRealtimeByCollar,
  getHighFreqRealtimeFromRedis,
  postTelemetry,
  getProducerCollarsTelemetry,
  streamTenantCollarsFromRedis,
  getTenantCollarsTelemetry,
} from "../controllers/telemetry.controller";
import { streamRealtimeByCollar } from "../controllers/telemetry.controller";

const router = Router();

// Ingesta de telemetría desde collares o gateway
router.post("/telemetry", postTelemetry);

// Tiempo real (Redis + fallback a última medición)
router.get("/collars/:collarId/realtime", getRealtimeByCollar);
router.get("/collars/:collarId/realtime/stream", streamRealtimeByCollar);

// Tiempo real directo desde Redis (clave cow:{collarId})
router.get("/collars/:collarId/redis/realtime", getHighFreqRealtimeFromRedis);

// Histórico del collar
router.get("/collars/:collarId/history", getHistoricalByCollar);

// Telemetría de todos los collares de un productor (última medición por collar, Postgres)
router.get("/producers/:producerId/collars/telemetry", getProducerCollarsTelemetry);

// Stream SSE: telemetría desde Redis de todos los collares asignados a un tenant
router.get("/tenants/:tenantId/collars/realtime/stream", streamTenantCollarsFromRedis);

// Telemetría de todos los collares de un tenant (última medición por collar, Postgres)
router.get("/tenants/:tenantId/collars/telemetry", getTenantCollarsTelemetry);

export default router;
