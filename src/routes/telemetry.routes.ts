import { Router } from "express";
import {
  getHistoricalByCollar,
  getRealtimeByCollar,
  getHighFreqRealtimeFromRedis,
  postTelemetry,
} from "../controllers/telemetry.controller";
import {
  streamRealtimeByCollar,
} from "../controllers/telemetry.controller";

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

export default router;
