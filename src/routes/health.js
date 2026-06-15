import { Router } from "express";

function healthRouter() {
  const router = new Router();
  router.get("/", (_req, res) => {
    res.json({ status: "ok", version: "0.1.0" });
  });
  return router;
}

export { healthRouter };
