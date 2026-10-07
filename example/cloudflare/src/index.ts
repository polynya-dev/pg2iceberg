// pg2iceberg on Cloudflare Containers: one container, running
// `pg2iceberg run` for as long as the Worker is deployed.
//
// pg2iceberg serves no HTTP. The container is started from the Worker —
// by a cron trigger every minute, which also starts it again after
// Cloudflare stops it (a host restart, a deploy) — and it never idles out.
// pg2iceberg keeps its state in Postgres, so a restart resumes where it
// left off.

import { Container, getContainer } from "@cloudflare/containers";

interface Env {
  PG2ICEBERG: DurableObjectNamespace<Pg2Iceberg>;
  // Secrets (`wrangler secret put`).
  POSTGRES_URL: string;
  ICEBERG_CATALOG_TOKEN: string;
  // Vars (wrangler.jsonc).
  ICEBERG_CATALOG_URL: string;
  ICEBERG_WAREHOUSE: string;
  ICEBERG_NAMESPACE: string;
}

// One name, so one Durable Object and one container: a replication slot
// has one consumer.
const INSTANCE = "pg2iceberg";

export class Pg2Iceberg extends Container<Env> {
  // No `defaultPort`: there's nothing to serve.
  sleepAfter = "1h";

  // Never idle out: without a stop here, the activity timer renews.
  override async onActivityExpired() {}

  override onStart() {
    console.log("pg2iceberg started");
  }

  override onStop(params: unknown) {
    console.log("pg2iceberg stopped; the cron trigger starts it again", params);
  }

  override onError(error: unknown) {
    console.error("pg2iceberg failed", error);
    throw error;
  }

  /** Start pg2iceberg unless it's running. */
  async ensureRunning(): Promise<string> {
    if (this.ctx.container?.running) {
      return "running";
    }
    await this.start({
      entrypoint: ["/usr/bin/tini", "--", "/usr/local/bin/pg2iceberg", "run"],
      envVars: {
        POSTGRES_URL: this.env.POSTGRES_URL,
        ICEBERG_CATALOG_URL: this.env.ICEBERG_CATALOG_URL,
        ICEBERG_CATALOG_TOKEN: this.env.ICEBERG_CATALOG_TOKEN,
        ICEBERG_WAREHOUSE: this.env.ICEBERG_WAREHOUSE,
        ICEBERG_NAMESPACE: this.env.ICEBERG_NAMESPACE,
        RUST_LOG: "info",
      },
    });
    return "started";
  }

  async status() {
    return this.getState();
  }
}

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(getContainer(env.PG2ICEBERG, INSTANCE).ensureRunning());
  },

  // workers.dev is public: report the container's state, and start it,
  // but take nothing from the request.
  async fetch(_request, env) {
    const container = getContainer(env.PG2ICEBERG, INSTANCE);
    const action = await container.ensureRunning();
    return Response.json({ action, state: await container.status() });
  },
} satisfies ExportedHandler<Env>;
