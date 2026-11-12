import type { Agent, Disposition, EntryPoint, Queue, Session } from "../types";

export type Api = { call: <T>(method: string, path: string, body?: unknown) => Promise<T>; agents: () => Promise<Agent[]>; queues: () => Promise<Queue[]>; dispositions: () => Promise<Disposition[]> };

const OUTCOMES = ["Resolved", "Follow-up needed", "Not resolved"];

export async function start(api: Api, name: string, number?: string, login?: { email: string; password: string }) {
  let at = "";
  const step = <T>(label: string, run: () => Promise<T>) => ((at = label), run());
  try {
    const me = (await step("Finding you", api.agents)).find((a) => a.name === name && a.role === "admin") ?? (await step("Adding you", () => api.call<Agent>("POST", `/api/v1/agents`, { name, role: "admin", email: login?.email })));
    if (login) await step("Setting your password", () => api.call("POST", `/api/v1/agents/${me.id}/password`, { password: login.password }));
    if (number) {
      const q = (await step("Finding the General queue", api.queues)).find((x) => x.name === "General");
      const general = !q
        ? await step("Adding the General queue", () => api.call<Queue>("POST", `/api/v1/queues`, { name: "General", agents: [me.id] }))
        : q.agents?.includes(me.id)
          ? q
          : await step("Adding you to General", () => api.call<Queue>("PATCH", `/api/v1/queues/${q.id}`, { agents: [...(q.agents ?? []), me.id] }));
      const have = await step("Finding the dispositions", api.dispositions);
      if (!have.some((d) => !d.system && !d.disabled && !d.parent_disposition_id && !OUTCOMES.includes(d.name)))
        for (const name of OUTCOMES.filter((n) => !have.some((d) => d.name === n))) await step(`Adding the disposition ${name}`, () => api.call("POST", `/api/v1/dispositions`, { name }));
      const old = await step(`Reading ${number}`, () => api.call<EntryPoint>("GET", `/api/v1/numbers/${encodeURIComponent(number)}`).catch((e) => (e?.status === 404 ? undefined : Promise.reject(e))));
      const { business_hours_id = null, closed_action = null } = old ?? {};
      await step(`Routing ${number} to General`, () => api.call("PUT", `/api/v1/numbers/${encodeURIComponent(number)}`, { open_action: { type: "queue", queue_id: general.id }, business_hours_id, closed_action }));
      await step("Letting agents call out", () => api.call("PATCH", `/api/v1/settings`, { agent_direct_dial: true, default_from: { call: number } }));
    }
    return await step("Opening your desk", () => api.call<Session>("POST", `/api/v1/agents/${me.id}/sessions`));
  } catch (e) {
    if (e instanceof Error) e.message = `${at}: ${e.message}`;
    throw e;
  }
}
