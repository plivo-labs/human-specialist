import type { Agent, Disposition, Queue, Session } from "../types";

export type Api = { call: <T>(method: string, path: string, body?: unknown) => Promise<T>; agents: () => Promise<Agent[]>; queues: () => Promise<Queue[]>; dispositions: () => Promise<Disposition[]> };
export type You = { name: string; email: string; password: string };

const OUTCOMES = ["Resolved", "Follow-up needed", "Not resolved"];

export const nameOf = (email: string) => email.split("@")[0].split(/[._-]+/).filter(Boolean).map((w) => w[0].toUpperCase() + w.slice(1)).join(" ") || email;

// "add" leaves an existing administrator with your email alone; "reset" sets its password; "register" is a new organization's first.
export async function start(api: Api, you: You, how: "add" | "reset" | "register" = "add") {
  let at = "";
  const step = <T>(label: string, run: () => Promise<T>) => ((at = label), run());
  try {
    const mine = (a: Agent) => a.role === "admin" && a.email?.toLowerCase() === you.email.toLowerCase();
    const found = (await step("Finding you", api.agents)).find(mine);
    if (found && how === "add") return { me: found, session: null };
    const me = found ?? (await step("Adding you", () => api.call<Agent>("POST", `/api/v1/agents`, { name: you.name, role: "admin", email: you.email })));
    await step("Setting your password", () => api.call("POST", `/api/v1/agents/${me.id}/password`, { password: you.password }));
    if (how === "register") {
      const q = (await step("Finding the General queue", api.queues)).find((x) => x.name === "General");
      if (!q) await step("Adding the General queue", () => api.call("POST", `/api/v1/queues`, { name: "General", agents: [me.id] }));
      else if (!q.agents?.includes(me.id)) await step("Adding you to General", () => api.call("PATCH", `/api/v1/queues/${q.id}`, { agents: [...(q.agents ?? []), me.id] }));
      const have = await step("Finding the dispositions", api.dispositions);
      if (!have.some((d) => !d.system && !d.disabled && !d.parent_disposition_id && !OUTCOMES.includes(d.name)))
        for (const name of OUTCOMES.filter((n) => !have.some((d) => d.name === n))) await step(`Adding the disposition ${name}`, () => api.call("POST", `/api/v1/dispositions`, { name }));
    }
    return { me, session: await step("Opening your desk", () => api.call<Session>("POST", `/api/v1/agents/${me.id}/sessions`)) };
  } catch (e) {
    if (e instanceof Error) e.message = `${at}: ${e.message}`;
    throw e;
  }
}

export type Registered = { tenant_id: string; api_key: string };

const MAYBE = "The server did not answer, so the organization may have been created. If pressing Create organization again says this Plivo account is already registered, finish it with Make a new organization key.";
const unanswered = (e: { status?: number }) => Promise.reject([0, 502, 504].includes(e.status ?? -1) ? new Error(MAYBE) : e);

// The organization a registration made lives only here, in memory, until its first administrator signs in.
// Once it exists the form is frozen: a retry reuses what made it, so it adds no second organization or administrator.
export function registrar(create: (plivo: { plivo_auth_id: string; plivo_auth_token: string }, operator: string) => Promise<Registered>, as: (key: string) => Api, forget: () => void) {
  let made: { org: Registered; you: Omit<You, "name"> } | undefined;
  let running: ReturnType<typeof run> | undefined;
  const run = async (f: FormData, operator: string) => {
    forget();
    const you = { email: String(f.get("email")).trim(), password: String(f.get("password")) };
    made ??= { org: await create({ plivo_auth_id: String(f.get("auth_id")).trim(), plivo_auth_token: String(f.get("auth_token")).trim() }, operator).catch(unanswered), you };
    const done = { ...made.org, ...(await start(as(made.org.api_key), { ...made.you, name: nameOf(made.you.email) }, "register")) };
    made = undefined;
    return done;
  };
  return { submit: (f: FormData, operator: string) => (running ??= run(f, operator).finally(() => (running = undefined))), pending: () => !!made };
}
