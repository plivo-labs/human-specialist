import { type InputHTMLAttributes, useState } from "react";
import { ApiError, BASE, type Page, api, asAgent, every, hold, reason, reloadToSignIn, mint, store } from "./api";
import { operatorKey } from "./format";
import { asksForKey, showsKey, signInTrouble } from "./held";
import { ROLES, allAgents } from "./setup/agents";
import { type Api, type Registered, registrar, start } from "./setup/start";
import { go } from "./shell";
import type { Agent, Disposition, Queue, Session } from "./types";
import { Button, Card, Copy, Field, Form, Input, Modal, Notice, Submit, Tab, TabList, TabPanel, Tabs, act, useLoad } from "./ui";

export type Minted = { key: string; email?: string | null };
const HS = (key?: string): Api => ({
  call: <T,>(method: string, path: string, body?: unknown) => api<T>(method, path, body, key),
  agents: () => every<Agent>((c) => api<Page<Agent>>("GET", `/api/v1/agents?limit=200${c}`, undefined, key)),
  queues: () => every<Queue>((c) => api<Page<Queue>>("GET", `/api/v1/queues?limit=200${c}`, undefined, key)),
  dispositions: () => every<Disposition>((c) => api<Page<Disposition>>("GET", `/api/v1/dispositions?limit=200${c}`, undefined, key)),
});

const trouble = (e: unknown) => {
  const t = e instanceof ApiError && signInTrouble(e.code, e.details?.retry_after);
  return Promise.reject(t ? new Error(t) : e);
};
const Entry = ({ label, hint, ...p }: InputHTMLAttributes<HTMLInputElement> & { label: string; hint?: string }) => <Field label={label} hint={hint}><Input required {...p} /></Field>;
const Password = ({ label, name, fresh }: { label: string; name: string; fresh?: boolean }) => <Entry label={label} name={name} type="password" autoComplete={fresh ? "new-password" : "current-password"} {...(fresh && { minLength: 8, maxLength: 128 })} />;

export function You() {
  const agents = useLoad(allAgents);
  const bosses = agents.data?.filter((a) => a.role === "admin" || a.role === "supervisor") ?? [];
  const [asked, setAsked] = useState<string>();
  const sit = async (label: string, run: () => Promise<{ session: Session | null }>, email?: string) => {
    const r = await act(label, run, (r) => (r.session ? "Signed in" : "This email already has an account"));
    if (r?.session) (hold({ key: null, session: r.session }), go("/desk"));
    else if (r) setAsked(email);
  };
  return (
    <Card>
      <div className="grid gap-4">
        <h1 className="title">Who are you?</h1>
        {agents.error && <Notice tone="danger">{agents.error}</Notice>}
        {bosses.map((a) => (
          <Button key={a.id} size="md" className="justify-start" onClick={() => sit(`Signing in as ${a.name}`, () => mint(a.id).then((session) => ({ session })))}>
            {a.name}, {ROLES[a.role ?? "agent"]}
          </Button>
        ))}
        <Form onSubmit={(f) => {
          const you = { name: String(f.get("name")).trim(), email: String(f.get("email")).trim(), password: String(f.get("password")) };
          return sit("Adding you", () => start(HS(), you, asked === you.email ? "reset" : "add"), you.email);
        }}>
          <Entry label={bosses.length ? "Or add yourself" : "Your name"} name="name" autoComplete="name" />
          <Entry label="Your email" name="email" type="email" autoComplete="username" />
          <Password label="Your password" name="password" fresh />
          <Submit className="w-full">{asked ? `Set a new password for ${asked}` : "Continue"}</Submit>
        </Form>
        <Button variant="ghost" onClick={() => go("/agents")}>Not now</Button>
      </div>
    </Card>
  );
}

const SCOPES = ["admin", "sessions", "calls", "reports", "events", "recordings", "messages"];

export function NewKey({ shown, onDone }: { shown: Minted; onDone: () => void }) {
  return (
    <aside aria-labelledby="new-key" className="fixed bottom-4 right-4 z-40 grid w-[calc(100%-2rem)] max-w-sm gap-3 rounded-sm border bg-background p-4 shadow-sh1">
      <h2 id="new-key" className="text-s-600">Organization API key</h2>
      <p className="text-s-400 text-secondary">Shown once. Copy it now.</p>
      <span className="flex items-center gap-2">
        <Input readOnly value={shown.key} aria-label="Organization API key" className="font-mono" onFocus={(e) => e.target.select()} />
        <Copy value={shown.key} />
      </span>
      {shown.email && <p className="text-s-400">You sign in as <span className="font-medium">{shown.email}</span>.</p>}
      <span className="flex justify-end gap-2">
        <Button onClick={() => go("/numbers")}>Add a phone number</Button>
        <Button variant="primary" onClick={onDone}>Done</Button>
      </span>
    </aside>
  );
}

export function SignIn({ onMinted }: { onMinted: (m: Minted) => void }) {
  const [taken, setTaken] = useState<{ tenant: string; operator: string }>();
  const [askKey, setAskKey] = useState(false);
  const [stopped, setStopped] = useState<string>();
  const [reg] = useState(() => registrar((plivo, operator) => api<Registered>("POST", `/api/v1/tenants`, plivo, operator).catch((e) => refused(e, operator)), HS, () => hold({ key: null, session: null })));
  const refused = (e: unknown, operator: string) => {
    if (e instanceof ApiError && asksForKey(e.status, operator)) return setAskKey(true), Promise.reject(new Error("This server needs its operator key."));
    if (e instanceof ApiError && e.code === "tenant_exists") setTaken({ tenant: String(e.details?.tenant_id), operator });
    return Promise.reject(e);
  };
  const land = (tenant: string, key: string, you?: { me: Agent; session: Session | null }) => {
    store.set("tenant", tenant);
    hold({ key: you ? null : key, session: you?.session ?? null, signedOut: null });
    onMinted({ key, email: you?.me.email });
    go(you ? "/desk" : "/you");
  };
  const status = useLoad(() => fetch(`${BASE}/status`).then((r) => r.json() as Promise<{ setup?: boolean }>));
  const why = store.get<string>("signedOut");
  const org = new URLSearchParams(location.search).get("org");
  return (
    <Card>
      {why && <div className="mb-5"><Notice>{why}</Notice></div>}
      <Tabs defaultValue="password">
        <TabList label="Sign in or register" fill>
          <Tab value="password">Sign in</Tab>
          <Tab value="key">API key</Tab>
          <Tab value="register">Register</Tab>
        </TabList>
        <TabPanel value="password" className="pt-6">
          <Form
            onSubmit={async (f) => {
              const body = { email: String(f.get("email")).trim(), password: f.get("password"), ...(org && { tenant_id: org }) };
              const s = await act("Signing in", () => api<Session>("POST", `/api/v1/sessions`, body, "").catch(trouble), "Signed in");
              if (!s) return;
              if (org) store.set("tenant", org);
              hold({ key: null, session: s, signedOut: null });
              location.assign("/desk");
            }}
          >
            <Entry label="Email" name="email" type="email" autoComplete="username" autoFocus />
            <Password label="Password" name="password" />
            <Submit className="w-full">Sign in</Submit>
          </Form>
        </TabPanel>
        <TabPanel value="key" className="pt-6">
          <Form
            onSubmit={async (f) => {
              const key = String(f.get("key")).trim();
              const ok = await act("Signing in", () => api("GET", `/api/v1/agents?limit=1`, undefined, key), "Signed in");
              if (ok) (hold({ key, session: null, signedOut: null }), location.assign("/you"));
            }}
          >
            <Entry label="Organization API key" name="key" type="password" autoComplete="off" autoFocus />
            <Submit className="w-full">Sign in</Submit>
          </Form>
        </TabPanel>
        <TabPanel value="register" className="pt-6">
          <Form
            onSubmit={async (f) => {
              const run = () => reg.submit(f, f.has("operator") ? operatorKey(f.get("operator")) : "");
              const r = await act("Creating the organization", () => run().catch((e) => (setStopped(reg.pending() ? reason(e) : undefined), Promise.reject(e))), "Organization created");
              if (r) land(r.tenant_id, r.api_key, r);
            }}
          >
            <fieldset disabled={!!stopped} className="grid gap-5">
              <Entry label="Plivo Auth ID" name="auth_id" autoComplete="off" spellCheck={false} />
              <Entry label="Plivo Auth Token" name="auth_token" type="password" autoComplete="off" />
              <Entry label="Email" name="email" type="email" autoComplete="username" />
              <Password label="Password" name="password" fresh />
              {showsKey(status.data?.setup, askKey) && <Entry label="Operator key" hint="The server's HS_OPERATOR_KEY." name="operator" type="password" autoComplete="off" autoFocus={askKey} />}
            </fieldset>
            {stopped && <Notice tone="danger">The organization is created, but its setup stopped. {stopped}</Notice>}
            <Submit className="w-full">{stopped ? "Try again" : "Create organization"}</Submit>
            {taken && (
              <Notice>
                This Plivo account is already registered. If its setup stopped partway, make a new organization key, then add yourself.
                <Button className="mt-2" onClick={async () => {
                  const r = await act("Making a key", () => api<{ api_key: string }>("POST", `/api/v1/tenants/${taken.tenant}/api-keys`, { scopes: SCOPES }, taken.operator), "Key made");
                  if (r) land(taken.tenant, r.api_key);
                }}>Make a new organization key</Button>
              </Notice>
            )}
          </Form>
        </TabPanel>
      </Tabs>
    </Card>
  );
}

export function ChangePassword({ onClose }: { onClose: () => void }) {
  return (
    <Modal open onClose={onClose} title="Change password" sub="This signs you out everywhere.">
      <Form
        onSubmit={async (f) => {
          const body = { current_password: f.get("current"), password: f.get("password") };
          if (await act("Changing your password", () => asAgent("PUT", `/api/v1/me/password`, body).catch(trouble), "Password changed"))
            reloadToSignIn("Your password changed, and every session of yours ended. Sign in with the new password.");
        }}
      >
        <Password label="Current password" name="current" />
        <Password label="New password" name="password" fresh />
        <Submit className="justify-self-end">Change password</Submit>
      </Form>
    </Modal>
  );
}
