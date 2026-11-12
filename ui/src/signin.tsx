import { useState } from "react";
import { ApiError, type Page, api, asAgent, every, hold, reloadToSignIn, mint, store } from "./api";
import { operatorKey } from "./format";
import { signInTrouble } from "./held";
import { ROLES, SignInLink, allAgents, allQueues } from "./setup/agents";
import { start } from "./setup/start";
import { go } from "./shell";
import type { Disposition, Session } from "./types";
import { Button, Card, Copy, Field, Form, Input, Modal, Notice, Submit, Tab, TabList, TabPanel, Tabs, act, useLoad } from "./ui";

type Registered = { tenant_id: string; api_key: string };

const trouble = (e: unknown) => {
  const t = e instanceof ApiError && signInTrouble(e.code, e.details?.retry_after);
  return Promise.reject(t ? new Error(t) : e);
};
const enter = (route: string) => ((location.hash = route), location.reload());
const Password = ({ label, name, hint, fresh }: { label: string; name: string; hint?: string; fresh?: boolean }) => (
  <Field label={label} hint={hint}>
    <Input name={name} type="password" required autoComplete={fresh ? "new-password" : "current-password"} {...(fresh && { minLength: 8, maxLength: 128 })} />
  </Field>
);

export function You() {
  const first = store.get<boolean>("start");
  const agents = useLoad(allAgents);
  const bosses = agents.data?.filter((a) => a.role === "admin" || a.role === "supervisor") ?? [];
  const sit = async (label: string, run: () => Promise<Session>) => {
    const s = await act(label, run, "Signed in");
    if (!s) return;
    store.set("start", null);
    hold({ key: null, session: s });
    go("/desk");
  };
  return (
    <Card>
      <div className="grid gap-4">
        <h1 className="title">{first ? "Get started" : "Who are you?"}</h1>
        {first && (
          <>
            <p className="text-s-400 text-secondary">This is the organization's API key. It is shown only this once, so store it in your password manager now.</p>
            <div className="flex items-center gap-2 rounded-sm bg-muted p-2">
              <code className="min-w-0 flex-1 break-all px-1 font-mono text-s-400">{store.get<string>("key")}</code>
              <Copy value={store.get<string>("key")!} />
            </div>
            <Field label="Your agents sign in here" hint="Hand them this link with the email and password you set for each under Settings, Users.">
              <SignInLink />
            </Field>
          </>
        )}
        <p className="text-s-400 text-secondary">
          {first
            ? "Get started adds you as an administrator who signs in with this email and password, and a queue named General with you in it. It routes your number to General, lets agents call out with it as the caller ID, and adds the wrap-up outcomes Resolved, Follow-up needed and Not resolved when you have none. Change any of it later under Settings."
            : "Your status, the dialer and the calls that ring here belong to the user you choose."}
        </p>
        {agents.error && <Notice tone="danger">{agents.error}</Notice>}
        {!first && bosses.map((a) => (
          <Button key={a.id} size="md" className="justify-start" onClick={() => sit(`Signing in as ${a.name}`, () => mint(a.id))}>
            {a.name}, {ROLES[a.role ?? "agent"]}
          </Button>
        ))}
        <Form onSubmit={(f) => sit(first ? "Getting started" : "Adding you", () => start({ call: api, agents: allAgents, queues: allQueues, dispositions: () => every<Disposition>((c) => api<Page<Disposition>>("GET", `/api/v1/dispositions?limit=200${c}`)) }, String(f.get("name")).trim(), first ? String(f.get("number")).trim() : undefined, { email: String(f.get("email")).trim(), password: String(f.get("password")) }))}>
          <Field label={bosses.length && !first ? "Or add yourself" : "Your name"} hint={first ? undefined : "This adds you as an administrator, with a phone login on your Plivo account."}>
            <Input name="name" required autoComplete="name" />
          </Field>
          <Field label="Your email">
            <Input name="email" type="email" required autoComplete="username" />
          </Field>
          <Password label="Your password" name="password" hint="8 to 128 characters. You sign in with it from now on, with no key." fresh />
          {first && (
            <>
              <Field label="Your phone number" hint="A number on your Plivo account. If a step fails, fix it and press Get started again: the steps already done stay done.">
                <Input name="number" required inputMode="tel" placeholder="+14155550100" pattern="\+\d{6,15}" title="A plus sign, then the country code and number" />
              </Field>
            </>
          )}
          <Submit className="w-full">{first ? "Get started" : "Continue"}</Submit>
        </Form>
        <Button variant="ghost" onClick={() => (store.set("start", null), go("/agents"))}>{first ? "Skip" : "Not now"}</Button>
      </div>
    </Card>
  );
}

const SCOPES = ["admin", "sessions", "calls", "reports", "events", "recordings", "messages"];

const welcome = (r: Registered, name?: string) => {
  store.set("tenant", r.tenant_id);
  store.set("tenantName", name);
  hold({ key: r.api_key, session: null, signedOut: null });
  store.set("start", true);
  enter("/you");
};

export function SignIn() {
  const [taken, setTaken] = useState<{ tenant: string; operator: string }>();
  const why = store.get<string>("signedOut");
  const org = new URLSearchParams(location.hash.split("?")[1]).get("org");
  return (
    <Card>
      {why && (
        <div className="mb-5">
          <Notice>{why}</Notice>
        </div>
      )}
      <Tabs defaultValue="password">
        <TabList label="Sign in or register">
          <Tab value="password">Sign in</Tab>
          <Tab value="key">Organization key</Tab>
          <Tab value="register">Register an organization</Tab>
        </TabList>
        <TabPanel value="password" className="pt-6">
          <Form
            onSubmit={async (f) => {
              const body = { email: String(f.get("email")).trim(), password: f.get("password"), ...(org && { tenant_id: org }) };
              const s = await act("Signing in", () => api<Session>("POST", `/api/v1/sessions`, body, "").catch(trouble), "Signed in");
              if (!s) return;
              if (org) store.set("tenant", org);
              hold({ key: null, session: s, signedOut: null });
              enter("/desk");
            }}
          >
            <Field label="Email">
              <Input name="email" type="email" required autoComplete="username" autoFocus />
            </Field>
            <Password label="Password" name="password" />
            <Submit className="w-full">Sign in</Submit>
          </Form>
        </TabPanel>
        <TabPanel value="key" className="pt-6">
          <Form
            onSubmit={async (f) => {
              const key = String(f.get("key")).trim();
              const ok = await act("Signing in", () => api("GET", `/api/v1/agents?limit=1`, undefined, key), "Signed in");
              if (ok) {
                hold({ key, session: null, signedOut: null });
                enter("/you");
              }
            }}
          >
            <Field label="Organization API key" hint="For setup and integrations: the key shown when the organization was registered. People sign in with their email instead.">
              <Input name="key" type="password" required autoComplete="off" autoFocus />
            </Field>
            <Submit className="w-full">Sign in</Submit>
          </Form>
        </TabPanel>
        <TabPanel value="register" className="pt-6">
          <Form
            onSubmit={async (f) => {
              const operator = operatorKey(f.get("operator"));
              const body = { name: f.get("name"), plivo_auth_id: String(f.get("auth_id")).trim(), plivo_auth_token: String(f.get("auth_token")).trim() };
              const exists = (e: unknown) => (e instanceof ApiError && e.code === "tenant_exists" && setTaken({ tenant: String(e.details?.tenant_id), operator }), Promise.reject(e));
              const r = await act("Registering the organization", () => api<Registered>("POST", `/api/v1/tenants`, body, operator).catch(exists), "Organization registered");
              if (r) welcome(r, String(body.name));
            }}
          >
            <p className="text-s-400 text-secondary">First run only. This creates the organization, points a Plivo application at this server, and shows the organization's API key.</p>
            <Field label="Operator key">
              <Input name="operator" type="password" required autoComplete="off" placeholder="HS_OPERATOR_KEY from docker/.env" />
            </Field>
            <Field label="Organization name">
              <Input name="name" required placeholder="Your company name" />
            </Field>
            <Field label="Plivo Auth ID" hint="From your Plivo account. A sub-account's Auth ID and Auth Token also work, and keep this organization's numbers and usage apart.">
              <Input name="auth_id" required autoComplete="off" spellCheck={false} />
            </Field>
            <Field label="Plivo Auth Token">
              <Input name="auth_token" type="password" required autoComplete="off" />
            </Field>
            <Submit className="w-full">Register organization</Submit>
            {taken && (
              <Notice>
                This Plivo account is already registered here, and its key was shown only once.
                <Button className="mt-2" onClick={async () => {
                  const r = await act("Making a key", () => api<{ api_key: string }>("POST", `/api/v1/tenants/${taken.tenant}/api-keys`, { scopes: SCOPES }, taken.operator), "Key made");
                  if (r) welcome({ tenant_id: taken.tenant, api_key: r.api_key });
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
    <Modal open onClose={onClose} title="Change password" sub="Every session of yours ends, this one too, so you sign in again with the new password.">
      <Form
        onSubmit={async (f) => {
          const body = { current_password: f.get("current"), password: f.get("password") };
          if (await act("Changing your password", () => asAgent("PUT", `/api/v1/me/password`, body).catch(trouble), "Password changed"))
            reloadToSignIn("Your password changed, and every session of yours ended. Sign in with the new password.");
        }}
      >
        <Password label="Current password" name="current" />
        <Password label="New password" name="password" hint="8 to 128 characters." fresh />
        <Submit className="justify-self-end">Change password</Submit>
      </Form>
    </Modal>
  );
}
