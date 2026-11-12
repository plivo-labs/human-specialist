import { Plus } from "lucide-react";
import { useState } from "react";
import { ApiError, type Page as Paged, api, asAgent, every, held, reloadToSignIn, store } from "../api";
import { phone } from "../format";
import type { Agent, Me, Named, PresenceStatus, Queue } from "../types";
import { Badge, Button, Confirm, Copy, EditForm, Editor, Empty, Facts, Field, Form, Input, ListView, Page, Section, Select, Sheet, State, Submit, act, useLoad } from "../ui";

type Directory = { agents: Agent[]; teams: Named[]; queues: Queue[]; statuses: PresenceStatus[] };

export const allAgents = () => every<Agent>((c) => api<Paged<Agent>>("GET", `/api/v1/agents?limit=200${c}`));
export const allQueues = () => every<Queue>((c) => api<Paged<Queue>>("GET", `/api/v1/queues?limit=200${c}`));

const loadDirectory = async (): Promise<Directory> => {
  const [agents, teams, queues, statuses] = await Promise.all([
    allAgents(),
    every<Named>((c) => api<Paged<Named>>("GET", `/api/v1/teams?limit=200${c}`)),
    allQueues(),
    every<PresenceStatus>((c) => api<Paged<PresenceStatus>>("GET", `/api/v1/agent-statuses?limit=200${c}`)),
  ]);
  return { agents, teams, queues, statuses };
};

export const ROLES = { agent: "Agent", supervisor: "Supervisor", admin: "Administrator" };

export const names = (ids: string[], from: { id: string; name: string | null }[]) =>
  ids.map((id) => from.find((x) => x.id === id)?.name ?? "Unknown").join(", ");

export const twin = (what: string, rows: Named[], name: unknown, self?: string): [string, string] | undefined =>
  rows.some((r) => r.id !== self && r.name?.trim().toLowerCase() === String(name).trim().toLowerCase()) ? [`A ${what} named ${String(name).trim()} already exists. Lists will show the two the same way.`, self ? "Save anyway" : "Create anyway"] : undefined;

export function SignInLink() {
  const org = store.get<string>("tenant");
  const url = `${location.origin}${location.pathname}#/signin${org ? `?org=${org}` : ""}`;
  return (
    <span className="flex items-center gap-2">
      <Input readOnly value={url} aria-label="Sign-in link" onFocus={(e) => e.target.select()} />
      <Copy value={url} />
    </span>
  );
}

export function StatusText({ status, statuses }: { status: Agent["status"]; statuses: PresenceStatus[] }) {
  const s = statuses.find((x) => x.id === status?.id);
  return <State color={s?.availability ? "var(--available)" : (s?.color ?? "var(--tertiary)")}>{status?.name ?? "Offline"}</State>;
}

export function Agents() {
  const dir = useLoad(loadDirectory);
  const [adding, setAdding] = useState(false);
  const [open, setOpen] = useState<string | null>(null);
  const agent = dir.data?.agents.find((a) => a.id === open);
  return (
    <Page title="Users">
      <div className="mt-4 max-w-xl">
        <Field label="Sign-in link" hint={store.get("tenant") ? "Hand it to your users with their email and password." : "This browser does not know the organization's ID, so the link works only while this server has one organization."}>
          <SignInLink />
        </Field>
      </div>
      <ListView
        load={{ ...dir, data: dir.data?.agents }}
        search={(a) => a.name}
        refresh
        open={(a) => setOpen(a.id)}
        action={
          <Button variant="primary" onClick={() => setAdding(true)}>
            <Plus aria-hidden /> Add user
          </Button>
        }
        empty={<Empty title="No users yet. Add the first one, then open their desk to take calls." />}
        cols={[
          [
            "Name",
            (a) => (
              <span className="grid">
                {a.name}
                {!a.endpoint_username && <span className="text-xs-400 font-normal text-secondary">No phone login yet</span>}
              </span>
            ),
          ],
          ["Team", (a) => (a.teams.length > 1 ? <Badge>{a.teams.length} teams</Badge> : names(a.teams, dir.data!.teams)), "hidden md:table-cell"],
          ["Status", (a) => <StatusText status={a.status} statuses={dir.data!.statuses} />],
          ["Role", (a) => ROLES[a.role ?? "agent"], "hidden sm:table-cell"],
        ]}
      />
      {dir.data && adding && <AddAgent dir={dir.data} onClose={() => setAdding(false)} onAdded={dir.reload} />}
      {agent && <AgentSheet agent={agent} dir={dir.data!} onClose={() => setOpen(null)} reload={dir.reload} />}
    </Page>
  );
}

const agentBody = (f: FormData) => ({
  name: f.get("name"),
  role: f.get("role"),
  email: String(f.get("email")).trim() || null,
  teams: f.getAll("teams"),
  caller_id: String(f.get("caller_id")).trim() || null,
});

function AgentFields({ agent, teams }: { agent?: Agent; teams: Named[] }) {
  return (
    <>
      <Field label="Name" hint={agent ? undefined : "Adding a user creates their phone login on your Plivo account."}>
        <Input name="name" required autoFocus={!agent} defaultValue={agent?.name} />
      </Field>
      <Field label="Role" hint={`Supervisors and administrators also see the floor and can listen in.${agent ? " A new role signs the user out everywhere." : ""}`}>
        <Select name="role" defaultValue={agent?.role ?? "agent"}>
          {Object.entries(ROLES).map(([v, l]) => (
            <option key={v} value={v}>
              {l}
            </option>
          ))}
        </Select>
      </Field>
      <Field label="Email" hint="Optional. The user signs in with it and the password set in their sheet.">
        <Input name="email" type="email" autoComplete="off" defaultValue={agent?.email ?? ""} />
      </Field>
      <fieldset className="grid gap-2">
        <legend className="pb-1.5 text-s-500 text-secondary">Teams</legend>
        {teams.length ? (
          teams.map((t) => (
            <label key={t.id} className="flex items-center gap-2 text-p-400">
              <input type="checkbox" name="teams" value={t.id} defaultChecked={agent?.teams.includes(t.id)} className="size-4 accent-primary" /> {t.name}
            </label>
          ))
        ) : (
          <p className="text-s-400 text-secondary">No teams yet. Create them under Teams.</p>
        )}
      </fieldset>
      <Field label="Caller ID" hint="Optional. A number this organization routes, shown when the user calls out.">
        <Input name="caller_id" placeholder="+14155550100" inputMode="tel" defaultValue={agent?.caller_id ?? ""} />
      </Field>
    </>
  );
}

const taken = (agents: Agent[], call: () => Promise<unknown>) =>
  call().catch((e) => {
    const who = e instanceof ApiError && e.code === "agent_exists" && agents.find((a) => a.id === e.details?.id);
    return Promise.reject(who ? new Error(`Another user, ${who.name}, already has this email.`) : e);
  });

function AddAgent({ dir: { agents, teams }, onClose, onAdded }: { dir: Directory; onClose: () => void; onAdded: () => void }) {
  return (
    <Editor what="user" reload={onAdded} onClose={onClose} body={agentBody} save={(b) => taken(agents, () => api<Agent>("POST", `/api/v1/agents`, b))} warn={(b) => twin("user", agents, b.name)}>
      <AgentFields teams={teams} />
    </Editor>
  );
}

const Once = ({ text, value }: { text: string; value: string }) => (
  <div className="mt-3 grid gap-2 rounded-sm bg-muted p-3">
    <p className="text-s-400 text-secondary">{text}</p>
    <div className="flex items-center gap-2">
      <code className="min-w-0 flex-1 break-all font-mono text-s-400">{value}</code>
      <Copy value={value} />
    </div>
  </div>
);

function AgentSheet({ agent, dir, onClose, reload }: { agent: Agent; dir: Directory; onClose: () => void; reload: () => void }) {
  const [made, setMade] = useState<string | null>(null);
  const [sip, setSip] = useState<{ username: string; password: string } | null>(null);
  const queues = dir.queues.filter((q) => q.agents?.includes(agent.id));
  const me = useLoad(() => (held().session ? asAgent<Me>("GET", `/api/v1/me`) : Promise.resolve(null)));
  const self = me.data?.id === agent.id;
  const password = async (body?: { password: FormDataEntryValue | null }) => {
    const r = await act(body ? "Setting the password" : "Making a password", () => api<{ password: string | null }>("POST", `/api/v1/agents/${agent.id}/password`, body), `Password set. Every session of ${agent.name} ended.`);
    if (r && self) reloadToSignIn("Your password changed. Sign in with the new one.");
    return r;
  };
  return (
    <Sheet open onClose={onClose} title={agent.name} sub={ROLES[agent.role ?? "agent"]}>
      <Section title="Profile">
        <EditForm
          what="user"
          row={agent}
          reload={reload}
          onClose={onClose}
          body={agentBody}
          save={(b) => taken(dir.agents, () => api("PATCH", `/api/v1/agents/${agent.id}`, b))}
          remove={() => api("DELETE", `/api/v1/agents/${agent.id}`)}
          warn={(b) => twin("user", dir.agents, b.name, agent.id)}
        >
          <AgentFields agent={agent} teams={dir.teams} />
        </EditForm>
      </Section>
      <Section title="Sign-in">
        <p className="mb-3 text-s-400 text-secondary">
          {agent.email ? `${agent.name} signs in with ${agent.email} and a password. A new password signs them out everywhere${self ? ", this session too, so set one you know" : ""}.` : `Add an email above, then a password, so ${agent.name} can sign in.`}
        </p>
        <Form onSubmit={async (f, form) => void ((await password({ password: f.get("password") })) && form.reset())}>
          <Field label="New password" hint="8 to 128 characters.">
            <Input name="password" type="password" required minLength={8} maxLength={128} autoComplete="new-password" />
          </Field>
          <Submit className="justify-self-start" disabled={!agent.email}>Set password</Submit>
        </Form>
        <div className="mt-3 flex flex-wrap gap-2">
          <Button disabled={!agent.email || self} onClick={async () => setMade((await password())?.password ?? null)}>Generate password</Button>
          {held().key && <Confirm label="Sign out everywhere" sure="Press again to sign them out" run={() => act(`Signing ${agent.name} out`, () => api("DELETE", `/api/v1/agents/${agent.id}/sessions`), `${agent.name} is signed out everywhere`)} />}
        </div>
        {made && <Once text="The new password, shown once. Hand it to the user with the sign-in link." value={made} />}
      </Section>
      <Section title="Phone login">
        <Facts
          rows={[
            ["SIP username", agent.endpoint_username ? <code className="font-mono">{agent.endpoint_username}</code> : "Not set up"],
            ["Caller ID", agent.caller_id ? phone(agent.caller_id) : "The organization's default"],
          ]}
        />
        {sip ? (
          <Once text="The new password, shown once. Enter it in the user's SIP phone." value={sip.password} />
        ) : (
          <Button
            className="mt-3"
            onClick={async () => {
              const r = await act("Resetting the SIP password", () => api<{ username: string; password: string }>("POST", `/api/v1/agents/${agent.id}/sip-credentials`), "New SIP password made");
              if (r) setSip(r);
            }}
          >
            Reset SIP password
          </Button>
        )}
      </Section>
      <Section title="Routing">
        <Facts
          rows={[
            ["Status", <StatusText key="s" status={agent.status} statuses={dir.statuses} />],
            ["Queues", queues.map((q) => q.name).join(", ") || "In no queue. Add them under Queues."],
          ]}
        />
      </Section>
    </Sheet>
  );
}
