import type { ReactNode } from "react";
import { type Page as Paged, api, every, renewals } from "../api";
import { phone } from "../format";
import type { EntryPoint, PresenceStatus } from "../types";
import { Field, Form, Input, Listed, Page, Select, Submit, Tab, TabList, TabPanel, Tabs, Toggle, act, cls, useLoad } from "../ui";

type Ref = { status_id: string | null } | null;
type Settings = {
  ring_timeout_sec: number;
  acw_blocks_routing: boolean;
  agent_direct_dial: boolean;
  capacity: Record<string, number>;
  text: { offer_timeout_sec: number | Record<string, number>; session_timeout_sec: Record<string, number> };
  idle: { enabled: boolean; after_min: number; status_id: string | null };
  sign_in: Ref;
  sign_out: Ref;
  wrap_up: { mode: "manual" | "auto"; seconds: number };
  recording: { inbound: boolean; outbound: boolean; allow_pause: boolean };
  default_from: Record<string, string>;
};

const TEXT = [["chat", "Chat"], ["email", "Email"], ["sms", "SMS"], ["whatsapp", "WhatsApp"]];
const SESSION = [["sms", "SMS"], ["chat", "Chat"], ["whatsapp", "WhatsApp"], ["email", "Email"]];
const CAPACITY = [["chat", "Chat"], ["sms", "SMS"], ["whatsapp", "WhatsApp"], ["email", "Email"], ["callback", "Callback"], ["task", "Task"]];
const num = (f: FormData, name: string) => Number(f.get(name));
const on = (f: FormData, name: string) => f.get(name) === "on";

function SettingsForm({ body, children, className = "py-6" }: { body: (f: FormData) => object; children: (s: Settings) => ReactNode; className?: string }) {
  const load = useLoad(() => api<Settings>("GET", `/api/v1/settings`));
  return (
    <Listed load={{ ...load, data: load.data && [load.data] }} empty={null}>
      {([s]) => (
        <Form
          key={JSON.stringify(s)}
          changes
          className={className}
          onSubmit={async (f) => {
            if (!(await act("Saving the settings", () => api("PATCH", `/api/v1/settings`, body(f)), "Settings saved"))) return;
            load.reload();
            renewals.dispatchEvent(new Event("now"));
          }}
        >
          {children(s)}
          <Submit className="mt-3 justify-self-start">Save</Submit>
        </Form>
      )}
    </Listed>
  );
}

const Part = ({ title, sub, end, children }: { title: string; sub?: string; end?: boolean; children: ReactNode }) => (
  <section className={cls("grid gap-4", !end && "border-b pb-6")}>
    <div>
      <h2 className="text-p-600">{title}</h2>
      {sub && <p className="text-p-400 text-secondary">{sub}</p>}
    </div>
    {children}
  </section>
);

const Unit = ({ label, name, value, unit, min, max, disabled }: { label: string; name: string; value: number; unit?: string; min?: number; max?: number; disabled?: boolean }) => (
  <Field label={label}>
    <span className="relative block">
      <Input name={name} type="number" required min={min} max={max} defaultValue={value} disabled={disabled} className="pr-20 font-mono disabled:bg-muted" />
      {unit && <span className="pointer-events-none absolute right-9 top-2.5 font-mono text-xs-400 text-secondary">{unit}</span>}
    </span>
  </Field>
);

const SPANS: [string, number][] = [["Seconds", 1], ["Minutes", 60], ["Hours", 3600], ["Days", 86400]];
const span = (f: FormData, name: string) => num(f, name) * num(f, `${name}_unit`);

function Span({ label, name, value, max }: { label: string; name: string; value: number; max?: number }) {
  const unit = SPANS.findLast(([, k]) => value % k === 0)![1];
  return (
    <Field label={label}>
      <span className="flex h-9 rounded-sm border bg-background">
        <input name={name} type="number" required min={1} max={max && max / unit} defaultValue={value / unit} className="w-0 flex-1 bg-transparent px-3 font-mono text-p-400" />
        <Select name={`${name}_unit`} defaultValue={unit} aria-label={`${label} unit`} className="w-32 [&_select]:h-[34px] [&_select]:border-0">
          {SPANS.map(([l, k]) => (
            <option key={k} value={k}>
              {l}
            </option>
          ))}
        </Select>
      </span>
    </Field>
  );
}

const pair = "grid max-w-[576px] grid-cols-2 gap-x-4 gap-y-4";
const offer = (s: Settings, ch: string) => (typeof s.text.offer_timeout_sec === "number" ? s.text.offer_timeout_sec : (s.text.offer_timeout_sec[ch] ?? 30));

export const Workload = () => (
  <Page title="Workload" sub="Manage your agents' workload">
    <SettingsForm
      body={(f) => ({
        ring_timeout_sec: num(f, "ring"),
        text: { offer_timeout_sec: Object.fromEntries(TEXT.map(([ch]) => [ch, num(f, `offer_${ch}`)])) },
        acw_blocks_routing: !on(f, "release"),
        capacity: Object.fromEntries(CAPACITY.map(([ch]) => [ch, num(f, `cap_${ch}`)])),
      })}
    >
      {(s) => (
        <>
          <Part title="Offer time" sub="How long an offer rings an agent before it moves on, on each channel">
            <div className={pair}>
              <Unit label="Call" name="ring" value={s.ring_timeout_sec} unit="seconds" min={5} max={120} />
              {TEXT.map(([ch, label]) => (
                <Unit key={ch} label={label} name={`offer_${ch}`} value={offer(s, ch)} unit="seconds" min={5} max={3600} />
              ))}
            </div>
          </Part>
          <Part title="Agent capacity" sub="How many conversations an agent holds at once on each channel. 0 holds one, as 1 does." end>
            <Toggle name="release" label="Free an agent's slot during wrap-up" defaultChecked={!s.acw_blocks_routing} />
            <div className={pair}>
              <Unit label="Call" name="cap_call" value={1} disabled />
              {CAPACITY.map(([ch, label]) => (
                <Unit key={ch} label={label} name={`cap_${ch}`} value={s.capacity[ch] ?? 1} min={0} max={100} />
              ))}
            </div>
          </Part>
        </>
      )}
    </SettingsForm>
  </Page>
);

const ref = (v: FormDataEntryValue | null): Ref => (v === "" ? null : { status_id: v === "none" ? null : String(v) });
const refValue = (r: Ref) => (r == null ? "" : (r.status_id ?? "none"));

function StatusPick({ label, name, value, statuses, unset }: { label: string; name: string; value: string; statuses: PresenceStatus[]; unset?: string }) {
  return (
    <Field label={label}>
      <Select name={name} defaultValue={value}>
        {unset && <option value="">{unset}</option>}
        <option value="none">No change</option>
        {statuses.map((x) => (
          <option key={x.id} value={x.id}>
            {x.name}
          </option>
        ))}
      </Select>
    </Field>
  );
}

export const StatusRules = ({ statuses }: { statuses: PresenceStatus[] }) => (
  <SettingsForm
    className="pb-6 pt-4"
    body={(f) => ({
      sign_in: ref(f.get("sign_in")),
      sign_out: ref(f.get("sign_out")),
      idle: { enabled: on(f, "idle"), after_min: num(f, "after"), status_id: f.get("idle_status") === "none" ? null : f.get("idle_status") },
    })}
  >
    {(s) => (
      <>
        <Part title="Configure agent default status" sub="Set the default status for agents during the following events">
          <div className="grid max-w-80 gap-5">
            <StatusPick label="Agent Status on Sign-in" name="sign_in" value={refValue(s.sign_in)} statuses={statuses} unset="Default: an available status" />
            <StatusPick label="Agent Status on Sign-out" name="sign_out" value={refValue(s.sign_out)} statuses={statuses} unset="Default: Offline" />
          </div>
        </Part>
        <Part title="Agent Status when idle" sub="When an agent is idle for the specified duration, their status is changed. The agent will not be assigned new conversation requests." end>
          <Toggle name="idle" label="Enable Agent Idle time" defaultChecked={s.idle.enabled} />
          <div className="grid max-w-80 gap-5">
            <Unit label="Agent Inactivity Duration" name="after" value={s.idle.after_min} unit="minutes" min={5} max={500} />
            <StatusPick label="Agent Status changes to" name="idle_status" value={s.idle.status_id ?? "none"} statuses={statuses} />
          </div>
        </Part>
      </>
    )}
  </SettingsForm>
);

export const ConversationRules = () => (
  <SettingsForm
    className="max-w-[664px] px-1 pb-6 pt-5"
    body={(f) => ({
      text: { session_timeout_sec: Object.fromEntries(SESSION.map(([ch]) => [ch, span(f, `session_${ch}`)])) },
      wrap_up: { seconds: span(f, "acw"), mode: on(f, "auto") ? "auto" : "manual" },
    })}
  >
    {(s) => (
      <>
        <Part title="Conversation Timeout" sub="Set the timeout for each conversation channel.">
          <div className="grid grid-cols-2 gap-4">
            {SESSION.map(([ch, label]) => (
              <Span key={ch} label={label} name={`session_${ch}`} value={s.text.session_timeout_sec[ch]} />
            ))}
          </div>
        </Part>
        <Part title="After Conversation Work (ACW)" sub="How long an agent has to wrap up. With the switch on, a call's wrap-up is submitted when the timer ends. Any other wrap-up waits for the agent, and closes on its own 5 minutes after the timer ends.">
          <div className="grid max-w-80 gap-5">
            <Span label="After Conversation Work Timeout" name="acw" value={s.wrap_up.seconds} max={3600} />
          </div>
          <Toggle name="auto" label="Submit a call's wrap-up when the timer ends" defaultChecked={s.wrap_up.mode === "auto"} />
        </Part>
      </>
    )}
  </SettingsForm>
);

const CHANNELS = [["call", "Voice Call"], ["sms", "SMS"], ["whatsapp", "WhatsApp"]];

export function Outbound() {
  const numbers = useLoad(() => every<EntryPoint>((c) => api<Paged<EntryPoint>>("GET", `/api/v1/numbers?limit=200${c}`)));
  return (
    <Page title="Outbound Configurations">
      <SettingsForm
        body={(f) => ({
          default_from: Object.fromEntries(CHANNELS.map(([ch]) => [ch, f.get(`from_${ch}`) || null])),
          agent_direct_dial: on(f, "dial"),
          recording: { inbound: on(f, "rec_in"), outbound: on(f, "rec_out"), allow_pause: on(f, "rec_pause") },
        })}
      >
        {(s) => (
          <Tabs defaultValue="call" className="-mt-2">
            <TabList label="Outbound Configurations">
              {CHANNELS.map(([ch, label]) => (
                <Tab key={ch} value={ch}>
                  {label}
                </Tab>
              ))}
            </TabList>
            {CHANNELS.map(([ch, label]) => (
              <TabPanel key={ch} value={ch} forceMount className="grid gap-6 pt-6 data-[state=inactive]:hidden">
                <div className={cls("grid gap-4", ch === "call" && "border-b pb-6")}>
                  <Field label={ch === "call" ? "Default Caller ID" : `Default ${label} sender`}>
                    <Select name={`from_${ch}`} defaultValue={s.default_from[ch] ?? ""} className="max-w-72">
                      <option value="">No default</option>
                      {[...new Set([...(numbers.data ?? []).map((n) => n.number), s.default_from[ch]].filter(Boolean))].map((n) => (
                        <option key={n} value={n}>
                          {phone(n)}
                        </option>
                      ))}
                    </Select>
                  </Field>
                  {ch === "call" && <Toggle name="dial" label="Let agents place outbound calls" defaultChecked={s.agent_direct_dial} />}
                </div>
                {ch === "call" && (
                  <Part title="Call Recording Preferences" sub="Manage your call recording preferences here" end>
                    <Toggle name="rec_in" label="Record all incoming calls" defaultChecked={s.recording.inbound} />
                    <Toggle name="rec_out" label="Record all outgoing calls" defaultChecked={s.recording.outbound} />
                    <Toggle name="rec_pause" label="Let agents pause and resume call recording" defaultChecked={s.recording.allow_pause} />
                  </Part>
                )}
              </TabPanel>
            ))}
          </Tabs>
        )}
      </SettingsForm>
    </Page>
  );
}
