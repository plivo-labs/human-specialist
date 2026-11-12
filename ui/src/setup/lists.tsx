import { Plus, UserPen } from "lucide-react";
import { type ReactNode, useState } from "react";
import { type Page as Paged, api, every } from "../api";
import { cap, day, phone, plain, zone } from "../format";
import type { BusinessHours, Destination, HolidayGroup, Named, PresenceStatus, Skill, Slot, Task } from "../types";
import { Badge, Button, cls, Editor, Empty, Facts, Field, Input, ListView, Page, Rows, Select, Sheet, State, TabList, TabPanel, Tab, Tabs, type Tone, useLoad, usePaged } from "../ui";
import { allAgents, twin } from "./agents";
import { StatusRules } from "./settings";

function useEdit<T>() {
  const [edit, setEdit] = useState<T | "new" | null>(null);
  return { open: edit !== null, row: edit === "new" || edit === null ? undefined : edit, start: setEdit, close: () => setEdit(null) };
}

const New = ({ icon = <Plus aria-hidden />, label, onClick, size }: { icon?: ReactNode; label: string; onClick: () => void; size?: "md" }) => (
  <Button variant="primary" onClick={onClick} size={size}>
    {icon} {label}
  </Button>
);

export function Teams() {
  const load = useLoad(() => Promise.all([every<Named>((c) => api<Paged<Named>>("GET", `/api/v1/teams?limit=200${c}`)), allAgents()]));
  const [teams, agents] = load.data ?? [];
  const members = (id: string) => agents?.filter((a) => a.teams.includes(id)).length ?? 0;
  const e = useEdit<Named>();
  const row = e.row;
  return (
    <Page title="Teams" sub="Manage teams and assign members">
      <ListView
        load={{ ...load, data: teams }}
        search={(t) => t.name}
        open={e.start}
        action={<New label="Create team" onClick={() => e.start("new")} />}
        empty={<Empty title="No teams yet." />}
        cols={[
          ["Name", (t) => t.name],
          ["Members", (t) => members(t.id) > 0 && <Badge>{`${members(t.id)} member${members(t.id) === 1 ? "" : "s"}`}</Badge>, "w-[35%]"],
        ]}
      />
      {e.open && (
        <Editor
          what="team"
          row={row}
          reload={load.reload}
          onClose={e.close}
          body={(f) => ({ name: f.get("name") })}
          save={(b) => (row ? api("PATCH", `/api/v1/teams/${row.id}`, b) : api("POST", `/api/v1/teams`, b))}
          warn={(b) => twin("team", teams!, b.name, row?.id)}
          remove={row && (() => api("DELETE", `/api/v1/teams/${row.id}`))}
        >
          <Field label="Team name">
            <Input name="name" required autoFocus defaultValue={row?.name ?? ""} />
          </Field>
          <p className="text-s-400 text-secondary">Add members from each user's sheet under Users.</p>
        </Editor>
      )}
    </Page>
  );
}

export function Skills() {
  const load = useLoad(() => every<Skill>((c) => api<Paged<Skill>>("GET", `/api/v1/skills?limit=200${c}`)));
  const e = useEdit<Skill>();
  const row = e.row;
  return (
    <Page title="Skills" sub="Manage skills for your organization">
      <ListView
        load={load}
        search={(s) => s.name}
        open={e.start}
        action={<New label="Create skill" onClick={() => e.start("new")} />}
        empty={<Empty title="No skills yet." />}
        cols={[
          ["Skill name", (s) => s.name],
          ["Minimum level", (s) => s.min_level, "w-[23%]"],
          ["Maximum level", (s) => s.max_level, "w-[22%]"],
        ]}
      />
      {e.open && (
        <Editor
          what="skill"
          row={row}
          reload={load.reload}
          onClose={e.close}
          body={(f) => ({ name: f.get("name"), min_level: Number(f.get("min")), max_level: Number(f.get("max")) })}
          save={(b) => (row ? api("PATCH", `/api/v1/skills/${row.id}`, b) : api("POST", `/api/v1/skills`, b))}
          remove={row && (() => api("DELETE", `/api/v1/skills/${row.id}`))}
        >
          <Field label="Skill name">
            <Input name="name" required autoFocus defaultValue={row?.name ?? ""} />
          </Field>
          <div className="grid grid-cols-2 gap-4">
            <Field label="Minimum level">
              <Input name="min" type="number" required min={0} max={100} defaultValue={row?.min_level ?? 1} className="font-mono" />
            </Field>
            <Field label="Maximum level">
              <Input name="max" type="number" required min={0} max={100} defaultValue={row?.max_level ?? 100} className="font-mono" />
            </Field>
          </div>
        </Editor>
      )}
    </Page>
  );
}

const COLORS: Record<string, string> = { "#27AE7D": "Green", "#EB5757": "Red", "#EBA000": "Amber", "#9B9FAA": "Gray" };

export function Statuses() {
  const load = useLoad(() => every<PresenceStatus>((c) => api<Paged<PresenceStatus>>("GET", `/api/v1/agent-statuses?limit=200${c}`)).then((s) => s.sort((a, b) => a.sequence - b.sequence)));
  const e = useEdit<PresenceStatus>();
  const row = e.row;
  const color = row?.color?.toUpperCase() ?? "#27AE7D";
  return (
    <Page title="Availability Status" sub="Manage your organization's status">
      <Tabs defaultValue="status" className="mt-2 flex min-h-0 flex-1 flex-col">
        <TabList label="Availability Status">
          <Tab value="status">Status</Tab>
          <Tab value="config">Configurations</Tab>
        </TabList>
        <TabPanel value="status" className="flex min-h-0 flex-1 flex-col">
          <ListView
            load={load}
            search={(s) => s.name}
            iconSearch
            open={e.start}
            reorder={(ids) => api("POST", "/api/v1/agent-statuses/reorder", { ids }).then(load.reload)}
            action={<New icon={<UserPen aria-hidden />} label="Add status" size="md" onClick={() => e.start("new")} />}
            empty={<Empty title="No statuses yet." />}
            cols={[
              ["Status", (s) => s.name],
              ["Color", (s) => <span aria-label={s.color ?? "No color"} className="m-0.5 block size-3 rounded-full ring-1 ring-border ring-offset-2 ring-offset-background" style={{ background: s.color }} />],
              ["Availability", (s) => (s.availability ? <State tone="live">Available</State> : <State tone="off" dim>Unavailable</State>)],
              ["Default", (s) => s.default && <span className="label inline-block rounded-sm bg-muted px-2 py-[5px] text-secondary">Default</span>],
            ]}
          />
        </TabPanel>
        <TabPanel value="config" className="min-h-0 flex-1 overflow-y-auto">
          <StatusRules statuses={load.data ?? []} />
        </TabPanel>
      </Tabs>
      {e.open && (
        <Editor
          what="status"
          row={row}
          reload={load.reload}
          onClose={e.close}
          body={(f) => ({ name: f.get("name"), color: f.get("color"), availability: row?.default ? row.availability : f.get("availability") === "on" })}
          save={(b) => (row ? api("PATCH", `/api/v1/agent-statuses/${row.id}`, b) : api("POST", `/api/v1/agent-statuses`, b))}
          remove={row && !row.default ? () => api("DELETE", `/api/v1/agent-statuses/${row.id}`) : undefined}
        >
          <Field label="Name">
            <Input name="name" required autoFocus defaultValue={row?.name ?? ""} />
          </Field>
          <fieldset className="grid gap-1.5">
            <legend className="pb-1.5 text-s-500 text-secondary">Status Color</legend>
            <div className="flex gap-2">
              {[...new Set([...Object.keys(COLORS), color])].map((c) => (
                <label key={c} className="grid size-8 place-items-center rounded-full has-[:checked]:ring-2 has-[:checked]:ring-primary has-[:focus-visible]:outline-2 has-[:focus-visible]:outline-ring">
                  <input type="radio" name="color" value={c} defaultChecked={c === color} aria-label={COLORS[c] ?? "The current color"} className="sr-only" />
                  <span aria-hidden className="size-5 rounded-full" style={{ background: c }} />
                </label>
              ))}
            </div>
          </fieldset>
          <fieldset className="grid gap-1.5">
            <legend className="pb-1.5 text-s-500 text-secondary">Status Type{row?.default && ", fixed for a built-in status"}</legend>
            <div className="flex gap-12">
              {["Available", "Unavailable"].map((l, i) => (
                <label key={l} className="flex items-center gap-2 text-p-400">
                  <input type="radio" name="availability" value={i ? "off" : "on"} disabled={row?.default} defaultChecked={(row?.availability ?? true) === !i} className="size-4 accent-brand" /> {l}
                </label>
              ))}
            </div>
          </fieldset>
        </Editor>
      )}
    </Page>
  );
}

export function Contacts() {
  const load = useLoad(() => every<Destination>((c) => api<Paged<Destination>>("GET", `/api/v1/transfer-destinations?limit=200${c}`)));
  const e = useEdit<Destination>();
  const row = e.row;
  return (
    <Page title="Internal Contacts" sub="Manage phone contacts used for forwarding conversations">
      <ListView
        load={load}
        search={(d) => `${d.name} ${d.url ?? d.number}`}
        placeholder="Search contacts"
        open={e.start}
        action={<New label="Add new number" onClick={() => e.start("new")} />}
        empty={<Empty title="No contacts yet." />}
        cols={[
          ["Internal contact", (d) => <span className="font-mono font-normal">{d.url ?? phone(d.number)}</span>, "w-[53%]"],
          ["Alias", (d) => d.name],
        ]}
      />
      {e.open && (
        <Editor
          what="contact"
          row={row}
          reload={load.reload}
          onClose={e.close}
          body={(f) => ({ name: f.get("name"), ...(row?.url ? { url: String(f.get("url")).trim() } : { number: String(f.get("number")).trim() }) })}
          save={(b) => (row ? api("PATCH", `/api/v1/transfer-destinations/${row.id}`, b) : api("POST", `/api/v1/transfer-destinations`, b))}
          remove={row && (() => api("DELETE", `/api/v1/transfer-destinations/${row.id}`))}
        >
          {row?.url ? (
            <Field label="URL">
              <Input name="url" type="url" required autoFocus defaultValue={row.url} className="font-mono" />
            </Field>
          ) : (
            <Field label="Phone number">
              <Input name="number" required autoFocus inputMode="tel" placeholder="+14155550100" pattern="\+\d{6,15}" title="A plus sign, then the country code and number" defaultValue={row?.number ?? ""} className="font-mono" />
            </Field>
          )}
          <Field label="Alias">
            <Input name="name" required defaultValue={row?.name ?? ""} />
          </Field>
        </Editor>
      )}
    </Page>
  );
}

const DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const zones = Intl.supportedValuesOf("timeZone");
const zip = (f: FormData, ...names: string[]) => f.getAll(names[0]).map((_, i) => names.map((n) => String(f.getAll(n)[i])));

export function Hours() {
  const hours = useLoad(() => every<BusinessHours>((c) => api<Paged<BusinessHours>>("GET", `/api/v1/business-hours?limit=200${c}`)));
  const holidays = useLoad(() => every<HolidayGroup>((c) => api<Paged<HolidayGroup>>("GET", `/api/v1/holiday-groups?limit=200${c}`)));
  const eh = useEdit<BusinessHours>();
  const eg = useEdit<HolidayGroup>();
  const [h, g] = [eh.row, eg.row];
  return (
    <Page title="Business Hours & Holidays">
      <Tabs defaultValue="hours" className="mt-4 flex min-h-0 flex-1 flex-col">
        <TabList label="Business Hours & Holidays">
          <Tab value="hours">Business Hours</Tab>
          <Tab value="holidays">Holidays</Tab>
        </TabList>
        <TabPanel value="hours" className="flex min-h-0 flex-1 flex-col">
          <ListView
            load={hours}
            search={(x) => x.name}
            open={eh.start}
            action={<New label="New business hours" onClick={() => eh.start("new")} />}
            empty={<Empty title="No business hours yet." />}
            cols={[
              ["Name", (x) => x.name],
              ["Time zone", (x) => <span className="grid">{zone(x.time_zone)[0]}<span className="text-s-400 text-secondary">{zone(x.time_zone)[1]}</span></span>, "w-[35%]"],
            ]}
          />
        </TabPanel>
        <TabPanel value="holidays" className="flex min-h-0 flex-1 flex-col">
          <ListView
            load={holidays}
            search={(x) => x.name}
            open={eg.start}
            action={<New label="New holiday list" onClick={() => eg.start("new")} />}
            empty={<Empty title="No holiday lists yet." />}
            cols={[
              ["Name", (x) => x.name, "w-[38%]"],
              ["Number of holidays", (x) => x.holidays.length],
            ]}
          />
        </TabPanel>
      </Tabs>
      {eh.open && (
        <Editor
          what="business hours"
          row={h}
          reload={hours.reload}
          onClose={eh.close}
          body={(f) => ({ name: f.get("name"), time_zone: f.get("zone"), holiday_group_id: f.get("holidays") || null, selected_holidays: f.get("holidays") ? f.getAll("closed") : [], slots: zip(f, "day", "start", "end").map(([day, start, end]): Slot => ({ day, start, end })) })}
          save={(b) => (h ? api("PATCH", `/api/v1/business-hours/${h.id}`, b) : api("POST", `/api/v1/business-hours`, b))}
          remove={h && (() => api("DELETE", `/api/v1/business-hours/${h.id}`))}
        >
          <Field label="Name">
            <Input name="name" required autoFocus defaultValue={h?.name ?? ""} />
          </Field>
          <Field label="Time zone">
            <Select name="zone" required defaultValue={h?.time_zone ?? Intl.DateTimeFormat().resolvedOptions().timeZone}>
              {zones.map((z) => (
                <option key={z}>{z}</option>
              ))}
            </Select>
          </Field>
          <HolidayPick groups={holidays.data ?? []} hours={h} />
          <Rows label="Open hours" add="Add hours" items={h?.slots ?? []} blank={{ day: "mon", start: "09:00", end: "17:00" }}>
            {(s) => (
              <>
                <Select name="day" defaultValue={s.day} aria-label="Day" className="w-28">
                  {DAYS.map((d) => (
                    <option key={d} value={d}>
                      {cap(d)}
                    </option>
                  ))}
                </Select>
                <Input name="start" type="time" required defaultValue={s.start} aria-label="Opens" className="font-mono" />
                <Input name="end" type="time" required defaultValue={s.end} aria-label="Closes" className="font-mono" />
              </>
            )}
          </Rows>
        </Editor>
      )}
      {eg.open && (
        <Editor
          what="holiday list"
          row={g}
          reload={holidays.reload}
          onClose={eg.close}
          body={(f) => ({ name: f.get("name"), holidays: zip(f, "date", "holiday").map(([date, name]) => ({ date, name, slots: g?.holidays.find((x) => x.date === date)?.slots ?? [] })) })}
          save={(b) => (g ? api("PATCH", `/api/v1/holiday-groups/${g.id}`, b) : api("POST", `/api/v1/holiday-groups`, b))}
          remove={g && (() => api("DELETE", `/api/v1/holiday-groups/${g.id}`))}
        >
          <Field label="Name">
            <Input name="name" required autoFocus defaultValue={g?.name ?? ""} />
          </Field>
          <Rows label="Holidays" add="Add holiday" items={g?.holidays ?? []} blank={{ date: "", name: "" }}>
            {(x) => (
              <>
                <Input name="date" type="date" required defaultValue={x.date} aria-label="Date" className="w-40 font-mono" />
                <Input name="holiday" required defaultValue={x.name} aria-label="Holiday" placeholder="New Year" />
              </>
            )}
          </Rows>
        </Editor>
      )}
    </Page>
  );
}

function HolidayPick({ groups, hours }: { groups: HolidayGroup[]; hours?: BusinessHours }) {
  const [id, setId] = useState(hours?.holiday_group_id ?? "");
  const group = groups.find((g) => g.id === id);
  const kept = id === hours?.holiday_group_id;
  return (
    <>
      <Field label="Holidays">
        <Select name="holidays" value={id} onChange={(e) => setId(e.target.value)}>
          <option value="">No holiday list</option>
          {groups.map((x) => (
            <option key={x.id} value={x.id}>
              {x.name}
            </option>
          ))}
        </Select>
      </Field>
      {group && (
        <fieldset key={id} className="grid gap-2">
          <legend className="pb-1.5 text-s-500 text-secondary">Closed on</legend>
          {group.holidays.length ? (
            group.holidays.map((x) => (
              <label key={x.date} className="flex items-center gap-2 text-p-400">
                <input type="checkbox" name="closed" value={x.date} defaultChecked={!kept || hours!.selected_holidays.includes(x.date)} className="size-4 accent-primary" />
                {x.name}, <span className="font-mono text-s-400">{new Date(`${x.date}T00:00`).toLocaleDateString([], { month: "short", day: "numeric", year: "numeric" })}</span>
              </label>
            ))
          ) : (
            <p className="text-s-400 text-secondary">This list has no holidays yet.</p>
          )}
        </fieldset>
      )}
    </>
  );
}

const PRIORITY: Record<Task["priority"], [Tone | undefined, string?]> = { low: ["off"], medium: [undefined, "#3b82f6"], high: [undefined, "#f59e0b"], critical: ["bad"] };
const STATUSES = ["todo", "in_progress", "waiting_on_customer", "resolved", "cancelled"];
const ENDED = ["resolved", "cancelled"];

export function Tasks() {
  const load = usePaged<Task>((c) => api<Paged<Task>>("GET", `/api/v1/tasks?limit=50${c}`));
  const e = useEdit<Task>();
  const row = e.row;
  return (
    <Page title="Tasks">
      <ListView
        load={load}
        more={load.more}
        search={(t) => t.title}
        placeholder="Search the loaded tasks"
        refresh
        open={e.start}
        empty={<Empty title="No tasks yet." />}
        cols={[
          ["Task name", (t) => t.title || "--"],
          ["Task type", (t) => <Badge>{[t.type, t.channel].filter(Boolean).map((w) => plain(w).toUpperCase()).join(" · ")}</Badge>],
          ["Priority", (t) => <span className={cls(t.priority === "critical" && "text-destructive")}><State tone={PRIORITY[t.priority][0]} color={PRIORITY[t.priority][1]}>{cap(t.priority)}</State></span>],
          ["Status", (t) => <State tone={t.status === "resolved" ? "live" : "off"}>{cap(plain(t.status))}</State>],
          ["Created date", (t) => <span className="font-mono text-xs-400">{day(t.created_at)}</span>],
          ["Due date", (t) => <span className="font-mono text-xs-400">{day(t.due_at)}</span>],
        ]}
      />
      {row && ENDED.includes(row.status) && (
        <Sheet open onClose={e.close} title="Task" sub={`This task was ${row.status}, so it can no longer be changed.`}>
          <div className="px-4 py-2">
            <Facts rows={[["Task name", row.title], ["Priority", cap(row.priority)], ["Status", cap(plain(row.status))]]} />
          </div>
        </Sheet>
      )}
      {row && !ENDED.includes(row.status) && (
        <Editor what="task" row={row} reload={load.reload} onClose={e.close} body={(f) => ({ title: f.get("title"), priority: f.get("priority"), status: f.get("status") })} save={(b) => api("PATCH", `/api/v1/tasks/${row.id}`, b)}>
          <Field label="Task name">
            <Input name="title" required autoFocus defaultValue={row.title} />
          </Field>
          <Field label="Priority">
            <Select name="priority" defaultValue={row.priority}>
              {Object.keys(PRIORITY).map((p) => (
                <option key={p} value={p}>
                  {cap(p)}
                </option>
              ))}
            </Select>
          </Field>
          <Field label="Status">
            <Select name="status" defaultValue={row.status}>
              {STATUSES.map((s) => (
                <option key={s} value={s}>
                  {cap(plain(s))}
                </option>
              ))}
            </Select>
          </Field>
        </Editor>
      )}
    </Page>
  );
}
