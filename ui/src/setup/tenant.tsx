import { ChevronDown, ChevronRight, Plus } from "lucide-react";
import { useState } from "react";
import { type Page as Paged, api, every, store } from "../api";
import type { Disposition } from "../types";
import { operatorKey } from "../format";
import { Badge, Button, Editor, Empty, Facts, Field, Form, Input, ListView, Page, Reorder, Select, Submit, TabList, TabPanel, Tab, Tabs, Toggle, act, useLoad } from "../ui";
import { ConversationRules } from "./settings";

export function Dispositions() {
  const load = useLoad(() => every<Disposition>((c) => api<Paged<Disposition>>("GET", `/api/v1/dispositions?limit=200${c}`)).then((d) => d.filter((x) => !x.system).sort((a, b) => a.sequence - b.sequence)));
  const [edit, setEdit] = useState<Disposition | "new" | null>(null);
  const [shown, setShown] = useState<Record<string, boolean>>({});
  const row = edit === "new" ? undefined : (edit ?? undefined);
  const tops = load.data?.filter((d) => !d.parent_disposition_id) ?? [];
  const order = (ids: string[]) => api("POST", "/api/v1/dispositions/reorder", { ids }).then(load.reload);
  const subs = (d: Disposition) => load.data?.filter((s) => s.parent_disposition_id === d.id) ?? [];
  const on = (d: Disposition) => (
    <span onClick={(e) => e.stopPropagation()}>
      <Toggle label="" aria-label={`Agents can pick ${d.name}`} checked={!d.disabled} onChange={(e) => act(`Turning ${d.name} ${e.target.checked ? "on" : "off"}`, () => api("PATCH", `/api/v1/dispositions/${d.id}`, { disabled: !e.target.checked }), `${d.name} is ${e.target.checked ? "on" : "off"}`).then(load.reload)} />
    </span>
  );
  return (
    <Page title="Conversation Configurations" sub="Configure conversation behavior, dispositions, and their display order">
      <Tabs defaultValue="config" className="mt-4 flex min-h-0 flex-1 flex-col">
        <TabList label="Conversation Configurations">
          <Tab value="config">Configurations</Tab>
          <Tab value="dispositions">Dispositions</Tab>
        </TabList>
        <TabPanel value="config" className="min-h-0 flex-1 overflow-y-auto">
          <ConversationRules />
        </TabPanel>
        <TabPanel value="dispositions" className="flex min-h-0 flex-1 flex-col">
          <ListView
            load={{ ...load, data: load.data && tops }}
            search={(d) => d.name}
            iconSearch
            open={setEdit}
            reorder={order}
            below={(d) =>
              shown[d.id] && (
                <div className="ml-[11px] border-t bg-background">
                  <Reorder bare rows={subs(d)} open={setEdit} save={order} cols={[["", (x) => x.name], ["", () => null], ["", (x) => on(x)], ["", () => null]]} />
                </div>
              )
            }
            action={
              <Button variant="primary" size="md" onClick={() => setEdit("new")}>
                <Plus aria-hidden /> Add disposition
              </Button>
            }
            empty={<Empty title="No dispositions yet. Agents need at least one to wrap up a call. Add outcomes such as Resolved or Call back." />}
            cols={[
              [
                "Dispositions",
                (d) => (
                  <span className="flex items-center">
                    <button type="button" aria-label={shown[d.id] ? "Hide sub-dispositions" : "Show sub-dispositions"} aria-expanded={!!shown[d.id]} className="size-5" onClick={(e) => (e.stopPropagation(), setShown({ ...shown, [d.id]: !shown[d.id] }))}>
                      {shown[d.id] ? <ChevronDown className="size-4" aria-hidden /> : <ChevronRight className="size-4" aria-hidden />}
                    </button>
                    {d.name}
                  </span>
                ),
              ],
              ["", (d) => subs(d).length > 0 && <Badge>{`${subs(d).length} Sub dispositions`}</Badge>],
              ["Status type", on],
              ["", () => null],
            ]}
          />
        </TabPanel>
      </Tabs>
      {edit && (
        <Editor
          what="disposition"
          row={row}
          reload={load.reload}
          onClose={() => setEdit(null)}
          body={(f) => ({ name: f.get("name"), parent_disposition_id: f.get("parent") || null, note_required: f.get("note") === "on" })}
          save={(b) => (row ? api("PATCH", `/api/v1/dispositions/${row.id}`, b) : api("POST", `/api/v1/dispositions`, b))}
          remove={row && (() => api("DELETE", `/api/v1/dispositions/${row.id}`))}
        >
          <Field label="Name">
            <Input name="name" required autoFocus placeholder="Resolved" defaultValue={row?.name ?? ""} />
          </Field>
          <Field label="Under" hint="Pick a disposition to make this one of its sub-dispositions. Under a disabled one, it is disabled too.">
            <Select name="parent" defaultValue={row?.parent_disposition_id ?? ""}>
              <option value="">Nothing, a top-level disposition</option>
              {tops.filter((d) => d.id !== row?.id).map((d) => (
                <option key={d.id} value={d.id}>
                  {d.disabled ? `${d.name} (disabled)` : d.name}
                </option>
              ))}
            </Select>
          </Field>
          <Toggle name="note" label="The agent must write a note" defaultChecked={row?.note_required} />
        </Editor>
      )}
    </Page>
  );
}

type Tenant = { id: string; name: string; plivo_auth_id: string; plivo_app_id: string | null };

export function PlivoAccount() {
  const [done, setDone] = useState<Tenant | null>(null);
  return (
    <Page title="Plivo Account" sub="Plivo sends calls and texts to this server's public URL. When that URL changes, restart the server with the new one, then re-apply it here.">
      <div className="mt-6 grid max-w-xl gap-6 pb-6">
        <div>
          <h2 className="text-p-600">Re-apply Plivo URLs</h2>
          <p className="text-p-400 text-secondary">This needs the operator key. Organization keys cannot do it. The key is used once and not kept.</p>
        </div>
        <Form
          onSubmit={async (f, form) => {
            const operator = operatorKey(f.get("operator"));
            form.reset();
            const typed = String(f.get("tenant")).trim();
            const t = await act(
              "Re-applying the Plivo URLs",
              async () => {
                let id = typed;
                if (!id) {
                  const all = await api<Paged<Tenant>>("GET", `/api/v1/tenants?limit=2`, undefined, operator);
                  if (all.data.length !== 1) throw new Error(`This server has ${all.next_cursor ? "more than one" : all.data.length || "no"} organization${all.data.length === 1 ? "" : "s"}. Enter the organization ID.`);
                  id = all.data[0].id;
                }
                return api<Tenant>("PATCH", `/api/v1/tenants/${id}`, {}, operator);
              },
              (t) => `Plivo now sends ${t.name}'s calls here`,
            );
            if (t) {
              store.set("tenant", t.id);
              store.set("tenantName", t.name);
              setDone(t);
            }
          }}
        >
          <Field label="Operator key">
            <Input name="operator" type="password" required autoComplete="off" placeholder="HS_OPERATOR_KEY from docker/.env" />
          </Field>
          <Field label="Organization ID" hint="Leave it empty when this server has one organization.">
            <Input name="tenant" defaultValue={store.get<string>("tenant") ?? ""} className="font-mono" spellCheck={false} />
          </Field>
          <Submit className="justify-self-start">Re-apply Plivo URLs</Submit>
        </Form>
        {done && (
          <div className="border-t pt-6">
            <h2 className="text-p-600">Organization</h2>
            <Facts
              rows={[
                ["Name", done.name],
                ["Organization ID", <code key="t" className="font-mono">{done.id}</code>],
                ["Plivo Auth ID", <code key="a" className="font-mono">{done.plivo_auth_id}</code>],
                ["Plivo application", done.plivo_app_id ? <code key="p" className="font-mono">{done.plivo_app_id}</code> : "None, the organization points its numbers itself"],
              ]}
            />
          </div>
        )}
      </div>
    </Page>
  );
}
