import { useState } from "react";
import { asAgent } from "../api";
import { go } from "../shell";
import { clock } from "../format";
import { Button, Field, Select, Textarea, act } from "../ui";
import type { Ctx } from "./desk";
import { type Line, autoWrap } from "./state";

export function WrapUp({ ctx, line }: { ctx: Ctx; line: Line }) {
  const { s, now, loadLine } = ctx;
  const tops = s.dispositions.filter((d) => !d.parent_disposition_id && !d.system && !d.disabled);
  const [pick, setPick] = useState("");
  const [sub, setSub] = useState("");
  const [note, setNote] = useState("");
  const chosen = tops.find((d) => d.id === pick);
  const subs = s.dispositions.filter((d) => d.parent_disposition_id === pick && !d.disabled);
  const need = !tops.length
    ? "There are no dispositions to choose from. An admin adds them under Conversation Configurations."
    : !chosen
      ? "Choose a disposition to wrap up."
      : chosen.note_required && !note.trim()
        ? `${chosen.name} needs a note.`
        : "";
  const submit = async () => {
    const body = { disposition_id: pick, sub_disposition_id: sub || null, note: note.trim() || null };
    if (await act("Submitting wrap-up", () => asAgent("POST", `/api/v1/conversations/${line.cid}/wrap-up`, body), "Wrap-up submitted. Ready for the next one.")) {
      await loadLine(line.cid);
      go("/desk");
    }
  };
  return (
    <section className="grid w-full max-w-[500px] gap-4 rounded-sm border bg-background px-6 pb-6" aria-labelledby="wrap-title">
      <h2 id="wrap-title" className="mt-4 text-[18px] font-semibold leading-7">
        Wrap-up
      </h2>
      <Field label="Disposition">
        <Select
          value={pick}
          onChange={(e) => {
            setPick(e.target.value);
            setSub("");
          }}
        >
          <option value="">Choose one</option>
          {tops.map((d) => (
            <option key={d.id} value={d.id}>
              {d.name}
            </option>
          ))}
        </Select>
      </Field>
      {subs.length > 0 && (
        <Field label="Sub-disposition">
          <Select value={sub} onChange={(e) => setSub(e.target.value)}>
            <option value="">None</option>
            {subs.map((d) => (
              <option key={d.id} value={d.id}>
                {d.name}
              </option>
            ))}
          </Select>
        </Field>
      )}
      <Field label={chosen?.note_required ? "Wrap-up notes (required)" : "Wrap-up notes"}>
        <Textarea value={note} onChange={(e) => setNote(e.target.value)} required={chosen?.note_required} placeholder="Key notes about the conversation" />
      </Field>
      <div className="mt-4 flex items-center justify-between gap-3">
        <Button variant="primary" size="md" disabled={!!need} onClick={submit}>
          Submit
        </Button>
        {line.until && (
          <p className="text-s-400 text-secondary" role="timer">
            {autoWrap(s, line) ? "Auto wrap-up in" : "Wrap-up time left"} {clock((line.until - now) / 1000)}
          </p>
        )}
      </div>
      {need && (
        <p className="text-s-400 text-secondary" aria-live="polite">
          {need}
        </p>
      )}
    </section>
  );
}
